/* ----------------------------------------------------------------
 *
 * libnvme-strom.c
 *
 * Collection of routines to use 'nvme-strom' kernel module
 * --------
 * Copyright 2016 (C) KaiGai Kohei <kaigai@kaigai.gr.jp>
 * Copyright 2016 (C) The PG-Strom Development Team
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2,
 * as published by the Free Software Foundation.
 * ----------------------------------------------------------------
 */
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <cuda.h>
#include "nvme_strom.h"

#define offsetof(type, field)   ((long) &((type *)0)->field)
#define Max(a,b)				((a) > (b) ? (a) : (b))
#define Min(a,b)				((a) < (b) ? (a) : (b))
#define BLCKSZ					(8192)
#define RELSEG_SIZE				(131072)

/* command line options */
static int		device_index = -1;
static int		nr_segments = 6;
static size_t	segment_sz = 32UL << 20;
static int		enable_checks = 0;
static int		print_mapping = 0;
static int		test_by_vfs = 0;
static size_t	vfs_io_size = 0;

static sem_t	buffer_sem;
static pthread_mutex_t	buffer_lock;

/*
 * nvme_strom_ioctl - entrypoint of NVME-Strom
 */
static int
nvme_strom_ioctl(int cmd, const void *arg)
{
	static __thread int fdesc_nvme_strom = -1;

	if (fdesc_nvme_strom < 0)
	{
		fdesc_nvme_strom = open(NVME_STROM_IOCTL_PATHNAME, O_RDONLY);
		if (fdesc_nvme_strom < 0)
		{
			fprintf(stderr, "failed to open \"%s\" : %m\n",
					NVME_STROM_IOCTL_PATHNAME);
			return -1;
		}
	}
	return ioctl(fdesc_nvme_strom, cmd, arg);
}

#define cuda_exit_on_error(__RC, __API_NAME)							\
	do {																\
		if ((__RC) != CUDA_SUCCESS)										\
		{																\
			const char *error_name;										\
																		\
			if (cuGetErrorName((__RC), &error_name) != CUDA_SUCCESS)	\
				error_name = "unknown error";							\
			fprintf(stderr, "%d: failed on %s: %s\n",					\
					__LINE__, __API_NAME, error_name);					\
			exit(1);													\
		}																\
	} while(0)

#define system_exit_on_error(__RC, __API_NAME)							\
	do {																\
		if ((__RC))														\
		{																\
			fprintf(stderr, "%d: failed on %s: %m\n",					\
					__LINE__, __API_NAME);								\
			exit(1);													\
		}																\
	} while(0)

static void
ioctl_check_file(const char *filename, int fdesc)
{
	StromCmd__CheckFile uarg;
	int		rc;

	memset(&uarg, 0, sizeof(uarg));
	uarg.fdesc = fdesc;

	rc = nvme_strom_ioctl(STROM_IOCTL__CHECK_FILE, &uarg);
	if (rc)
	{
		fprintf(stderr, "STROM_IOCTL__CHECK_FILE('%s') --> %d: %m\n",
				filename, rc);
		exit(1);
	}
}

static unsigned long
ioctl_map_gpu_memory(CUdeviceptr cuda_devptr, size_t buffer_size)
{
	StromCmd__MapGpuMemory uarg;
	int			retval;

	memset(&uarg, 0, sizeof(StromCmd__MapGpuMemory));
	uarg.vaddress = cuda_devptr;
	uarg.length = buffer_size;

	retval = nvme_strom_ioctl(STROM_IOCTL__MAP_GPU_MEMORY, &uarg);
	if (retval)
	{
		fprintf(stderr, "STROM_IOCTL__MAP_GPU_MEMORY(%p, %lu) --> %d: %m",
			   (void *)cuda_devptr, buffer_size, retval);
		exit(1);
	}
	return uarg.handle;
}

typedef struct
{
	loff_t			fpos;
	int				index;
	int				is_running;
	CUstream		cuda_stream;
	void		   *src_buffer;
	void		   *dest_buffer;
	StromCmd__MemCpySsdToGpuWriteBack uarg;
} async_task;

static void
callback_dma_wait(CUstream cuda_stream, CUresult status, void *private)
{
	StromCmd__MemCpySsdToGpuWait	uarg;
	async_task	   *atask = private;
	int				rv;

	cuda_exit_on_error(status, "async_task");

	memset(&uarg, 0, sizeof(StromCmd__MemCpySsdToGpuWait));
	uarg.dma_task_id = atask->uarg.dma_task_id;
	rv = nvme_strom_ioctl(STROM_IOCTL__MEMCPY_SSD2GPU_WAIT, &uarg);
	if (uarg.status)
		printf("async dma (id=%lu, status=%ld)\n",
			   uarg.dma_task_id, uarg.status);
	system_exit_on_error(rv, "STROM_IOCTL__MEMCPY_SSD2GPU_WAIT");
}

static void
callback_release_atask(CUstream cuda_stream, CUresult status, void *private)
{
	async_task	   *atask = private;
	ssize_t			nbytes;

	/* integrity checks? */
	if (!enable_checks)
		goto check_ok;

	if (test_by_vfs)
	{
		if (memcmp(atask->src_buffer,
				   atask->dest_buffer,
				   segment_sz) != 0)
			system_exit_on_error(1, "memcmp");
	}
	else
	{
		int		i, j;

		/* read file via VFS */
		nbytes = pread(atask->uarg.file_desc,
					   atask->src_buffer,
					   segment_sz, atask->fpos);
		system_exit_on_error(nbytes < segment_sz, "pread");

		for (i=0; (i+1) * BLCKSZ < nbytes; i++)
		{
			j = atask->uarg.chunk_ids[i] % (segment_sz / BLCKSZ);
			if (memcmp(atask->src_buffer + i * BLCKSZ,
					   atask->dest_buffer + j * BLCKSZ,
					   BLCKSZ) != 0)
			{
				printf("i=%d j=%d\n", i, j);
				system_exit_on_error(1, "memcmp");
			}
		}
	}
check_ok:
	pthread_mutex_lock(&buffer_lock);
	atask->is_running = 0;
	pthread_mutex_unlock(&buffer_lock);

	sem_post(&buffer_sem);
}

static async_task *
setup_async_tasks(int fdesc, unsigned long handle)
{
	async_task	   *async_tasks;
	CUresult		rc;
	int				i, rv;

	async_tasks = malloc(sizeof(async_task) * nr_segments);
	system_exit_on_error(!async_tasks, "malloc");

	rv = sem_init(&buffer_sem, 0, nr_segments);
	system_exit_on_error(rv, "sem_init");
	rv = pthread_mutex_init(&buffer_lock, NULL);
	system_exit_on_error(rv, "pthread_mutex_init");

	for (i=0; i < nr_segments; i++)
	{
		StromCmd__MemCpySsdToGpuWriteBack  *uarg;

		uarg = &async_tasks[i].uarg;

		uarg->handle = handle;
		uarg->offset = i * segment_sz;
		uarg->file_desc = dup(fdesc);
		system_exit_on_error(uarg->file_desc < 0, "dup");
		uarg->nr_chunks = segment_sz / BLCKSZ;
		uarg->chunk_sz = BLCKSZ;
		uarg->relseg_sz = 0;
		uarg->chunk_ids = malloc(sizeof(uint32_t) * uarg->nr_chunks);
		system_exit_on_error(!uarg->chunk_ids, "malloc");

		rc = cuMemAllocHost(&async_tasks[i].src_buffer, segment_sz);
		cuda_exit_on_error(rc, "cuMemAllocHost");
		uarg->wb_buffer = async_tasks[i].src_buffer;

		rc = cuMemAllocHost(&async_tasks[i].dest_buffer, segment_sz);
        cuda_exit_on_error(rc, "cuMemAllocHost");

		rc = cuStreamCreate(&async_tasks[i].cuda_stream,
                            CU_STREAM_DEFAULT);
        cuda_exit_on_error(rc, "cuStreamCreate");

		async_tasks[i].fpos  = 0;
        async_tasks[i].index = i;
        async_tasks[i].is_running = 0;
	}
	return async_tasks;
}

static void
show_throughput(const char *filename, size_t file_size,
				struct timeval tv1, struct timeval tv2,
				long usec_sem_wait,
				long nr_ram2gpu, long nr_ssd2gpu,
				long nr_dma_submit, long nr_dma_blocks)
{
	long		time_ms;
	double		throughput;

	time_ms = ((tv2.tv_sec * 1000 + tv2.tv_usec / 1000) -
			   (tv1.tv_sec * 1000 + tv1.tv_usec / 1000));
	throughput = (double)file_size / ((double)time_ms / 1000.0);

	if (file_size < (4UL << 10))
		printf("read: %zuBytes", file_size);
	else if (file_size < (4UL << 20))
		printf("read: %.2fKB", (double)file_size / (double)(1UL << 10));
	else if (file_size < (4UL << 30))
		printf("read: %.2fMB", (double)file_size / (double)(1UL << 20));
	else
		printf("read: %.2fGB", (double)file_size / (double)(1UL << 30));

	if (time_ms < 4000UL)
		printf(", time: %lums", time_ms);
	else
		printf(", time: %.2fsec", (double)time_ms / 1000.0);

	if (throughput < (double)(4UL << 10))
		printf(", throughput: %zuB/s\n", (size_t)throughput);
	else if (throughput < (double)(4UL << 20))
		printf(", throughput: %.2fKB/s\n", throughput / (double)(1UL << 10));
	else if (throughput < (double)(4UL << 30))
		printf(", throughput: %.2fMB/s\n", throughput / (double)(1UL << 20));
	else
		printf(", throughput: %.2fGB/s\n", throughput / (double)(1UL << 30));

	if (usec_sem_wait < 4000)
		printf("sem_wait: %ldus", usec_sem_wait);
	else if (usec_sem_wait < 4000 * 4000)
		printf("sem_wait: %ldms", usec_sem_wait / 1000);
	else
		printf("sem_wait: %.2fsec", (double)usec_sem_wait / 1000000.0);

	if (nr_ram2gpu > 0 || nr_ssd2gpu > 0)
	{
		printf(", nr_ram2gpu: %ld, nr_ssd2gpu: %ld",
			   nr_ram2gpu, nr_ssd2gpu);
	}
	if (nr_dma_submit > 0)
	{
		printf(", average DMA blocks: %.2f",
			   (double)nr_dma_blocks / (double)nr_dma_submit);
	}
	putchar('\n');
}

static void
exec_test_by_strom(CUdeviceptr cuda_devptr, unsigned long handle,
				   const char *filename, int fdesc, size_t file_size)
{
	async_task	   *async_tasks;
	CUresult		rc;
	int				i, j, rv;
	size_t			fpos;
	struct timeval	tv1, tv2;
	struct timeval	wt1, wt2;
	long			usec_sem_wait = 0;
	long			nr_ram2gpu = 0;
	long			nr_ssd2gpu = 0;
	long			nr_dma_submit = 0;
	long			nr_dma_blocks = 0;

	async_tasks = setup_async_tasks(fdesc, handle);
	gettimeofday(&tv1, NULL);
	for (fpos=0; fpos < file_size; fpos += segment_sz)
	{
		async_task *atask = NULL;

		/* wait for DMA slot */
		gettimeofday(&wt1, NULL);
		rv = sem_wait(&buffer_sem);
		system_exit_on_error(rv, "sem_wait");
		gettimeofday(&wt2, NULL);
		usec_sem_wait += ((wt2.tv_sec * 1000000 + wt2.tv_usec) -
						  (wt1.tv_sec * 1000000 + wt1.tv_usec));
		/* find out an available async_task */
		pthread_mutex_lock(&buffer_lock);
		for (j=0; j < nr_segments; j++)
		{
			atask = &async_tasks[j % nr_segments];
			if (!atask->is_running)
			{
				atask->is_running = 1;
				atask->fpos = fpos;
				break;		/* found */
			}
		}
		if (j == nr_segments)
		{
			fprintf(stderr, "Bug? no free async_task but semaphore > 0\n");
			exit(1);
		}
		pthread_mutex_unlock(&buffer_lock);

		/* setup SSD-to-GPU DMA request */
		atask->fpos = fpos;
		for (i=0; i < atask->uarg.nr_chunks; i++)
			atask->uarg.chunk_ids[i] = fpos / BLCKSZ + i;

		rv = nvme_strom_ioctl(STROM_IOCTL__MEMCPY_SSD2GPU_WRITEBACK,
							  &atask->uarg);
		system_exit_on_error(rv, "STROM_IOCTL__MEMCPY_SSD2GPU_WRITEBACK");

		nr_ram2gpu += atask->uarg.nr_ram2gpu;
		nr_ssd2gpu += atask->uarg.nr_ssd2gpu;
		nr_dma_submit += atask->uarg.nr_dma_submit;
		nr_dma_blocks += atask->uarg.nr_dma_blocks;

		/* kick RAM-to-GPU DMA, if written back */
		if (atask->uarg.nr_ram2gpu > 0)
		{
			rc = cuMemcpyHtoDAsync(cuda_devptr + atask->uarg.offset,
								   (char *)atask->src_buffer +
								   BLCKSZ * (atask->uarg.nr_chunks -
											 atask->uarg.nr_ram2gpu),
								   BLCKSZ * atask->uarg.nr_ram2gpu,
								   atask->cuda_stream);
			cuda_exit_on_error(rc, "cuMemcpyHtoDAsync");
		}

		/* kick callback for synchronization */
		rc = cuStreamAddCallback(atask->cuda_stream,
								 callback_dma_wait, atask, 0);
		cuda_exit_on_error(rc, "cuStreamAddCallback");

		/* kick GPU-to-RAM DMA */
		if (enable_checks)
		{
			rc = cuMemcpyDtoHAsync(atask->dest_buffer,
								   cuda_devptr + atask->uarg.offset,
								   segment_sz,
								   atask->cuda_stream);
			cuda_exit_on_error(rc, "cuMemcpyDtoHAsync");
		}

		/* kick callback to release atask */
		rc = cuStreamAddCallback(atask->cuda_stream,
								 callback_release_atask, atask, 0);
		cuda_exit_on_error(rc, "cuStreamAddCallback");
	}
	/* wait for completion of the asyncronous tasks */
	do {
		gettimeofday(&wt1, NULL);
		rv = sem_wait(&buffer_sem);
		system_exit_on_error(rv, "sem_wait");
		gettimeofday(&wt2, NULL);
		usec_sem_wait += ((wt2.tv_sec * 1000000 + wt2.tv_usec) -
						  (wt1.tv_sec * 1000000 + wt1.tv_usec));
		pthread_mutex_lock(&buffer_lock);
		for (j=0; j < nr_segments; j++)
		{
			async_task *atask = &async_tasks[j];
			if (atask->is_running)
				break;	/* here is still running task */
		}
		pthread_mutex_unlock(&buffer_lock);
	} while (j < nr_segments);
	gettimeofday(&tv2, NULL);
	show_throughput(filename, file_size, tv1, tv2,
					usec_sem_wait,
					nr_ram2gpu, nr_ssd2gpu,
					nr_dma_submit, nr_dma_blocks);
}

static void
exec_test_by_vfs(CUdeviceptr cuda_devptr, unsigned long handle,
				 const char *filename, int fdesc, size_t file_size)
{
	async_task	   *async_tasks;
	CUresult		rc;
	int				j, rv;
	size_t			offset;
	size_t			pos;
	size_t			count;
	ssize_t			retval;
	struct timeval	tv1, tv2;
	struct timeval	wt1, wt2;
	long			usec_sem_wait = 0;

	async_tasks = setup_async_tasks(fdesc, handle);
	gettimeofday(&tv1, NULL);
	for (offset=0; offset < file_size; offset += segment_sz)
	{
		async_task *atask = NULL;

		gettimeofday(&wt1, NULL);
		rv = sem_wait(&buffer_sem);
		system_exit_on_error(rv, "sem_wait");
		gettimeofday(&wt2, NULL);
		usec_sem_wait += ((wt1.tv_sec * 1000000 + wt2.tv_usec) -
						  (wt2.tv_sec * 1000000 + wt2.tv_usec));
		/* find out an available async_task */
		pthread_mutex_lock(&buffer_lock);
		for (j=0; j < nr_segments; j++)
		{
			atask = &async_tasks[j % nr_segments];
			if (!atask->is_running)
				break;		/* found */
		}
		if (j == nr_segments)
		{
			fprintf(stderr, "Bug? no free async_task but semaphore > 0\n");
			exit(1);
		}
		pthread_mutex_unlock(&buffer_lock);

		/* Load SSD-to-RAM */
		count = 0;
		for (pos=0; pos < segment_sz; pos += vfs_io_size)
		{
			retval = pread(fdesc, (char *)atask->src_buffer + pos,
						   vfs_io_size, offset + pos);
			system_exit_on_error(retval < 0, "pread");
			count += retval;
			if (retval < vfs_io_size)
				break;
		}
		if (count < segment_sz)
			break;	/* EOF */

		atask->is_running = 1;
		atask->fpos = offset;

		/* Kick RAM-to-GPU DMA */
		rc = cuMemcpyHtoDAsync(cuda_devptr + atask->uarg.offset,
							   atask->src_buffer, segment_sz,
							   atask->cuda_stream);
		cuda_exit_on_error(rc, "cuMemcpyHtoDAsync");

		/* Kick GPU-to-RAM DMA */
		if (enable_checks)
		{
			rc = cuMemcpyDtoHAsync(atask->dest_buffer,
								   cuda_devptr + atask->uarg.offset,
								   segment_sz,
								   atask->cuda_stream);
			cuda_exit_on_error(rc, "cuMemcpyDtoHAsync");
		}
		/* Kick callback to release atask */
		rc = cuStreamAddCallback(atask->cuda_stream,
								 callback_release_atask, atask, 0);
		cuda_exit_on_error(rc, "cuStreamAddCallback");
	}
	/* wait for completion of the asyncronous tasks */
	do {
		gettimeofday(&wt1, NULL);
		rv = sem_wait(&buffer_sem);
		system_exit_on_error(rv, "sem_wait");
		gettimeofday(&wt2, NULL);
		usec_sem_wait += ((wt1.tv_sec * 1000000 + wt2.tv_usec) -
						  (wt2.tv_sec * 1000000 + wt2.tv_usec));

		pthread_mutex_lock(&buffer_lock);
		for (j=0; j < nr_segments; j++)
		{
			async_task *atask = &async_tasks[j];
			if (atask->is_running)
				break;  /* here is still running task */
		}
		pthread_mutex_unlock(&buffer_lock);
	} while (j < nr_segments);

	gettimeofday(&tv2, NULL);
	show_throughput(filename, file_size, tv1, tv2, usec_sem_wait, 0, 0, 0, 0);
}

/*
 * ioctl_print_gpu_memory
 */
static int ioctl_print_gpu_memory(void)
{
	StromCmd__ListGpuMemory *cmd_list;
	StromCmd__InfoGpuMemory	*cmd_info;
	uint32_t		nrooms = 2000;
	int				i, j;

	/* get list of mapped memory handles */
	do {
		cmd_list = malloc(offsetof(StromCmd__ListGpuMemory,
								   handles[nrooms]));
		system_exit_on_error(!cmd_list, "malloc");
		cmd_list->nrooms = nrooms;
		cmd_list->nitems = 0;
		if (nvme_strom_ioctl(STROM_IOCTL__LIST_GPU_MEMORY, cmd_list))
		{
			if (errno != ENOBUFS)
				system_exit_on_error(errno, "STROM_IOCTL__LIST_GPU_MEMORY");
			assert(cmd_list->nitems > cmd_list->nrooms);
			nrooms = cmd_list->nitems + 100;	/* with some margin */
			free(cmd_list);
		}
	} while (errno != 0);

	/* get property for each mapped device memory */
	cmd_info = malloc(offsetof(StromCmd__InfoGpuMemory,
							   paddrs[nrooms]));
	system_exit_on_error(!cmd_info, "malloc");
	i = 0;
	while (i < cmd_list->nitems)
	{
		cmd_info->handle = cmd_list->handles[i];
		cmd_info->nrooms = nrooms;

		if (nvme_strom_ioctl(STROM_IOCTL__INFO_GPU_MEMORY, cmd_info))
		{
			if (errno == ENOENT)
			{
				i++;
				continue;
			}
			else if (errno != ENOBUFS)
				system_exit_on_error(errno, "STROM_IOCTL__INFO_GPU_MEMORY");
			assert(cmd_info->nitems > nrooms);
			nrooms = cmd_info->nitems + 100;
			free(cmd_info);
			cmd_info = malloc(offsetof(StromCmd__InfoGpuMemory,
									   paddrs[nrooms]));
			system_exit_on_error(!cmd_info, "malloc");
			continue;
		}
		else
		{
			printf("%s"
				   "Mapped GPU Memory (handle: 0x%016lx) %p - %p\n"
				   "GPU Page: version=%u, size=%u, n_entries=%u\n"
				   "Owner: uid=%u\n",
				   (i == 0 ? "" : "\n"),
				   cmd_info->handle,
				   (void *)(cmd_info->paddrs[0] +
							cmd_info->map_offset),
				   (void *)(cmd_info->paddrs[0] +
							cmd_info->map_offset + cmd_info->map_length),
				   cmd_info->version,
				   cmd_info->gpu_page_sz,
				   cmd_info->nitems,
				   cmd_info->owner);

			for (j=0; j < cmd_info->nitems; j++)
			{
				printf("+%08lx: %p - %p\n",
					   j * (size_t)cmd_info->gpu_page_sz,
					   (void *)(cmd_info->paddrs[j]),
					   (void *)(cmd_info->paddrs[j] + cmd_info->gpu_page_sz));
			}
		}
		i++;
	}
	return 0;
}

/*
 * usage
 */
static void usage(const char *cmdname)
{
	fprintf(stderr,
			"usage: %s [OPTIONS] <filename>\n"
			"    -d <device index>:        (default 0)\n"
			"    -n <num of segments>:     (default 6)\n"
			"    -s <segment size in MB>:  (default 32MB)\n"
			"    -c : Enables corruption check (default off)\n"
			"    -h : Print this message   (default off)\n"
			"    -f([<i/o size in KB>]): Test by VFS access (default off)\n"
			"    -p (<map handle>): Print property of mapped device memory\n",
			basename(strdup(cmdname)));
	exit(1);
}

/*
 * entrypoint of driver_test
 */
int main(int argc, char * const argv[])
{
	const char	   *filename = NULL;
	int				fdesc = -1;
	struct stat		stbuf;
	size_t			filesize;
	size_t			buffer_size;
	CUresult		rc;
	CUdevice		cuda_device;
	CUcontext		cuda_context;
	CUdeviceptr		cuda_devptr;
	unsigned long	mgmem_handle;
	char			devname[256];
	int				code;

	while ((code = getopt(argc, argv, "d:n:s:cpf::h")) >= 0)
	{
		switch (code)
		{
			case 'd':
				device_index = atoi(optarg);
				break;
			case 'n':		/* number of chunks */
				nr_segments = atoi(optarg);
				break;
			case 's':		/* size of chunks in MB */
				segment_sz = (size_t)atoi(optarg) << 20;
				break;
			case 'c':
				enable_checks = 1;
				break;
			case 'p':
				print_mapping = 1;
				break;
			case 'f':
				test_by_vfs = 1;
				if (optarg)
					vfs_io_size = (size_t)atoi(optarg) << 10;
				break;
			case 'h':
			default:
				usage(argv[0]);
				break;
		}
	}
	buffer_size = (size_t)segment_sz * nr_segments;

	/* dump the current device memory mapping */
	if (print_mapping)
		return ioctl_print_gpu_memory();

	if (optind + 1 == argc)
		filename = argv[optind];
	else
		usage(argv[0]);

	if (vfs_io_size == 0)
		vfs_io_size = segment_sz;
	else if (segment_sz % vfs_io_size != 0)
	{
		fprintf(stderr, "VFS I/O size (%zuKB) mismatch to ChunkSize (%zuMB)\n",
				vfs_io_size >> 10, segment_sz >> 20);
		return 1;
	}

	/* open the target file */
	fdesc = open(filename, O_RDONLY);
	if (fdesc < 0)
	{
		fprintf(stderr, "failed to open \"%s\": %m\n", filename);
		return 1;
	}

	if (fstat(fdesc, &stbuf) != 0)
	{
		fprintf(stderr, "failed on fstat(\"%s\"): %m\n", filename);
		return 1;
	}
	filesize = (stbuf.st_size / segment_sz) * segment_sz;
	if (filesize == 0)
	{
		fprintf(stderr, "file size (%zu) is smaller than segment size %zu",
				filesize, segment_sz);
		return 1;
	}

	/* is this file supported? */
	ioctl_check_file(filename, fdesc);

	/* allocate and map device memory */
	rc = cuInit(0);
	cuda_exit_on_error(rc, "cuInit");

	if (device_index < 0)
	{
		int		count;

		rc = cuDeviceGetCount(&count);
		cuda_exit_on_error(rc, "cuDeviceGetCount");

		for (device_index = 0; device_index < count; device_index++)
		{
			rc = cuDeviceGet(&cuda_device, device_index);
			cuda_exit_on_error(rc, "cuDeviceGet");

			rc = cuDeviceGetName(devname, sizeof(devname), cuda_device);
			cuda_exit_on_error(rc, "cuDeviceGetName");

			if (strstr(devname, "Tesla") != NULL ||
				strstr(devname, "Quadro") != NULL)
				break;
		}
		if (device_index == count)
		{
			fprintf(stderr, "No Tesla or Quadro GPUs are installed\n");
			return 1;
		}
	}
	else
	{
		rc = cuDeviceGet(&cuda_device, device_index);
		cuda_exit_on_error(rc, "cuDeviceGet");

		rc = cuDeviceGetName(devname, sizeof(devname), cuda_device);
		cuda_exit_on_error(rc, "cuDeviceGetName");
	}

	/* print test scenario */
	printf("GPU[%d] %s - file: %s", device_index, devname, filename);
	if (filesize < (4UL << 10))
		printf(", i/o size: %zuB", filesize);
	else if (filesize < (4UL << 20))
		printf(", i/o size: %.2fKB", (double)filesize / (double)(1UL << 10));
	else if (filesize < (4UL << 30))
		printf(", i/o size: %.2fMB", (double)filesize / (double)(1UL << 20));
	else
		printf(", i/o size: %.2fGB", (double)filesize / (double)(1UL << 30));
	if (test_by_vfs)
		printf(" by VFS (i/o unitsz: %zuKB)", vfs_io_size >> 10);

	printf(", buffer %zuMB x %d\n",
		   segment_sz >> 20, nr_segments);

	rc = cuCtxCreate(&cuda_context, CU_CTX_SCHED_AUTO, cuda_device);
	cuda_exit_on_error(rc, "cuCtxCreate");

	rc = cuMemAlloc(&cuda_devptr, buffer_size);
	cuda_exit_on_error(rc, "cuMemAlloc");

	rc = cuMemsetD32(cuda_devptr, 0x41424344,
					 segment_sz * nr_segments / sizeof(int));
	cuda_exit_on_error(rc, "cuMemsetD32");

	mgmem_handle = ioctl_map_gpu_memory(cuda_devptr, buffer_size);

	/* test execution */
	if (test_by_vfs)
		exec_test_by_vfs(cuda_devptr, mgmem_handle,
						 filename, fdesc, filesize);
	else
		exec_test_by_strom(cuda_devptr, mgmem_handle,
						   filename, fdesc, filesize);
	return 0;
}
