/*
 * ssd2ram_test.c
 *
 * A stand-alone test tool for SSD2RAM feature
 * --------
 * Copyright 2017 (C) KaiGai Kohei <kaigai@kaigai.gr.jp>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2,
 * as published by the Free Software Foundation.
 */
#define _GNU_SOURCE
#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sched.h>
#include <unistd.h>
#include "utils_common.h"

static char		   *source_filename = NULL;
static int			source_fdesc = -1;
static struct stat	source_fstat;
static size_t		source_fpos = 0;
static int			numa_node_id = -1;
static int			proc_node_id = -1;		/* process's NUMA-Id */
static int			enable_checks = 0;
static int			num_processes = 0;		/* single process in default */
static size_t		buffer_size = (32UL << 20);		/* 32MB in default */
static long			total_memcpy_wait = 0;	/* in ms */
static long			total_nr_ram2ram = 0;
static long			total_nr_ssd2ram = 0;
static long			total_nr_dma_submit = 0;
static long			total_nr_dma_blocks = 0;

/*
 * Run STROM_IOCTL__CHECK_FILE
 */
static int
run_ioctl_check_file(int fdesc)
{
	StromCmd__CheckFile uarg;

	memset(&uarg, 0, sizeof(uarg));
    uarg.fdesc = fdesc;

	if (nvme_strom_ioctl(STROM_IOCTL__CHECK_FILE, &uarg))
		ELOG(errno, "failed on ioctl(STROM_IOCTL__CHECK_FILE, '%s')",
			 source_filename);

	printf("file: %s, numa-id: %d, DMA64: %s\n",
		   source_filename,
		   uarg.numa_node_id,
		   uarg.support_dma64 ? "supported" : "unsupported");
	if (!uarg.support_dma64)
		exit(0);
	return uarg.numa_node_id;
}

/*
 * Bind this process to the suggested NUMA node
 */
static void
setup_cpu_affinity(int node_id)
{
#define BITSPERINT		(8 * sizeof(int))
	char		namebuf[256];
	FILE	   *filp;
	int			c, cpuid;
	cpu_set_t	mask;

	/* nothing to do, if no numa binding */
	if (node_id < 0)
		return;
	/* try to get CPU list */
	snprintf(namebuf, sizeof(namebuf),
			 "/sys/devices/system/node/node%d/cpulist", node_id);
	filp = fopen(namebuf, "rb");
	if (!filp)
		ELOG(errno, "failed on fopen('%s')", namebuf);

	cpuid = -1;
	CPU_ZERO(&mask);
	do {
		c = fgetc(filp);
		if (isdigit(c))
			cpuid = (cpuid >= 0 ? 10 * cpuid + (c - '0') : (c - '0'));
		else if (c == ',' || c == '\n' || c == EOF)
		{
			CPU_SET(cpuid, &mask);
			cpuid = -1;
		}
		else
			ELOG(EINVAL, "unexpected character at %s (%c)", namebuf, c);
	} while (c != '\n' && c != EOF);

	if (sched_setaffinity(0, sizeof(mask), &mask))
		ELOG(errno, "failed on sched_setaffinity(2)");

	fclose(filp);
}






static void *
alloc_dma_buffer(int node_id)
{
	StromCmd__AllocDMABuffer cmd;
	void	   *buffer;

	memset(&cmd, 0, sizeof(StromCmd__AllocDMABuffer));
	cmd.length = buffer_size;
	cmd.node_id = node_id;

	if (nvme_strom_ioctl(STROM_IOCTL__ALLOC_DMA_BUFFER, &cmd))
		ELOG(errno, "failed on ioctl(STROM_IOCTL__ALLOC_DMA_BUFFER)");

	buffer = mmap(NULL, buffer_size,
				  PROT_READ | PROT_WRITE,
				  MAP_SHARED,
				  cmd.dmabuf_fdesc, 0);
	if (buffer == MAP_FAILED)
		ELOG(errno, "failed on mmap(2) with DMA buffer FD");

	return buffer;
}

static void *
ssd2ram_worker(void *__args__)
{
	StromCmd__MemCpySsdToRam cmd;
	char	   *dma_buffer;
	unsigned long *dma_tasks;
	uint32_t   *chunk_ids;
	size_t		unitsz = (1UL << 20);	/* 1MB unit size */
	int			n_units = (buffer_size / unitsz);
	int			rindex;		/* read index */
	int			windex;		/* wait index */
	int			i;
	long		memcpy_wait = 0;
	long		nr_ram2ram = 0;
	long		nr_ssd2ram = 0;
	long		nr_dma_submit = 0;
	long		nr_dma_blocks = 0;
	struct timeval tv1, tv2;

	dma_tasks = malloc(sizeof(unsigned long) * n_units);
	if (!dma_tasks)
		ELOG(errno, "out of memory");
	chunk_ids = malloc(sizeof(uint32_t) * (unitsz / BLCKSZ));
	if (!chunk_ids)
		ELOG(errno, "out of memory");

	/* allocation of the DMA buffer */
	dma_buffer = alloc_dma_buffer(proc_node_id);

	for (;;)
	{
		size_t	fpos = __sync_fetch_and_add(&source_fpos, unitsz);

		if (fpos >= source_fstat.st_size)
			break;
		/* wait until DMA buffer getting available */
		i = rindex++ % n_units;
		if (i == windex)
		{
			StromCmd__MemCpyWait	__cmd;

			gettimeofday(&tv1, NULL);
			memset(&__cmd, 0, sizeof(__cmd));
			__cmd.dma_task_id	= dma_tasks[windex++];
			if (nvme_strom_ioctl(STROM_IOCTL__MEMCPY_WAIT, &__cmd))
				ELOG(errno, "failed on ioctl(STROM_IOCTL__MEMCPY_WAIT)");
			gettimeofday(&tv2, NULL);

			memcpy_wait += ((tv2.tv_sec * 1000 + tv2.tv_usec / 1000) -
							(tv1.tv_sec * 1000 + tv1.tv_usec / 1000));
			/*
			 * TODO: data corruption check here
			 */
		}

		/* setup MEMCPY_SSD2RAM command */
		memset(&cmd, 0, sizeof(cmd));
		cmd.dest_uaddr	= dma_buffer + rindex * unitsz;
		cmd.file_desc	= source_fdesc;
		if (fpos + unitsz <= source_fstat.st_size)
			cmd.nr_chunks = (unitsz / BLCKSZ);
		else
			cmd.nr_chunks = (source_fstat.st_size - fpos) / BLCKSZ;
		cmd.chunk_sz	= BLCKSZ;
		cmd.relseg_sz	= 0;
		cmd.chunk_ids	= chunk_ids;

		for (i=0; i < cmd.nr_chunks; i++)
			cmd.chunk_ids[i] = fpos + i * BLCKSZ;

		if (nvme_strom_ioctl(STROM_IOCTL__MEMCPY_SSD2RAM, &cmd))
			ELOG(errno, "failed on ioctl(STROM_IOCTL__MEMCPY_SSD2RAM)");

		dma_tasks[i]	= cmd.dma_task_id;
		nr_ram2ram		+= cmd.nr_ram2ram;
		nr_ssd2ram		+= cmd.nr_ssd2ram;
		nr_dma_submit	+= cmd.nr_dma_submit;
		nr_dma_blocks	+= cmd.nr_dma_blocks;
	}
	/* collect statistics */
	__sync_fetch_and_add(&total_memcpy_wait, memcpy_wait);
	__sync_fetch_and_add(&total_nr_ram2ram, nr_ram2ram);
	__sync_fetch_and_add(&total_nr_ssd2ram, nr_ssd2ram);
	__sync_fetch_and_add(&total_nr_dma_submit, nr_dma_submit);
	__sync_fetch_and_add(&total_nr_dma_blocks, nr_dma_blocks);

	return NULL;
}

static void
print_results(long time_ms)
{
	size_t		fsize = source_fstat.st_size;
	double		throughput = (double)fsize / ((double)time_ms / 1000.0);

	if (fsize < (4UL << 10))
		printf("i/o size: %zuB", fsize);
	else if (fsize < (4UL << 20))
		printf("i/o size: %zuKB", fsize >> 10);
	else if (fsize < (4UL << 30))
		printf("i/o size: %.2fMB", (double)fsize / (double)(1UL << 20));
	else if (fsize < (4UL << 40))
		printf("i/o size: %.2fGB", (double)fsize / (double)(1UL << 30));
	else
		printf("i/o size: %.3fTB", (double)fsize / (double)(1UL << 40));

	if (time_ms < 4000UL)
		printf(", time: %ldms", time_ms);
	else
		printf(", time: %.2fs", (double)time_ms / 1000.0);

	if (throughput < (double)(4UL << 10))
		printf(", throughput: %zuB/s\n", (size_t)throughput);
	else if (throughput < (double)(4UL << 20))
		printf(", throughput: %.2fKB/s\n", throughput / (double)(1UL << 10));
	else if (throughput < (double)(4UL << 30))
		printf(", throughput: %.2fMB/s\n", throughput / (double)(1UL << 20));
	else
		printf(", throughput: %.3fGB/s\n", throughput / (double)(1UL << 30));

	if (total_memcpy_wait < 4000)
		printf("wait time: %ldms", total_memcpy_wait);
	else
		printf("wait time: %.2fs", (double)total_memcpy_wait / 1000.0);

	if (total_nr_ram2ram > 0 || total_nr_ssd2ram > 0)
		printf(", nr_ram2ram: %ld, nr_ssd2ram: %ld",
			   total_nr_ram2ram,
			   total_nr_ssd2ram);
	if (total_nr_dma_submit > 0)
		printf(", avg DMA blocks: %.2f",
			   (double)total_nr_dma_blocks /
			   (double)total_nr_dma_submit);
}

static void
usage(const char *argv0)
{
	fprintf(stderr,
			"usage: %s [OPTIONS] <filename>\n"
			"  -c : check SSD2RAM capability of the file\n"
			"  -n <num worker threads>\n"
			"  -p <numa node-id of process>\n"
			"  -s <buffer size in MB>\n",
			basename(strdup(argv0)));
	exit(1);
}

int
main(int argc, char *argv[])
{
	struct timeval	tv1, tv2;
	int				c, i;

	while ((c = getopt(argc, argv, "cn:p:s:h")) >= 0)
	{
		switch (c)
		{
			case 'c':
				enable_checks = 1;
				break;
			case 'n':
				num_processes = atoi(optarg);
				break;
			case 'p':
				proc_node_id = atoi(optarg);
				break;
			case 's':
				buffer_size = (size_t)atol(optarg) << 20;	/* size in MB */
				break;
			default:
				usage(argv[0]);
				break;
		}

	}
	if (optind + 1 != argc)
		usage(argv[0]);
	/* Open source file */
	source_filename = argv[optind];
	source_fdesc = open(source_filename, O_RDONLY);
	if (source_fdesc < 0)
		ELOG(errno, "failed on open('%s')", source_filename);
	if (fstat(source_fdesc, &source_fstat))
		ELOG(errno, "failed on fstat('%s')", source_filename);

	/* Get NUMA node-id */
	numa_node_id = run_ioctl_check_file(source_fdesc);
	if (enable_checks)
		return 0;
	/* Process works close to storage, if no configuration */
	if (proc_node_id < 0)
		proc_node_id = numa_node_id;
	/* Setup CPU affinity based on NUMA node-id */
	setup_cpu_affinity(proc_node_id);
	/* Launch worker threads if any */
	gettimeofday(&tv1, NULL);
	if (num_processes > 0)
	{
		pthread_t  *thread_ids;

		thread_ids = malloc(sizeof(pthread_t) * num_processes);
		if (!thread_ids)
			ELOG(errno, "out of memory");
		for (i=0; i < num_processes; i++)
		{
			if (pthread_create(thread_ids + i, NULL,
							   ssd2ram_worker, NULL))
				ELOG(errno, "failed on pthread_create");
		}

		for (i=0; i < num_processes; i++)
		{
			if (pthread_join(thread_ids[i], NULL))
				ELOG(errno, "failed on pthread_join");
		}
	}
	else
	{
		/* run by myself */
		ssd2ram_worker(NULL);
	}
	gettimeofday(&tv2, NULL);

	print_results((tv2.tv_sec * 1000 + tv2.tv_usec / 1000) -
				  (tv1.tv_sec * 1000 + tv1.tv_usec / 1000));
	return 0;
}
