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
#include <sched.h>
#include <unistd.h>
#include "utils_common.h"

static char	   *source_filename = NULL;
static int		source_fdesc = -1;
static int		numa_node_id = -1;
static int		enable_checks = 0;
static int		num_processes = 0;		/* single process in default */
static size_t	buffer_size = (32UL << 20);		/* 32MB in default */
static size_t	fpos = 0;

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
alloc_dma_buffer(void)
{
	StromCmd__AllocDMABuffer cmd;
	void	   *buffer;

	memset(&cmd, 0, sizeof(StromCmd__AllocDMABuffer));
	cmd.length = buffer_size;
	cmd.node_id = numa_node_id;

	if (nvme_strom_ioctl(STROM_IOCTL__ALLOC_DMA_BUFFER, &cmd))
		ELOG(errno, "failed on ioctl(STROM_IOCTL__ALLOC_DMA_BUFFER)");

	printf("DMA buffer on FD=%d\n", cmd.dmabuf_fdesc);

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
	char	   *dma_buffer;

	dma_buffer = alloc_dma_buffer();
	// get file pos with atomic ops
	// issue async dma
	// wait for the first dma task
	// iterate until file end



	printf("mapped on %p\n", dma_buffer);
	/* vm_fault */
	memset(dma_buffer, 0xdeadbeaf, buffer_size);



	return NULL;
}

static void
usage(const char *argv0)
{
	fprintf(stderr,
			"usage: %s [OPTIONS] <filename>\n"
			"  -c : check SSD2RAM capability of the file\n"
			"  -n <num worker threads>\n"
			"  -s <buffer size in MB>\n",
			basename(strdup(argv0)));
	exit(1);
}

int
main(int argc, char *argv[])
{
	int		c, i;

	while ((c = getopt(argc, argv, "cn:s:h")) >= 0)
	{
		switch (c)
		{
			case 'c':
				enable_checks = 1;
				break;
			case 'n':
				num_processes = atoi(optarg);
				break;
			case 's':
				buffer_size = atol(optarg) << 20;	/* size in MB */
				break;
			default:
				usage(argv[0]);
				break;
		}

	}
	if (optind + 1 != argc)
		usage(argv[0]);
	source_filename = argv[optind];
	source_fdesc = open(source_filename, O_RDONLY);
	if (source_fdesc < 0)
		ELOG(errno, "failed on open('%s')", source_filename);
	/* Get NUMA node-id */
	numa_node_id = run_ioctl_check_file(source_fdesc);
	if (enable_checks)
		return 0;
	/* Setup CPU affinity based on NUMA node-id */
	setup_cpu_affinity(numa_node_id);
	/* Launch worker threads if any */
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
	return 0;
}
