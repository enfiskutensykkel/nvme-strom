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
#include <stdio.h>
#include <sys/mman.h>
#include <unistd.h>
#include "utils_common.h"

static int		numa_node_id = 0;
static size_t	buffer_size = (32UL << 20);		/* 32MB in default */


static void *
alloc_dma_buffer(void)
{
	StromCmd__AllocateDMABuffer cmd;
	void	   *buffer;

	memset(&cmd, 0, sizeof(StromCmd__AllocateDMABuffer));
	cmd.length = buffer_size;
	cmd.node_id = numa_node_id;

	if (nvme_strom_ioctl(STROM_IOCTL__ALLOCATE_DMA_BUFFER, &cmd))
		ELOG(errno, "failed on ioctl(STROM_IOCTL__ALLOCATE_DMA_BUFFER)");

	printf("DMA buffer on FD=%d\n", cmd.dmabuf_fdesc);

	buffer = mmap(NULL, buffer_size,
				  PROT_READ | PROT_WRITE,
				  MAP_SHARED,
				  cmd.dmabuf_fdesc, 0);
	if (buffer == MAP_FAILED)
		ELOG(errno, "failed on mmap(2) with DMA buffer FD");

	return buffer;
}





static void
usage(const char *argv0)
{
	fprintf(stderr,
			"usage: %s [OPTIONS]\n"
			"  -n <numa node>\n"
			"  -s <buffer size in MB>\n",
			basename(strdup(argv0)));
	exit(1);
}

int
main(int argc, char *argv[])
{
	int		c;
	void   *buffer;

	while ((c = getopt(argc, argv, "n:s:h")) >= 0)
	{
		switch (c)
		{
			case 'n':
				numa_node_id = atoi(optarg);
				break;
			case 's':
				buffer_size = atol(optarg) << 20;	/* size in MB */
				break;
			default:
				usage(argv[0]);
				break;
		}

	}
#if 0
	if (optind + 1 != argc)
		usage(argv[0]);
	filename = argv[optind];
#endif
	buffer = alloc_dma_buffer();

	printf("mapped on %p\n", buffer);
	/* vm_fault */
	memset(buffer, 0xdeadbeaf, buffer_size);

	{
		char	cmd[1024];

		snprintf(cmd, sizeof(cmd), "ls -l /proc/%u/fd", getpid());
		system(cmd);
	}

	return 0;
}
