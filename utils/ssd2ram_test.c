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

static char	   *source_filename = NULL;
static int		numa_node_id = 0;
static int		enable_checks = 0;
static size_t	buffer_size = (32UL << 20);		/* 32MB in default */

/*
 * Run STROM_IOCTL__CHECK_FILE
 */
static void
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





static void
usage(const char *argv0)
{
	fprintf(stderr,
			"usage: %s [OPTIONS] <filename>\n"
			"  -c : check SSD2RAM capability of the file\n"
			"  -n <numa node>\n"
			"  -s <buffer size in MB>\n",
			basename(strdup(argv0)));
	exit(1);
}

int
main(int argc, char *argv[])
{
	void   *buffer;
	int		c, fdesc;

	while ((c = getopt(argc, argv, "cn:s:h")) >= 0)
	{
		switch (c)
		{
			case 'c':
				enable_checks = 1;
				break;
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
	if (optind + 1 != argc)
		usage(argv[0]);
	source_filename = argv[optind];
	fdesc = open(source_filename, O_RDONLY);
	if (fdesc < 0)
		ELOG(errno, "failed on open('%s')", source_filename);

	/* Run STROM_IOCTL__CHECK_FILE only */
	if (enable_checks)
	{
		run_ioctl_check_file(fdesc);
		return 0;
	}




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
