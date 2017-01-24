/*
 * nvme_stat.c
 *
 * A utility command to collect run-time statistics of NVMe-Strom
 * --------
 * Copyright 2017 (C) KaiGai Kohei <kaigai@kaigai.gr.jp>
 * Copyright 2017 (C) The PG-Strom Development Team
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2,
 * as published by the Free Software Foundation.
 */
#include <fcntl.h>
#include <libgen.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include "nvme_strom.h"

static void
print_mean(uint64_t N, uint64_t clocks, double clock_per_sec)
{
	double		value;

	if (N == 0)
	{
		printf("----");
		return;
	}

	value = (double)(clocks / N) / clock_per_sec;
	if (value > 2.0)			/* 2.0s */
		printf("%.2fs", value);
	else if (value > 0.005)		/* 5ms */
		printf("%.2fms", value * 1000.0);
	else if (value > 0.000005)	/* 5us */
		printf("%.2fus", value * 1000000.0);
	else
		printf("%.0fns", value * 1000000000.0);
}

static void
print_stat(int loop, StromCmd__StatInfo *p, StromCmd__StatInfo *c,
		   struct timeval *tv1, struct timeval *tv2)
{
  	uint64_t	nr_ssd2gpu = c->nr_ssd2gpu - p->nr_ssd2gpu;
	uint64_t	clk_ssd2gpu = c->clk_ssd2gpu - p->clk_ssd2gpu;
	uint64_t	nr_setup_prps = c->nr_setup_prps - p->nr_setup_prps;
	uint64_t	clk_setup_prps = c->clk_setup_prps - p->clk_setup_prps;
	uint64_t	nr_submit_dma = c->nr_submit_dma - p->nr_submit_dma;
	uint64_t	clk_submit_dma = c->clk_submit_dma - p->clk_submit_dma;
	uint64_t	nr_wait_dtask = c->nr_wait_dtask - p->nr_wait_dtask;
	uint64_t	clk_wait_dtask = c->clk_wait_dtask - p->clk_wait_dtask;
	uint64_t	nr_wrong_wakeup = c->nr_wrong_wakeup - p->nr_wrong_wakeup;
	double		interval;
	double		clocks_per_sec;

	interval = ((double)((tv2->tv_sec - tv1->tv_sec) * 1000000 +
						 (tv2->tv_usec - tv1->tv_usec))) / 1000000.0;
	clocks_per_sec = (double)(c->tsc - p->tsc) / interval;

	if (loop % 25 == 0)
		printf("avg-dma  avg-prps  avg-submit  avg-wait  bad-wakeup  DMA(cur)  DMA(max)\n");
	print_mean(nr_ssd2gpu, clk_ssd2gpu, clocks_per_sec);
	putchar('\t');
	print_mean(nr_setup_prps, clk_setup_prps, clocks_per_sec);
	putchar('\t');
	print_mean(nr_submit_dma, clk_submit_dma, clocks_per_sec);
	putchar('\t');
	print_mean(nr_wait_dtask, clk_wait_dtask, clocks_per_sec);
	printf("\t%lu\t%lu\t%lu\n",
		   nr_wrong_wakeup,
		   c->cur_dma_count,
		   c->max_dma_count);
}

static void
usage(const char *command_name)
{
	fprintf(stderr,
			"usage: %s [<interval>]\n",
			basename(strdup(command_name)));
	exit(1);
}

int
main(int argc, char *argv[])
{
	int		fdesc;
	int		loop;
	int		interval;
	int		c;
	StromCmd__StatInfo	curr_stat;
	StromCmd__StatInfo	prev_stat;
	struct timeval		tv1, tv2;

	while ((c = getopt(argc, argv, "h")) >= 0)
	{
		switch (c)
		{
			case 'h':
			default:
				usage(argv[0]);
				break;
		}
	}
	if (optind == argc)
		interval = -1;
	else if (optind + 1 == argc)
		interval = atoi(argv[optind]);
	else
		usage(argv[0]);

	fdesc = open(NVME_STROM_IOCTL_PATHNAME, O_RDONLY);
	if (fdesc < 0)
	{
		fprintf(stderr, "failed to open \"%s\" : %m\n",
				NVME_STROM_IOCTL_PATHNAME);
		return 1;
	}

	if (interval > 0)
	{
		for (loop=-1; ; loop++)
		{
			memset(&curr_stat, 0, sizeof(StromCmd__StatInfo));
			curr_stat.version = 1;
			if (ioctl(fdesc, STROM_IOCTL__STAT_INFO, &curr_stat))
			{
				fprintf(stderr,
						"failed on ioctl(STROM_IOCTL__STAT_INFO): %m\n");
				return 1;
			}
			gettimeofday(&tv2, NULL);
			if (loop >= 0)
				print_stat(loop, &prev_stat, &curr_stat, &tv1, &tv2);
			sleep(interval);
			memcpy(&prev_stat, &curr_stat, sizeof(StromCmd__StatInfo));
			tv1 = tv2;
		}
	}
	else
	{
		memset(&curr_stat, 0, sizeof(StromCmd__StatInfo));
		curr_stat.version = 1;
		if (ioctl(fdesc, STROM_IOCTL__STAT_INFO, &curr_stat))
		{
			fprintf(stderr, "failed on ioctl(STROM_IOCTL__STAT_INFO): %m\n");
			return 1;
		}
		printf("tsc:             %lu\n"
			   "nr_ssd2gpu:      %lu\n"
			   "clk_ssd2gpu:     %lu\n"
			   "nr_setup_prps:   %lu\n"
			   "clk_setup_prps:  %lu\n"
			   "nr_submit_dma:   %lu\n"
			   "clk_submit_dma:  %lu\n"
			   "nr_wait_dtask:   %lu\n"
			   "clk_wait_dtask:  %lu\n"
			   "nr_wrong_wakeup: %lu\n"
			   "cur_dma_count:   %lu\n"
			   "max_dma_count:   %lu\n",
			   (unsigned long)curr_stat.tsc,
			   (unsigned long)curr_stat.nr_ssd2gpu,
			   (unsigned long)curr_stat.clk_ssd2gpu,
			   (unsigned long)curr_stat.nr_setup_prps,
			   (unsigned long)curr_stat.clk_setup_prps,
			   (unsigned long)curr_stat.nr_submit_dma,
			   (unsigned long)curr_stat.clk_submit_dma,
			   (unsigned long)curr_stat.nr_wait_dtask,
			   (unsigned long)curr_stat.clk_wait_dtask,
			   (unsigned long)curr_stat.nr_wrong_wakeup,
			   (unsigned long)curr_stat.cur_dma_count,
			   (unsigned long)curr_stat.max_dma_count);
	}
	close(fdesc);
	return 0;
}
