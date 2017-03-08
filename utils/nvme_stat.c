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
#include "utils_common.h"

static void
print_mean(uint64_t N, uint64_t clocks, double clock_per_sec)
{
	double		value;

	if (N == 0)
	{
		printf("       ----");
		return;
	}

	value = (double)(clocks / N) / clock_per_sec;
	if (value > 2.0)			/* 2.0s */
		printf(" % 9.2fs", value);
	else if (value > 0.005)		/* 5ms */
		printf(" % 8.2fms", value * 1000.0);
	else if (value > 0.000005)	/* 5us */
		printf(" % 8.2fus", value * 1000000.0);
	else
		printf(" % 8.0fns", value * 1000000000.0);
}

static void
print_stat(int loop, StromCmd__StatInfo *p, StromCmd__StatInfo *c,
		   struct timeval *tv1, struct timeval *tv2)
{
#define DECL_DIFF(C,P,FIELD)	uint64_t FIELD = (C)->FIELD - (P)->FIELD;
	DECL_DIFF(c,p,nr_ssd2gpu);
	DECL_DIFF(c,p,clk_ssd2gpu);
	DECL_DIFF(c,p,nr_setup_prps);
	DECL_DIFF(c,p,clk_setup_prps);
	DECL_DIFF(c,p,nr_submit_dma);
	DECL_DIFF(c,p,clk_submit_dma);
	DECL_DIFF(c,p,nr_wait_dtask);
	DECL_DIFF(c,p,clk_wait_dtask);
	DECL_DIFF(c,p,nr_wrong_wakeup);
	DECL_DIFF(c,p,nr_debug1);
	DECL_DIFF(c,p,nr_debug2);
	DECL_DIFF(c,p,nr_debug3);
	DECL_DIFF(c,p,nr_debug4);
	DECL_DIFF(c,p,clk_debug1);
	DECL_DIFF(c,p,clk_debug2);
	DECL_DIFF(c,p,clk_debug3);
	DECL_DIFF(c,p,clk_debug4);
#undef DECL_DIFF
	double		interval;
	double		clocks_per_sec;

	interval = ((double)((tv2->tv_sec - tv1->tv_sec) * 1000000 +
						 (tv2->tv_usec - tv1->tv_usec))) / 1000000.0;
	clocks_per_sec = (double)(c->tsc - p->tsc) / interval;

	if (loop % 25 == 0)
	{
		printf("    avg-dma   avg-prps avg-submit   avg-wait"
			   " bad-wakeup   DMA(cur)   DMA(max)");
		if (c->has_debug)
			printf("     debug1     debug2     debug3     debug4");
		putchar('\n');
	}
	print_mean(nr_ssd2gpu, clk_ssd2gpu, clocks_per_sec);
	print_mean(nr_setup_prps, clk_setup_prps, clocks_per_sec);
	print_mean(nr_submit_dma, clk_submit_dma, clocks_per_sec);
	print_mean(nr_wait_dtask, clk_wait_dtask, clocks_per_sec);
	printf(" %10lu %10lu %10lu",
		   nr_wrong_wakeup,
		   c->cur_dma_count,
		   c->max_dma_count);
	if (c->has_debug)
	{
		print_mean(nr_debug1, clk_debug1, clocks_per_sec);
		print_mean(nr_debug2, clk_debug2, clocks_per_sec);
		print_mean(nr_debug3, clk_debug3, clocks_per_sec);
		print_mean(nr_debug4, clk_debug4, clocks_per_sec);
	}
	putchar('\n');
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

	if (interval > 0)
	{
		for (loop=-1; ; loop++)
		{
			memset(&curr_stat, 0, sizeof(StromCmd__StatInfo));
			curr_stat.version = 1;
			if (nvme_strom_ioctl(STROM_IOCTL__STAT_INFO, &curr_stat))
				ELOG(errno, "failed on ioctl(STROM_IOCTL__STAT_INFO)");

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
		if (nvme_strom_ioctl(STROM_IOCTL__STAT_INFO, &curr_stat))
			ELOG(errno, "failed on ioctl(STROM_IOCTL__STAT_INFO)");

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
		if (curr_stat.has_debug)
			printf("nr_debug1:       %lu\n"
				   "clk_debug1:      %lu\n"
				   "nr_debug2:       %lu\n"
				   "clk_debug2:      %lu\n"
				   "nr_debug3:       %lu\n"
				   "clk_debug3:      %lu\n"
				   "nr_debug4:       %lu\n"
				   "clk_debug4:      %lu\n",
				   (unsigned long)curr_stat.nr_debug1,
				   (unsigned long)curr_stat.clk_debug1,
				   (unsigned long)curr_stat.nr_debug2,
				   (unsigned long)curr_stat.clk_debug2,
				   (unsigned long)curr_stat.nr_debug3,
				   (unsigned long)curr_stat.clk_debug3,
				   (unsigned long)curr_stat.nr_debug4,
				   (unsigned long)curr_stat.clk_debug4);
	}
	return 0;
}
