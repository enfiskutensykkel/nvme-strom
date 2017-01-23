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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "nvme_strom.h"

static double	clock_per_msec;

static void
print_stat(int loop, StromCmd__StatInfo *p, StromCmd__StatInfo *c)
{
	uint64_t	nr_ssd2gpu = c->nr_ssd2gpu - p->nr_ssd2gpu;
	uint64_t	clk_ssd2gpu = c->clk_ssd2gpu - p->clk_ssd2gpu;
	uint64_t	nr_wait_dtask = c->nr_wait_dtask - p->nr_wait_dtask;
	uint64_t	clk_wait_dtask = c->clk_wait_dtask - p->clk_wait_dtask;
	uint64_t	nr_wrong_wakeup = c->nr_wrong_wakeup - p->nr_wrong_wakeup;

	if (loop % 25 == 0)
		printf("avg-dma   avg-wait   nr-wrong-wakeup\n");
	printf("%.2fms\t%.2fms\t%ld\n",
		   nr_ssd2gpu == 0 ? 0.0 :
		   (double)(clk_ssd2gpu / nr_ssd2gpu) / clock_per_msec,
		   nr_wait_dtask == 0 ? 0.0 :
		   (double)(clk_wait_dtask / nr_wait_dtask) / clock_per_msec,
		   nr_wrong_wakeup);
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
		long	clock_per_sec;

		clock_per_sec = sysconf(_SC_CLK_TCK);
		if (clock_per_sec < 0)
		{
			fprintf(stderr, "failed on sysconf(_SC_CLK_TCK): %m\n");
			return 1;
		}
		clock_per_msec = (double)clock_per_sec / 1000.0;

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
			if (loop >= 0)
				print_stat(loop, &prev_stat, &curr_stat);
			sleep(interval);
			memcpy(&prev_stat, &curr_stat, sizeof(StromCmd__StatInfo));
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
		printf("nr_ssd2gpu:      %lu\n"
			   "clk_ssd2gpu:     %lu\n"
			   "nr_wait_dtask:   %lu\n"
			   "clk_wait_dtask:  %lu\n"
			   "nr_wrong_wakeup: %lu\n",
			   (unsigned long)curr_stat.nr_ssd2gpu,
			   (unsigned long)curr_stat.clk_ssd2gpu,
			   (unsigned long)curr_stat.nr_wait_dtask,
			   (unsigned long)curr_stat.clk_wait_dtask,
			   (unsigned long)curr_stat.nr_wrong_wakeup);
	}
	close(fdesc);
	return 0;
}
