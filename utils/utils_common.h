/*
 * utils_common.h
 *
 * A set of macros for utility command of NVMe-Strom
 * --------
 * Copyright 2017 (C) KaiGai Kohei <kaigai@kaigai.gr.jp>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2,
 * as published by the Free Software Foundation.
 */
#ifndef UTILS_COMMON_H
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "../kmod/nvme_strom.h"

#define offsetof(type, field)	((long) &((type *)0)->field)
#define Max(a,b)				((a) > (b) ? (a) : (b))
#define Min(a,b)				((a) < (b) ? (a) : (b))
#define BLCKSZ					(8192)		/* usual PostgreSQL config */
#define RELSEG_SIZE				(131072)	/* usual PostgreSQL config */

#define ELOG(rc,fmt,...)							\
	do {											\
		fprintf(stderr,"%s:%d " fmt " (%d:%s)\n",	\
				__FUNCTION__, __LINE__,				\
				##__VA_ARGS__,						\
				(rc), strerror(rc));				\
		exit(1);									\
	} while(0)

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
			ELOG(errno, "failed to open \"%s\"", NVME_STROM_IOCTL_PATHNAME);
	}
    return ioctl(fdesc_nvme_strom, cmd, arg);
}



#endif	/* UTILS_COMMON_H */
