/*
 * NVMe-Strom
 *
 * A Linux kernel driver to support SSD-to-GPU P2P DMA.
 *
 * Copyright (C) 2016 KaiGai Kohei <kaigai@kaigai.gr.jp>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */
#include <asm/uaccess.h>
#include <linux/anon_inodes.h>
#include <linux/blk-mq.h>
#include <linux/buffer_head.h>
#include <linux/fdtable.h>
#include <linux/file.h>
#include <linux/fs.h>
#include <linux/idr.h>
#include <linux/kallsyms.h>
#include <linux/kernel.h>
#include <linux/magic.h>
#include <linux/major.h>
#include <linux/moduleparam.h>
#include <linux/nvme.h>
#include <linux/pci.h>
#include <linux/proc_fs.h>
#include <linux/sched.h>
#include <linux/version.h>
#include <uapi/linux/nvme_ioctl.h>
#include <generated/utsrelease.h>
#include "nv-p2p.h"
#include "nvme_strom.h"

/* determine the target kernel to build */
#if defined(RHEL_MAJOR) && (RHEL_MAJOR == 7)
#include "rhel7_local.h"	/* local headers in RHEL7/CentOS7 kernel */
#else
#error Not a supported Linux Distribution
#endif

/* utility macros */
#define Assert(cond)												\
	do {															\
		if (!(cond)) {												\
			panic("assertion failure (" #cond ") at %s:%d, %s\n",	\
				  __FILE__, __LINE__, __FUNCTION__);				\
		}															\
	} while(0)
#define lengthof(array)	(sizeof (array) / sizeof ((array)[0]))
#define Max(a,b)		((a) > (b) ? (a) : (b))
#define Min(a,b)		((a) < (b) ? (a) : (b))

/* message verbosity control */
static int	verbose = 0;
module_param(verbose, int, 0644);
MODULE_PARM_DESC(verbose, "turn on/off debug message");
/* run-time statistics */
static int	stat_info = 1;
module_param(stat_info, int, 0644);
MODULE_PARM_DESC(stat_info, "turn on/off run-time statistics");
static atomic64_t	stat_nr_ssd2gpu = ATOMIC64_INIT(0);
static atomic64_t	stat_clk_ssd2gpu = ATOMIC64_INIT(0);
static atomic64_t	stat_nr_setup_prps = ATOMIC64_INIT(0);
static atomic64_t	stat_clk_setup_prps = ATOMIC64_INIT(0);
static atomic64_t	stat_nr_submit_dma = ATOMIC64_INIT(0);
static atomic64_t	stat_clk_submit_dma = ATOMIC64_INIT(0);
static atomic64_t	stat_nr_wait_dtask = ATOMIC64_INIT(0);
static atomic64_t	stat_clk_wait_dtask = ATOMIC64_INIT(0);
static atomic64_t	stat_nr_wrong_wakeup = ATOMIC64_INIT(0);
static atomic64_t	stat_cur_dma_count = ATOMIC64_INIT(0);
static atomic64_t	stat_max_dma_count = ATOMIC64_INIT(0);
static atomic64_t	stat_nr_debug1 = ATOMIC64_INIT(0);
static atomic64_t	stat_nr_debug2 = ATOMIC64_INIT(0);
static atomic64_t	stat_nr_debug3 = ATOMIC64_INIT(0);
static atomic64_t	stat_nr_debug4 = ATOMIC64_INIT(0);
static atomic64_t	stat_clk_debug1 = ATOMIC64_INIT(0);
static atomic64_t	stat_clk_debug2 = ATOMIC64_INIT(0);
static atomic64_t	stat_clk_debug3 = ATOMIC64_INIT(0);
static atomic64_t	stat_clk_debug4 = ATOMIC64_INIT(0);

static inline long
atomic64_max_return(long newval, atomic64_t *atomic_ptr)
{
	long	oldval, maxval;
	do {
		maxval = atomic64_read(atomic_ptr);
		if (newval <= maxval)
			break;
		oldval = atomic64_cmpxchg(atomic_ptr, maxval, newval);
	} while (oldval != maxval);
	return maxval;
}


#define prDebug(fmt, ...)												\
	do {																\
		if (verbose > 1)												\
			printk(KERN_ALERT "nvme-strom(%s:%d): " fmt "\n",			\
				   __FUNCTION__, __LINE__, ##__VA_ARGS__);				\
		else if (verbose)												\
			printk(KERN_ALERT "nvme-strom: " fmt "\n", ##__VA_ARGS__);	\
	} while(0)
#define prInfo(fmt, ...)						\
	printk(KERN_INFO "nvme-strom: " fmt "\n", ##__VA_ARGS__)
#define prNotice(fmt, ...)						\
	printk(KERN_NOTICE "nvme-strom: " fmt "\n", ##__VA_ARGS__)
#define prWarn(fmt, ...)						\
	printk(KERN_WARNING "nvme-strom: " fmt "\n", ##__VA_ARGS__)
#define prError(fmt, ...)						\
	printk(KERN_ERR "nvme-strom: " fmt "\n", ##__VA_ARGS__)

/* routines for extra symbols */
#define EXTRA_KSYMS_NEEDS_NVIDIA		1
#include "extra_ksyms.c"

/*
 * Macro definition for sector
 */
#define SECTOR_SHIFT		(9)
#define SECTOR_SIZE			(1UL << SECTOR_SHIFT)

/* procfs entry of "/proc/nvme-strom" */
static struct proc_dir_entry  *nvme_strom_proc = NULL;

/* rdtsc() is defined at msr.h if x86_64 */
#ifndef CONFIG_X86_64
#define rdtsc()				(0UL)
#endif

#include "pmemmap.c"

/*
 * strom_get_block - a generic version of get_block_t for the supported
 * filesystems. It assumes the target filesystem is already checked by
 * file_is_supported_nvme, so we have minimum checks here.
 */
#define XFS_SB_MAGIC	0x58465342		/* extra filesystem signature */

static inline int
strom_get_block(struct inode *inode, sector_t iblock,
				struct buffer_head *bh, int create)
{
	struct super_block	   *i_sb = inode->i_sb;

	if (i_sb->s_magic == EXT4_SUPER_MAGIC)
		return __ext4_get_block(inode, iblock, bh, create);
	else if (i_sb->s_magic == XFS_SB_MAGIC)
		return __xfs_get_blocks(inode, iblock, bh, create);
	else
		return -ENOTSUPP;
}

/*
 * ioctl_check_file - checks whether the supplied file descriptor is
 * capable to perform P2P DMA from NVMe SSD.
 * Here are various requirement on filesystem / devices.
 *
 * - application has permission to read the file.
 * - filesystem has to be Ext4 or XFS, because Linux has no portable way
 *   to identify device blocks underlying a particular range of the file.
 * - block device of the file has to be NVMe-SSD, managed by the inbox
 *   driver of Linux. RAID configuration is not available to use.
 * - file has to be larger than or equal to PAGE_SIZE, because Ext4/XFS
 *   are capable to have file contents inline, for very small files.
 */

/*
 * __extblock_is_supported_nvme - checker for BLOCK_EXT_MAJOR
 */
static int
__extblock_is_supported_nvme(struct block_device *blkdev,
							 int *p_numa_node_id,
							 int *p_support_dma64)
{
	struct gendisk	   *bd_disk = blkdev->bd_disk;
	struct nvme_ns	   *nvme_ns = (struct nvme_ns *)bd_disk->private_data;
	struct nvme_ctrl   *nvme_ctrl = nvme_ns->ctrl;
	struct device	   *this_dev = nvme_ctrl->dev;
	const char		   *dname;
	int					rc;

	/* 'devext' is wrapper of NVMe-SSD device */
	if (bd_disk->major != BLOCK_EXT_MAJOR)
	{
		prError("block device '%s' has unsupported major device number: %d",
				bd_disk->disk_name, bd_disk->major);
		return -ENOTSUPP;
	}

	/* disk_name should be 'nvme%dn%d' */
	dname = bd_disk->disk_name;
	if (dname[0] == 'n' &&
		dname[1] == 'v' &&
		dname[2] == 'm' &&
		dname[3] == 'e')
	{
		const char	   *pos = dname + 4;
		const char	   *pos_saved = pos;

		while (*pos >= '0' && *pos <='9')
			pos++;
		if (pos > pos_saved && *pos == 'n')
		{
			pos_saved = ++pos;

			while (*pos >= '0' && *pos <= '9')
				pos++;
			if (pos > pos_saved && *pos == '\0')
				dname = NULL;	/* ok, it is NVMe-SSD */
		}
	}

	if (dname)
	{
		prError("block device '%s' is not supported", dname);
		return -ENOTSUPP;
	}

	/* try to call ioctl for device ping */
	if (!bd_disk->fops->ioctl)
	{
		prError("block device '%s' does not provide ioctl",
				bd_disk->disk_name);
		return -ENOTSUPP;
	}

	rc = bd_disk->fops->ioctl(blkdev, 0, NVME_IOCTL_ID, 0UL);
	if (rc < 0)
	{
		prError("ioctl(NVME_IOCTL_ID) on '%s' returned an error: %d",
				bd_disk->disk_name, rc);
		return -ENOTSUPP;
	}

	/* Inform PCIe topolocy where the SSD device locates on */
	if (p_numa_node_id)
	{
#ifdef CONFIG_NUMA
		if (*p_numa_node_id < -1)
			*p_numa_node_id = this_dev->numa_node;
		else if (*p_numa_node_id > -1 &&
				 *p_numa_node_id != this_dev->numa_node)
			*p_numa_node_id = -1;
#else
		*p_numa_node_id = -1;
#endif
	}

	/* Check range of DMA destination address of the SSD device */
	if (p_support_dma64)
	{
		if (!this_dev->dma_mask ||
			*this_dev->dma_mask != DMA_BIT_MASK(64))
			*p_support_dma64 = 0;
	}
	return 0;	/* OK, we can assume this volume is raw NVMe-SSD */
}

/*
 * __mdblock_is_supported_nvme - checker for BLOCK_EXT_MAJOR
 */
static int
__mdblock_is_supported_nvme(struct block_device *blkdev,
							int *p_numa_node_id,
							int *p_support_dma64,
							struct r0conf **p_raid0_conf)
{
	struct gendisk *bd_disk = blkdev->bd_disk;
	const char	   *dname;
	struct mddev   *mddev;
	struct md_rdev *rdev;
	int				rc;

	/* 'md' logical RAID volume is expected */
    Assert(bd_disk->major == MD_MAJOR);

	dname = bd_disk->disk_name;
	if (dname[0] == 'm' &&
		dname[1] == 'd')
	{
		const char *pos = dname + 2;
		const char *pos_saved;

		if (pos[0] == '_' &&
			pos[1] == 'd')
			pos += 2;
		pos_saved = pos;
		while (*pos >= '0' && *pos <= '9')
			pos++;
		if (pos > pos_saved && *pos == '\0')
			dname = NULL;
	}

	if (dname)
	{
		prError("block device '%s' is not supported", dname);
		return -ENOTSUPP;
	}

	/*
	 * check md-raid configuration parameters
	 */
	mddev = bd_disk->private_data;

	if (!mddev || !mddev->pers)
	{
		prError("md-device '%s' is not ready to handle i/o",
				bd_disk->disk_name);
		return -ENOTSUPP;
	}

	if (mddev->raid_disks == 0)
	{
		prError("md-device '%s' contains no underlying disks",
				bd_disk->disk_name);
		return -ENOTSUPP;
	}

	if (mddev->level != 0 || mddev->layout != 0)
	{
		prError("md-device '%s' is not configured as RAID-0 volume",
				bd_disk->disk_name);
		return -ENOTSUPP;
	}

	if (mddev->chunk_sectors < (PAGE_CACHE_SIZE >> 9) ||
		(mddev->chunk_sectors & ((PAGE_CACHE_SIZE >> 9) - 1)) != 0)
	{
		prError("md-device '%s' has invalid stripe size: %zu",
				bd_disk->disk_name, (size_t)mddev->chunk_sectors << 9);
		return -ENOTSUPP;
	}

	/* check for each underlying devices */
	rdev_for_each(rdev, mddev)
	{
		rc = __extblock_is_supported_nvme(rdev->bdev,
										  p_numa_node_id,
										  p_support_dma64);
		if (rc)
		{
			prError("md-device '%s' - disk[%d] is not NVMe-SSD",
					bd_disk->disk_name, rdev->desc_nr);
			return rc;
		}
	}

	/* ok, MD RAID-0 volume consists of all NVMe-SSD devices */
	if (p_raid0_conf)
		*p_raid0_conf = mddev->private;

	return 0;
}

/*
 * file_is_supported_nvme
 */
static int
file_is_supported_nvme(struct file *filp,
					   int *p_numa_node_id,
					   int *p_support_dma64,
					   struct r0conf **p_raid0_conf)
{
	struct inode	   *f_inode = filp->f_inode;
	struct super_block *i_sb = f_inode->i_sb;
	struct file_system_type *s_type = i_sb->s_type;
	struct block_device *s_bdev = i_sb->s_bdev;
	struct gendisk	   *bd_disk = s_bdev->bd_disk;

	/*
	 * must have proper permission to the target file
	 */
	if ((filp->f_mode & FMODE_READ) == 0)
	{
		prError("process (pid=%u) has no permission to read file",
				current->pid);
		return -EACCES;
	}

	/*
	 * check whether it is on supported filesystem
	 *
	 * MEMO: Linux VFS has no reliable way to lookup underlying block
	 *   number of individual files (and, may be impossible in some
	 *   filesystems), so our module solves file offset <--> block number
	 *   on a part of supported filesystems.
	 *
	 * supported: ext4, xfs
	 */
	if (!((i_sb->s_magic == EXT4_SUPER_MAGIC &&
		   strcmp(s_type->name, "ext4") == 0 &&
		   s_type->owner == mod_ext4_get_block) ||
		  (i_sb->s_magic == XFS_SB_MAGIC &&
		   strcmp(s_type->name, "xfs") == 0 &&
		   s_type->owner == mod_xfs_get_blocks)))
	{
		prError("file_system_type name=%s, not supported", s_type->name);
		return -ENOTSUPP;
	}

	/*
	 * check block size of the filesystem
	 *
	 * MEMO: usually, Ext4/XFS on Linux shall not have block-size larger than
	 * PAGE_CACHE_SIZE (although mkfs.xfs(8) mention about up to 64KB block-
	 * size...).
	 * For safety, we reject block-size larger than PAGE SIZE.
	 */
	if (i_sb->s_blocksize > PAGE_CACHE_SIZE)
	{
		prError("block size of '%s' is %zu; larger than PAGE_CACHE_SIZE",
				bd_disk->disk_name, (size_t)i_sb->s_blocksize);
		return -ENOTSUPP;
	}

	/*
	 * check whether the file size is, at least, more than PAGE_SIZE
	 *
	 * MEMO: It is a rough alternative to prevent inline files on Ext4/XFS.
	 * Contents of these files are stored with inode, instead of separate
	 * data blocks. It usually makes no sense on SSD-to-GPU Direct fature.
	 */
	spin_lock(&f_inode->i_lock);
	if (f_inode->i_size < PAGE_SIZE)
	{
		size_t		i_size = f_inode->i_size;
		spin_unlock(&f_inode->i_lock);
		prError("file size too small (%zu bytes), not suitable", i_size);
		return -ENOTSUPP;
	}
	spin_unlock(&f_inode->i_lock);

	/*
	 * check whether the block device is either of:
	 * 1. physical NVMe-SSD device, or
	 * 2. logical MD RAID-0 device which consists of only NVMe-SSDs
	 */
	if (bd_disk->major == BLOCK_EXT_MAJOR)
		return __extblock_is_supported_nvme(s_bdev,
											p_numa_node_id,
											p_support_dma64);
	else if (bd_disk->major == MD_MAJOR)
		return __mdblock_is_supported_nvme(s_bdev,
										   p_numa_node_id,
										   p_support_dma64,
										   p_raid0_conf);

	prError("block device '%s' on behalf of the file is not supported",
			bd_disk->disk_name);
	return -ENOTSUPP;
}

/*
 * ioctl_check_file
 *
 * ioctl(2) handler for STROM_IOCTL__CHECK_FILE
 */
static int
ioctl_check_file(StromCmd__CheckFile __user *uarg)
{
	StromCmd__CheckFile karg;
	struct file	   *filp;
	int				numa_node_id = -2;
	int				support_dma64 = 1;
	int				rc;

	if (copy_from_user(&karg, uarg, sizeof(karg)))
		return -EFAULT;

	filp = fget(karg.fdesc);
	if (!filp)
		return -EBADF;

	rc = file_is_supported_nvme(filp,
								&numa_node_id,
								&support_dma64,
								NULL);
	fput(filp);

	if (rc == 0)
	{
		karg.numa_node_id	= numa_node_id;
		karg.support_dma64	= support_dma64;
		if (copy_to_user(uarg, &karg, sizeof(karg)))
			rc = -EFAULT;
	}
	return (rc < 0 ? rc : 0);
}

/* ================================================================
 *
 * Main part for SSD-to-GPU P2P DMA
 *
 * ================================================================
 */

/*
 * NOTE: It looks to us Intel 750 SSD does not accept DMA request larger
 * than 128KB. However, we are not certain whether it is restriction for
 * all the NVMe-SSD devices. Right now, 128KB is a default of the max unit
 * length of DMA request.
 */
#define STROM_DMA_SSD2GPU_MAXLEN		(128 * 1024)

struct nvme_ns;

struct strom_dma_task
{
	struct list_head	chain;
	unsigned long		dma_task_id;/* ID of this DMA task */
	int					hindex;		/* index of hash slot */
	atomic_t			refcnt;		/* reference counter */
	bool				frozen;		/* (DEBUG) no longer newly referenced */
	mapped_gpu_memory  *mgmem;		/* destination GPU memory segment */
	strom_dma_buffer   *sd_buf;		/* destination host mapped DMA buffer */
	/* reference to the backing file */
	struct file		   *filp;		/* source file */
	/* MD RAID-0 configuration, if any */
	struct r0conf	   *raid0_conf;
	/* current focus of the raw NVMe-SSD device */
	struct nvme_ns	   *nvme_ns;	/* NVMe namespace (=SCSI LUN) */

	/*
	 * status of asynchronous tasks
	 *
	 * MEMO: Pay attention to error status of the asynchronous tasks.
	 * Asynchronous task may cause errors on random timing, and kernel
	 * space wants to inform this status on the next call. On the other
	 * hands, application may invoke ioctl(2) to reference DMA results,
	 * but may not. So, we have to keep an error status somewhere, but
	 * also needs to be released on appropriate timing; to avoid kernel
	 * memory leak by rude applications.
	 * If any errors, we attach strom_dma_task structure on file handler
	 * used for ioctl(2). The error status shall be reclaimed on the
	 * next time when application wait for a particular DMA task, or
	 * this file handler is closed.
	 */
	long				dma_status;
	struct file		   *ioctl_filp;

	/* state of the current pending SSD2GPU DMA request */
	loff_t				dest_offset;/* current destination offset */
	sector_t			head_sector;
	unsigned int		nr_sectors;
	/* temporary buffer for locked page cache in a chunk */
	struct page		   *file_pages[STROM_DMA_SSD2GPU_MAXLEN / PAGE_CACHE_SIZE];
};
typedef struct strom_dma_task	strom_dma_task;

#define STROM_DMA_TASK_NSLOTS_BITS	9
#define STROM_DMA_TASK_NSLOTS		(1UL << STROM_DMA_TASK_NSLOTS_BITS)
static spinlock_t		strom_dma_task_locks[STROM_DMA_TASK_NSLOTS];
static struct list_head	strom_dma_task_slots[STROM_DMA_TASK_NSLOTS];
static struct list_head	failed_dma_task_slots[STROM_DMA_TASK_NSLOTS];
static wait_queue_head_t strom_dma_task_waitq[STROM_DMA_TASK_NSLOTS];

/*
 * strom_dma_task_index
 */
static inline int
strom_dma_task_index(unsigned long dma_task_id)
{
	return hash_64(dma_task_id, STROM_DMA_TASK_NSLOTS_BITS);
}

/*
 * strom_create_dma_task
 */
static strom_dma_task *
strom_create_dma_task(int fdesc,
					  mapped_gpu_memory *mgmem,
					  struct strom_dma_buffer *sd_buf,
					  struct file *ioctl_filp)
{
	strom_dma_task		   *dtask;
	struct file			   *filp;
	struct super_block	   *i_sb;
	struct block_device	   *s_bdev;
	struct r0conf		   *raid0_conf = NULL;
	int						node_id = -2;
	int						support_dma64 = 1;
	long					retval;
	unsigned long			flags;

	/* either of GPU or Host memory can be destination */
	Assert((mgmem != NULL && sd_buf == NULL) ||
		   (mgmem == NULL && sd_buf != NULL));

	/* ensure the source file is supported */
	filp = fget(fdesc);
	if (!filp)
	{
		prError("file descriptor %d of process %u is not available",
				fdesc, current->tgid);
		return ERR_PTR(-EBADF);
	}
	retval = file_is_supported_nvme(filp,
									&node_id,
									&support_dma64,
									&raid0_conf);
	if (retval < 0)
	{
		fput(filp);
		return ERR_PTR(retval);
	}
	i_sb = filp->f_inode->i_sb;
	s_bdev = i_sb->s_bdev;

	/* allocate strom_dma_task object */
	dtask = kzalloc(sizeof(strom_dma_task), GFP_KERNEL);
	if (!dtask)
	{
		fput(filp);
		return ERR_PTR(-ENOMEM);
	}
	dtask->dma_task_id	= (unsigned long) dtask;
	dtask->hindex		= strom_dma_task_index(dtask->dma_task_id);
    atomic_set(&dtask->refcnt, 1);
	dtask->frozen		= false;
    dtask->mgmem		= mgmem;
	dtask->sd_buf		= sd_buf;
    dtask->filp			= filp;
	dtask->raid0_conf	= raid0_conf;
	dtask->nvme_ns		= NULL;		/* to be set later */
    dtask->dma_status	= 0;
    dtask->ioctl_filp	= get_file(ioctl_filp);
	dtask->dest_offset	= 0;
	dtask->head_sector	= 0;
	dtask->nr_sectors	= 0;

	/*
	 * If no MD RAID-0 configuration here, the focused NVMe-SSD will not be
	 * changed during execution. So, we setup nvme_ns here.
	 */
	if (!raid0_conf)
	{
		struct gendisk	   *bd_disk = s_bdev->bd_disk;

		dtask->nvme_ns	= (struct nvme_ns *)bd_disk->private_data;
	}

	/* OK, this strom_dma_task is now tracked */
	spin_lock_irqsave(&strom_dma_task_locks[dtask->hindex], flags);
	list_add_rcu(&dtask->chain, &strom_dma_task_slots[dtask->hindex]);
	spin_unlock_irqrestore(&strom_dma_task_locks[dtask->hindex], flags);

	return dtask;
}

/*
 * strom_get_dma_task
 */
static strom_dma_task *
strom_get_dma_task(strom_dma_task *dtask)
{
	int		refcnt_new;

	Assert(!dtask->frozen);
	refcnt_new = atomic_inc_return(&dtask->refcnt);
	Assert(refcnt_new > 1);

	return dtask;
}

/*
 * strom_put_dma_task
 */
static void
strom_put_dma_task(strom_dma_task *dtask, long dma_status)
{
	int					hindex = dtask->hindex;
	unsigned long		flags = 0;
	bool				has_spinlock = false;

	if (unlikely(dma_status))
	{
		spin_lock_irqsave(&strom_dma_task_locks[hindex], flags);
		if (!dtask->dma_status)
			dtask->dma_status = dma_status;
		has_spinlock = true;
	}

	if (atomic_dec_and_test(&dtask->refcnt))
	{
		mapped_gpu_memory  *mgmem = dtask->mgmem;
		strom_dma_buffer   *sd_buf = dtask->sd_buf;
		struct file		   *ioctl_filp = dtask->ioctl_filp;
		struct file		   *data_filp = dtask->filp;
		long				dma_status;

		if (!has_spinlock)
			spin_lock_irqsave(&strom_dma_task_locks[hindex], flags);
		/* should be released after the final async job is submitted */
		Assert(dtask->frozen);
		/* fetch status under the lock */
		dma_status = dtask->dma_status;
		/* detach from the global hash table */
		list_del_rcu(&dtask->chain);
		/* move to the error task list, if any error */
		if (unlikely(dma_status))
		{
			dtask->ioctl_filp = NULL;
			dtask->filp = NULL;
			dtask->mgmem = NULL;
			dtask->sd_buf = NULL;
			list_add_tail_rcu(&dtask->chain, &failed_dma_task_slots[hindex]);
		}
		spin_unlock_irqrestore(&strom_dma_task_locks[hindex], flags);
		/* wake up all the waiting tasks, if any */
		wake_up_all(&strom_dma_task_waitq[hindex]);

		/* release the dtask object, if no error */
		if (likely(!dma_status))
			kfree(dtask);
		if (mgmem)
			strom_put_mapped_gpu_memory(mgmem);
		if (sd_buf)
			put_strom_dma_buffer(sd_buf);
		fput(data_filp);
		fput(ioctl_filp);

		prDebug("DMA task (id=%p) was completed", dtask);
	}
	else if (has_spinlock)
		spin_unlock_irqrestore(&strom_dma_task_locks[hindex], flags);
}

/*
 * MD RAID-0 Support
 */
static inline struct strip_zone *
find_zone(struct r0conf *conf, sector_t *p_sector)
{
	struct strip_zone  *zone = conf->strip_zone;
	sector_t			sector = *p_sector;
	int					i;

	for (i = 0; i < conf->nr_strip_zones; i++)
	{
		if (sector < zone[i].zone_end) {
			if (i)
				*p_sector = sector - zone[i-1].zone_end;
			return zone + i;
		}
	}
	return NULL;
}

static struct nvme_ns *
strom_raid0_map_sector(struct r0conf *raid0_conf,
					   struct mddev *mddev,
					   sector_t *p_sector,
					   unsigned int nr_sects)
{
	struct strip_zone  *zone;
	struct md_rdev	   *rdev;
	sector_t			sector = *p_sector;
	sector_t			sector_offset = sector;
	sector_t			chunk;
	unsigned int		sect_in_chunk;
	int					raid_disks = raid0_conf->strip_zone[0].nb_dev;
	unsigned int		chunk_sects = mddev->chunk_sectors;

	/*
	 * Ensure (sector)...(sector + nr_sects) does not go across the chunk
	 * boundary.
	 */
	if (sector / chunk_sects != (sector + nr_sects - 1) / chunk_sects)
	{
		prError("Bug? page-aligned i/o goes across boundary of md raid-0"
				" (sector=%ld, nr_sect=%u, chunk_sects=%ld)",
				(long)sector, nr_sects, (long)chunk_sects);
		return ERR_PTR(-ESPIPE);
	}
 
	zone = find_zone(raid0_conf, &sector_offset);
	if (!zone)
	{
		prError("sector='%lu': out of range in md raid-0 configuration",
				*p_sector);
		return ERR_PTR(-ERANGE);
	}

	if ((chunk_sects & (chunk_sects - 1)) == 0)		/* power of 2? */
	{
		int			chunksect_bits = ffz(~chunk_sects);
		/* find the sector offset inside the chunk */
		sect_in_chunk = sector & (chunk_sects - 1);
		sector >>= chunksect_bits;
		/* chunk in zone; quotient is the chunk in real device*/
		chunk = sector_offset;
		sector_div(chunk, zone->nb_dev << chunksect_bits);
	}
	else
	{
		sect_in_chunk = sector_div(sector, chunk_sects);
		chunk = sector_offset;
		sector_div(chunk, chunk_sects * zone->nb_dev);
	}
	sector_offset = (chunk * chunk_sects) + sect_in_chunk;

	/*
	 *  real sector = chunk in device + starting of zone
	 *   + the position in the chunk
	 */
	rdev = raid0_conf->devlist[(zone - raid0_conf->strip_zone) * raid_disks
							   + sector_div(sector, zone->nb_dev)];

	sector_offset += zone->dev_start + rdev->data_offset;
	if (rdev->bdev->bd_part)
		sector_offset += rdev->bdev->bd_part->start_sect;
	*p_sector = sector_offset;

	return (struct nvme_ns *) rdev->bdev->bd_disk->private_data;
}

/*
 * MEMO: nvme_setup_prps() in the vanilla kernel will lead scalability problem
 * if large concurrent asynchronous DMA is issued. Core of the problem is
 * dma_pool_alloc() to setup PRPs (Physical Region Page) list for NVMe's READ
 * command. It tries to walk on a list of pre-allocated pages under spinlock.
 * Concurrent workload easily grows length of the list up, then duration of
 * the critical section makes longer.
 * In case of NVMe-Strom, length of PRPs list is about (128KB / PAGE_SIZE)
 * entries. So, we can pre-allocate fixed-length PRPs-list buffer, and we can
 * look-up inactive buffer with one step.
 */
struct strom_prps_item
{
	struct list_head	chain;
	dma_addr_t			pitem_dma;	/* physical address of this structure */
	unsigned int		cpu_id;	/* index to strom_prps_locks/slots */
	unsigned int		nrooms;	/* size of prps_list[] array */
	unsigned int		nitems;	/* usage count of prps_list[] array */
	__le64				prps_list[STROM_DMA_SSD2GPU_MAXLEN / PAGE_SIZE + 1];
};
typedef struct strom_prps_item		strom_prps_item;
#define STROM_PRPS_ITEMS_NSLOTS		32
static struct device   *strom_prps_device = NULL;
static spinlock_t		strom_prps_locks[STROM_PRPS_ITEMS_NSLOTS];
static struct list_head	strom_prps_slots[STROM_PRPS_ITEMS_NSLOTS];

static __init int
strom_find_pci_device(struct device *dev, void *data)
{
	struct pci_dev	   *pci_device = to_pci_dev(dev);
    struct pci_driver  *nvme_driver = (struct pci_driver *) data;

	if (nvme_driver)
	{
		if (pci_device->driver == nvme_driver)
			return 1;
		return 0;
	}
	return 1;
}

static __init int
strom_init_prps_item_buffer(void)
{
    struct device_driver   *dev_driver;
	int		i;

	/*
	 * Try to acquire a PCI device which is likely NVMe-SSD.
	 */
	dev_driver = driver_find("nvme", &pci_bus_type);
	if (dev_driver && dev_driver->owner == mod_nvme_alloc_request)
	{
		strom_prps_device = bus_find_device(&pci_bus_type, NULL,
											to_pci_driver(dev_driver),
											strom_find_pci_device);
	}
	if (!strom_prps_device)
		strom_prps_device = bus_find_device(&pci_bus_type, NULL, NULL,
											strom_find_pci_device);
	if (!strom_prps_device)
	{
		prError("No PCI device found, thus unable to get entry to alloc/free coherent memory for DMA");
		return -ENOENT;
	}

	/* init strom_prps_locks/slots */
	for (i=0; i < STROM_PRPS_ITEMS_NSLOTS; i++)
	{
		spin_lock_init(&strom_prps_locks[i]);
		INIT_LIST_HEAD(&strom_prps_slots[i]);
	}
	return 0;
}

static void
strom_exit_prps_item_buffer(void)
{
	int		index;

	for (index=0; index < STROM_PRPS_ITEMS_NSLOTS; index++)
	{
		spinlock_t		   *lock = &strom_prps_locks[index];
		struct list_head   *slot = &strom_prps_slots[index];
		unsigned long		flags;
		strom_prps_item	   *pitem;

		spin_lock_irqsave(lock, flags);
		while (!list_empty(slot))
		{
			pitem = list_first_entry(slot, strom_prps_item, chain);
			list_del(&pitem->chain);
			spin_unlock_irqrestore(lock, flags);

			dma_free_coherent(strom_prps_device,
							  sizeof(strom_prps_item),
							  pitem,
							  pitem->pitem_dma);
			spin_lock_irqsave(lock, flags);
		}
		spin_unlock_irqrestore(lock, flags);
	}
	put_device(strom_prps_device);
}

static strom_prps_item *
strom_prps_item_alloc(void)
{
	int					index = smp_processor_id() % STROM_PRPS_ITEMS_NSLOTS;
	spinlock_t		   *lock = &strom_prps_locks[index];
	struct list_head   *slot = &strom_prps_slots[index];
	unsigned long		flags;
	dma_addr_t			pitem_dma;
	strom_prps_item	   *pitem;

	spin_lock_irqsave(lock, flags);
	if (!list_empty(slot))
	{
		pitem = list_first_entry(slot, strom_prps_item, chain);
		list_del(&pitem->chain);
		memset(&pitem->chain, 0, sizeof(struct list_head));
		spin_unlock_irqrestore(lock, flags);
		return pitem;
	}
	spin_unlock_irqrestore(lock, flags);
	/* no available prps_item, so create a new one */
	pitem = dma_alloc_coherent(strom_prps_device,
							   sizeof(strom_prps_item),
							   &pitem_dma,
							   GFP_KERNEL);
	if (pitem)
	{
		memset(&pitem->chain, 0, sizeof(struct list_head));
		pitem->pitem_dma = pitem_dma;
		pitem->cpu_id = smp_processor_id();
		pitem->nrooms = STROM_DMA_SSD2GPU_MAXLEN / PAGE_SIZE + 1;
		pitem->nitems = 0;
	}
	return pitem;
}

static void
strom_prps_item_free(strom_prps_item *pitem)
{
	int					index = pitem->cpu_id % STROM_PRPS_ITEMS_NSLOTS;
	spinlock_t		   *lock = &strom_prps_locks[index];
	struct list_head   *slot = &strom_prps_slots[index];
	unsigned long		flags;

	Assert(!pitem->chain.next && !pitem->chain.prev);
	spin_lock_irqsave(lock, flags);
	list_add(&pitem->chain, slot);
	spin_unlock_irqrestore(lock, flags);
}

/*
 * DMA transaction for SSD->GPU asynchronous copy
 */
struct strom_async_cmd_context {
	strom_prps_item	   *pitem;
	strom_dma_task	   *dtask;
	struct nvme_command	cmd;	/* NVMe command */
	uint64_t			tv1;	/* TSC value when DMA submit */
};
typedef struct strom_async_cmd_context strom_async_cmd_context;

/*
 * __callback_async_read_cmd - callback of async READ command
 */
static void
__callback_async_read_cmd(struct request *req, int error)
{
	strom_async_cmd_context *async_cxt = req->end_io_data;
	u32		result = (uintptr_t)req->special;
	u16		status = req->errors;
	u64		tv1 = async_cxt->tv1;
	u64		tv2 = rdtsc();

	prDebug("DMA Req Completed error=%d status=%d result=%u",
			error, status, result);
	/* update statistics */
	if (stat_info)
	{
		atomic64_inc(&stat_nr_ssd2gpu);
		atomic64_add((u64)(tv2 > tv1 ? tv2 - tv1 : 0), &stat_clk_ssd2gpu);
		atomic64_dec(&stat_cur_dma_count);
	}
	strom_prps_item_free(async_cxt->pitem);
	strom_put_dma_task(async_cxt->dtask, status);
	kfree(async_cxt);
	blk_mq_free_request(req);
}

/*
 * __submit_async_read_cmd - it submits READ command of NVMe-SSD, and then
 * returns immediately. Callback will put the supplied strom_dma_task,
 * thus, strom_dma_task_wait() allows synchronization of DMA completion.
 */
static int
__submit_async_read_cmd(strom_dma_task *dtask, strom_prps_item *pitem)
{
	struct nvme_ns		   *nvme_ns = dtask->nvme_ns;
	struct nvme_ctrl	   *nvme_ctrl = nvme_ns->ctrl;
	struct request		   *req;
	struct nvme_rw_command *cmd;
	strom_async_cmd_context *async_cmd_cxt;
	size_t					length;
	u16						control = 0;
	u32						dsmgmt = 0;
	u32						nblocks;
	u64						slba;
	dma_addr_t				prp1, prp2;
	int						npages;

	/* setup scatter-gather list */
	length = (dtask->nr_sectors << SECTOR_SHIFT);
	nblocks = (dtask->nr_sectors << (SECTOR_SHIFT - nvme_ns->lba_shift)) - 1;
	if (nblocks > 0xffff)
		return -EINVAL;
	slba = dtask->head_sector << (SECTOR_SHIFT - nvme_ns->lba_shift);

	prp1 = pitem->prps_list[0];
	npages = ((prp1 & (nvme_ctrl->page_size - 1)) +
			  length - 1) / nvme_ctrl->page_size;
	if (npages < 1)
		prp2 = 0;	/* reserved */
	else if (npages < 2)
		prp2 = pitem->prps_list[1];
	else
		prp2 = pitem->pitem_dma + offsetof(strom_prps_item, prps_list[1]);

	/* private datum of async DMA call */
	async_cmd_cxt = kzalloc(sizeof(strom_async_cmd_context), GFP_KERNEL);
	if (!async_cmd_cxt)
		return -ENOMEM;

	/* setup READ command */
	cmd = &async_cmd_cxt->cmd.rw;
	cmd->opcode		= nvme_cmd_read;
	cmd->flags		= 0;	/* we use PRPs, rather than SGL */
	cmd->command_id	= 0;	/* set by nvme driver later */
	cmd->nsid		= cpu_to_le32(nvme_ns->ns_id);
	cmd->prp1		= cpu_to_le64(prp1);
	cmd->prp2		= cpu_to_le64(prp2);
	cmd->metadata	= 0;	/* XXX integrity check, if needed */
	cmd->slba		= cpu_to_le64(slba);
	cmd->length		= cpu_to_le16(nblocks);
	cmd->control	= cpu_to_le16(control);
	cmd->dsmgmt		= cpu_to_le32(dsmgmt);
	/* NOTE: 'reftag', 'apptag' and 'appmask' fields are used only when
	 * nvme-namespace is formatted to use end-to-end protection information.
	 * Linux kernel of RHEL7/CentOS7 does not use these fields.
	 */

	/* allocation of the request */
	req = __nvme_alloc_request(nvme_ns->queue, &async_cmd_cxt->cmd, 0);
	if (IS_ERR(req))
	{
		kfree(async_cmd_cxt);
		return PTR_ERR(req);
	}
	async_cmd_cxt->pitem	= pitem;
	async_cmd_cxt->dtask	= strom_get_dma_task(dtask);
	async_cmd_cxt->tv1		= rdtsc();
	req->end_io_data		= async_cmd_cxt;

	/* throw asynchronous i/o request */
	blk_execute_rq_nowait(nvme_ns->queue, nvme_ns->disk, req, 0,
						  __callback_async_read_cmd);
	return 0;
}


/*
 * strom_memcpy_wait - synchronization of a dma_task
 */
static int
strom_dma_task_wait(unsigned long dma_task_id,
					long *p_dma_task_status,
					int task_state)
{
	int					hindex = strom_dma_task_index(dma_task_id);
	spinlock_t		   *lock = &strom_dma_task_locks[hindex];
	wait_queue_head_t  *waitq = &strom_dma_task_waitq[hindex];
	unsigned long		flags = 0;
	strom_dma_task	   *dtask;
	struct list_head   *slot;
	u64					tv1, tv2;
	int					retval = 0;
	bool				had_sleep = false;
	DEFINE_WAIT(__wait);

	tv1 = rdtsc();
	for (;;)
	{
		bool	has_spinlock = false;
		bool	task_is_running = false;

		rcu_read_lock();
	retry:
		/* check error status first */
		slot = &failed_dma_task_slots[hindex];
		list_for_each_entry_rcu(dtask, slot, chain)
		{
			if (dtask->dma_task_id == dma_task_id)
			{
				if (!has_spinlock)
				{
					rcu_read_unlock();
					has_spinlock = true;
					spin_lock_irqsave(lock, flags);
					goto retry;
				}
				if (p_dma_task_status)
					*p_dma_task_status = dtask->dma_status;
				list_del(&dtask->chain);
				spin_unlock_irqrestore(lock, flags);
				kfree(dtask);
				retval = -EIO;

				goto out;
			}
		}

		/* check whether it is a running task or not */
		slot = &strom_dma_task_slots[hindex];
		list_for_each_entry_rcu(dtask, slot, chain)
		{
			if (dtask->dma_task_id == dma_task_id)
			{
				task_is_running = true;
				break;
			}
		}
		if (has_spinlock)
			spin_unlock_irqrestore(lock, flags);
		else
			rcu_read_unlock();

		if (!task_is_running)
			break;
		if (signal_pending(current))
		{
			retval = -EINTR;
			break;
		}
		/* wait for completion of DMA task */
		prepare_to_wait(waitq, &__wait, task_state);
		schedule();
		if (stat_info && had_sleep)
			atomic64_inc(&stat_nr_wrong_wakeup);
		had_sleep = true;
	}
out:
	finish_wait(waitq, &__wait);
	tv2 = rdtsc();
	if (stat_info && had_sleep)
	{
		atomic64_inc(&stat_nr_wait_dtask);
		atomic64_add((u64)(tv2 > tv1 ? tv2 - tv1 : 0), &stat_clk_wait_dtask);
	}
	return retval;
}

/*
 * ioctl(2) handler for STROM_IOCTL__MEMCPY_WAIT
 */
static int
ioctl_memcpy_wait(StromCmd__MemCpyWait __user *uarg,
				  struct file *ioctl_filp)
{
	StromCmd__MemCpyWait karg;
	long		retval;

	if (copy_from_user(&karg, uarg, sizeof(StromCmd__MemCpyWait)))
		return -EFAULT;

	karg.status = 0;
	retval = strom_dma_task_wait(karg.dma_task_id,
								 &karg.status,
								 TASK_INTERRUPTIBLE);
	if (copy_to_user(uarg, &karg, sizeof(StromCmd__MemCpyWait)))
		return -EFAULT;

	return retval;
}

/*
 * memcpy_pgcache_to_ubuffer - write back page-cache to user buffer
 */
static int
memcpy_pgcache_to_ubuffer(strom_dma_task *dtask,
						  struct file *filp,
						  loff_t fpos,
						  int nr_pages,
						  char __user *dest_uaddr)
{
	struct page	   *fpage;
	char		   *kaddr;
	pgoff_t			fp_index = fpos >> PAGE_CACHE_SHIFT;
	loff_t			left;
	int				i, retval = 0;

	for (i=0; i < nr_pages; i++)
	{
		fpage = dtask->file_pages[i];
		/* Synchronous read, if not cached */
		if (!fpage)
		{
			fpage = read_mapping_page(filp->f_mapping, fp_index + i, NULL);
			if (IS_ERR(fpage))
			{
				retval = PTR_ERR(fpage);
				break;
			}
			lock_page(fpage);
			dtask->file_pages[i] = fpage;
		}
		Assert(fpage != NULL);

		/* write-back the pages to userspace, like file_read_actor() */
		if (unlikely(fault_in_pages_writeable(dest_uaddr, PAGE_CACHE_SIZE)))
			left = 1;	/* go to slow way */
		else
		{
			kaddr = kmap_atomic(fpage);
			left = __copy_to_user_inatomic(dest_uaddr, kaddr,
										   PAGE_CACHE_SIZE);
			kunmap_atomic(kaddr);
		}

		/* Do it by the slow way, if needed */
		if (unlikely(left))
		{
			kaddr = kmap(fpage);
			left = __copy_to_user(dest_uaddr, kaddr, PAGE_CACHE_SIZE);
			kunmap(fpage);

			if (unlikely(left))
			{
				retval = -EFAULT;
				break;
			}
		}
		dest_uaddr += PAGE_CACHE_SIZE;
	}
	return retval;
}

/*
 * Submit READ command to NVMe SSD device
 */
static int
memcpy_from_nvme_ssd(strom_dma_task *dtask,
					 struct inode *f_inode,
					 struct block_device *blkdev,
					 loff_t fpos,
					 int nr_pages,
					 loff_t dest_offset,
					 loff_t dest_segment_sz,
					 int (*submit_async_memcpy)(strom_dma_task *),
					 unsigned int *p_nr_dma_submit,
					 unsigned int *p_nr_dma_blocks)
{
	struct buffer_head	bh;
	struct nvme_ns *nvme_ns;
	sector_t		sector;
	unsigned int	nr_sects;
	unsigned int	max_nr_sects = (STROM_DMA_SSD2GPU_MAXLEN >> SECTOR_SHIFT);
	loff_t			curr_offset = dest_offset;
	int				i, retval = 0;

	for (i=0; i < nr_pages; i++, fpos += PAGE_CACHE_SIZE)
	{
		/* lookup the source block number */
		memset(&bh, 0, sizeof(bh));
		bh.b_size = (1UL << f_inode->i_blkbits);

		retval = strom_get_block(f_inode,
								 fpos >> f_inode->i_blkbits,
								 &bh, 0);
		if (retval)
		{
			prError("strom_get_block: %d", retval);
			break;
		}

		/* adjust location according to sector-size and table partition */
		sector = bh.b_blocknr << (f_inode->i_blkbits - SECTOR_SHIFT);
		if (blkdev->bd_part)
			sector += blkdev->bd_part->start_sect;
		nr_sects = PAGE_CACHE_SIZE >> SECTOR_SHIFT;

		/*
		 * NOTE: If we have MD RAID-0 configuration, block number on the MD
		 * device shall be remapped to the block number on the raw NVMe-SSD
		 * here.
		 * The logic to map block number is equivalent to find_zone() and
		 * map_sector() at drivers/md/raid0.c.
		 */
		if (dtask->raid0_conf)
		{
			struct gendisk *bd_disk = blkdev->bd_disk;
			struct mddev   *mddev = bd_disk->private_data;

			nvme_ns = strom_raid0_map_sector(dtask->raid0_conf,
											 mddev,
											 &sector,
											 nr_sects);
			if (IS_ERR(nvme_ns))
			{
				retval = PTR_ERR(nvme_ns);
				break;
			}
		}
		else
		{
			/* case of raw NVMe-SSD device */
			nvme_ns = NULL;
		}

		/* merge with pending request if possible */
		if ((!nvme_ns || dtask->nvme_ns == nvme_ns) &&
			dtask->nr_sectors > 0 &&
			dtask->nr_sectors + nr_sects <= max_nr_sects &&
			dtask->head_sector + dtask->nr_sectors == sector &&
			dtask->dest_offset +
			SECTOR_SIZE * dtask->nr_sectors == curr_offset &&
			(dest_segment_sz == 0 || (curr_offset % dest_segment_sz) > 0))
		{
			dtask->nr_sectors += nr_sects;
		}
		else
		{
			/* submit pending SSD2GPU DMA */
			if (dtask->nr_sectors > 0)
			{
				(*p_nr_dma_submit)++;
				(*p_nr_dma_blocks) += dtask->nr_sectors;
				retval = submit_async_memcpy(dtask);
				if (retval)
				{
					prError("submit_async_memcpy: %d", retval);
					break;
				}
			}
			if (nvme_ns != NULL)
				dtask->nvme_ns = nvme_ns;
			dtask->dest_offset = curr_offset;
			dtask->head_sector = sector;
			dtask->nr_sectors  = nr_sects;
		}
		curr_offset += PAGE_CACHE_SIZE;
	}
	return retval;
}


/* ================================================================
 *
 * Routines to support SSD-to-GPU (Host mapped GPU device memory)
 *
 * ================================================================
 */
static int
submit_ssd2gpu_memcpy(strom_dma_task *dtask)
{
	mapped_gpu_memory  *mgmem = dtask->mgmem;
	nvidia_p2p_page_table_t *page_table = mgmem->page_table;
	struct nvme_ns	   *nvme_ns = dtask->nvme_ns;
	strom_prps_item	   *pitem;
	ssize_t				total_nbytes;
	dma_addr_t			curr_paddr;
	int					length;
	int					i, retval;
	u32					nvme_page_size = nvme_ns->ctrl->page_size;
	u64					tv1, tv2;

	/* sanity checks */
	Assert(nvme_ns != NULL);
	WARN_ON(nvme_page_size < PAGE_SIZE);

	total_nbytes = SECTOR_SIZE * dtask->nr_sectors;
	if (!total_nbytes || total_nbytes > STROM_DMA_SSD2GPU_MAXLEN)
		return -EINVAL;
	if (dtask->dest_offset < mgmem->map_offset ||
		dtask->dest_offset + total_nbytes > (mgmem->map_offset +
											 mgmem->map_length))
		return -ERANGE;

	tv1 = rdtsc();
	pitem = strom_prps_item_alloc();
	if (!pitem)
		return -ENOMEM;

	i =  (dtask->dest_offset >> mgmem->gpu_page_shift);
	curr_paddr = (page_table->pages[i]->physical_address +
				  (dtask->dest_offset & (mgmem->gpu_page_sz - 1)));
	length = nvme_page_size - (curr_paddr & (nvme_page_size - 1));
	for (i=0; total_nbytes > 0; i++)
	{
		Assert(i < pitem->nrooms);
		pitem->prps_list[i] = curr_paddr;
		curr_paddr += length;
		total_nbytes -= length;

		length = Min(total_nbytes, nvme_page_size);
	}
	pitem->nitems = i;
	if (stat_info)
	{
		tv2 = rdtsc();
		atomic64_inc(&stat_nr_setup_prps);
		atomic64_add((u64)(tv2 > tv1 ? tv2 - tv1 : 0), &stat_clk_setup_prps);
	}

	tv1 = rdtsc();
	retval = __submit_async_read_cmd(dtask, pitem);
	if (retval)
		strom_prps_item_free(pitem);
	if (stat_info)
	{
		long	curval;

		tv2 = rdtsc();
		atomic64_inc(&stat_nr_submit_dma);
		atomic64_add((u64)(tv2 > tv1 ? tv2 - tv1 : 0), &stat_clk_submit_dma);

		curval = atomic64_inc_return(&stat_cur_dma_count);
		atomic64_max_return(curval, &stat_max_dma_count);
	}
	return retval;
}

/*
 * main logic of STROM_IOCTL__MEMCPY_SSD2GPU
 */
static int
do_memcpy_ssd2gpu(StromCmd__MemCpySsdToGpu *karg,
				  strom_dma_task *dtask,
				  uint32_t *chunk_ids_in,
				  uint32_t *chunk_ids_out)
{
	mapped_gpu_memory  *mgmem = dtask->mgmem;
	struct file		   *filp = dtask->filp;
	struct inode	   *f_inode = filp->f_inode;
	struct super_block *i_sb = f_inode->i_sb;
	char __user		   *dest_uaddr;
	size_t				dest_offset;
	unsigned int		nr_pages = (karg->chunk_sz >> PAGE_CACHE_SHIFT);
	int					threshold = nr_pages / 2;
	size_t				i_size;
	long				i, j, k;
	int					retval = 0;

	/* sanity checks */
	if ((karg->chunk_sz & (PAGE_CACHE_SIZE - 1)) != 0 ||	/* alignment */
		karg->chunk_sz < PAGE_CACHE_SIZE ||					/* >= 4KB */
		karg->chunk_sz > STROM_DMA_SSD2GPU_MAXLEN)			/* <= 128KB */
		return -EINVAL;

	dest_offset = mgmem->map_offset + karg->offset;
	if (dest_offset + ((size_t)karg->nr_chunks *
					   (size_t)karg->chunk_sz) > mgmem->map_length)
		return -ERANGE;

	i_size = i_size_read(filp->f_inode);
	for (i=0; i < karg->nr_chunks; i++)
	{
		loff_t			chunk_id = chunk_ids_in[i];
		loff_t			fpos;
		struct page	   *fpage;
		int				score = 0;

		if (karg->relseg_sz == 0)
			fpos = chunk_id * karg->chunk_sz;
		else
			fpos = (chunk_id % karg->relseg_sz) * karg->chunk_sz;
		Assert((fpos & (PAGE_CACHE_SIZE - 1)) == 0);
		if (fpos > i_size)
			return -ERANGE;

		for (j=0, k=fpos >> PAGE_CACHE_SHIFT; j < nr_pages; j++, k++)
		{
			fpage = find_lock_page(filp->f_mapping, k);
			dtask->file_pages[j] = fpage;
			if (fpage)
				score += (PageDirty(fpage) ? threshold + 1 : 1);
		}

		if (score > threshold)
		{
			/*
			 * Write-back of file pages if majority of the chunk is cached,
			 * then application shall call cuMemcpyHtoD for RAM2GPU DMA.
			 */
			karg->nr_ram2gpu++;
			dest_uaddr = karg->wb_buffer +
				karg->chunk_sz * (karg->nr_chunks - karg->nr_ram2gpu);
			retval = memcpy_pgcache_to_ubuffer(dtask,
											   filp,
											   fpos,
											   nr_pages,
											   dest_uaddr);
			chunk_ids_out[karg->nr_chunks -
						  karg->nr_ram2gpu] = (uint32_t)chunk_id;
		}
		else
		{
			retval = memcpy_from_nvme_ssd(dtask,
										  f_inode,
										  i_sb->s_bdev,
										  fpos,
										  nr_pages,
										  dest_offset,
										  0,
										  submit_ssd2gpu_memcpy,
										  &karg->nr_dma_submit,
										  &karg->nr_dma_blocks);
			chunk_ids_out[karg->nr_ssd2gpu] = (uint32_t)chunk_id;
			dest_offset += karg->chunk_sz;
			karg->nr_ssd2gpu++;
		}

		/*
		 * MEMO: score==0 means no pages were cached, so we can skip loop
		 * to unlock/release pages. It's a small optimization.
		 */
		if (score > 0)
		{
			for (j=0; j < nr_pages; j++)
			{
				fpage = dtask->file_pages[j];
				if (fpage)
				{
					unlock_page(fpage);
					page_cache_release(fpage);
				}
			}
		}

		if (retval)
			return retval;
	}
	/* submit pending SSD2GPU DMA request, if any */
	if (dtask->nr_sectors > 0)
	{
		karg->nr_dma_submit++;
		karg->nr_dma_blocks += dtask->nr_sectors;
		retval = submit_ssd2gpu_memcpy(dtask);
	}
	Assert(karg->nr_ram2gpu + karg->nr_ssd2gpu == karg->nr_chunks);

	return retval;
}

/*
 * ioctl(2) handler for STROM_IOCTL__MEMCPY_SSD2GPU
 */
static int
ioctl_memcpy_ssd2gpu(StromCmd__MemCpySsdToGpu __user *uarg,
					 struct file *ioctl_filp)
{
	StromCmd__MemCpySsdToGpu karg;
	mapped_gpu_memory  *mgmem;
	strom_dma_task	   *dtask;
	uint32_t		   *chunk_ids_in = NULL;
	uint32_t		   *chunk_ids_out = NULL;
	int					retval;

	if (copy_from_user(&karg, uarg, sizeof(StromCmd__MemCpySsdToGpu)))
		return -EFAULT;
	chunk_ids_in = kmalloc(2 * sizeof(uint32_t) * karg.nr_chunks, GFP_KERNEL);
	if (!chunk_ids_in)
		return -ENOMEM;
	if (copy_from_user(chunk_ids_in, karg.chunk_ids,
					   sizeof(uint32_t) * karg.nr_chunks))
	{
		retval = -EFAULT;
		goto out;
	}
	chunk_ids_out = chunk_ids_in + karg.nr_chunks;

	/* setup DMA task with mapped GPU memory */
	mgmem = strom_get_mapped_gpu_memory(karg.handle);
	if (!mgmem)
	{
		retval = -ENOENT;
		goto out;
	}

	dtask = strom_create_dma_task(karg.file_desc,
								  mgmem, NULL, ioctl_filp);
	if (IS_ERR(dtask))
	{
		strom_put_mapped_gpu_memory(mgmem);
		retval = PTR_ERR(dtask);
		goto out;
	}
	karg.dma_task_id = dtask->dma_task_id;
	karg.nr_ram2gpu = 0;
	karg.nr_ssd2gpu = 0;
	karg.nr_dma_submit = 0;
	karg.nr_dma_blocks = 0;
	
	retval = do_memcpy_ssd2gpu(&karg, dtask,
							   chunk_ids_in,
							   chunk_ids_out);
	/* no more async jobs shall not acquire the @dtask any more */
	dtask->frozen = true;
	barrier();

	strom_put_dma_task(dtask, 0);

	/* write back the results */
	if (!retval)
	{
		if (copy_to_user(uarg, &karg,
						 offsetof(StromCmd__MemCpySsdToGpu, handle)))
			retval = -EFAULT;
		else if (copy_to_user(karg.chunk_ids, chunk_ids_out,
							  sizeof(uint32_t) * karg.nr_chunks))
			retval = -EFAULT;
	}
	/* synchronization of completion if any error */
	if (retval)
		strom_dma_task_wait(karg.dma_task_id, NULL,
							TASK_UNINTERRUPTIBLE);
out:
	kfree(chunk_ids_in);
	return retval;
}

/* ================================================================
 *
 * Routines to support SSD-to-RAM (Host mapped DMA buffer)
 *
 * ================================================================
 */

/*
 * submit_ssd2ram_memcpy - submit DMA from SSD blocks to host mapped buffer
 */
static int
submit_ssd2ram_memcpy(strom_dma_task *dtask)
{
	strom_dma_buffer   *sd_buf = dtask->sd_buf;
	struct nvme_ns	   *nvme_ns = dtask->nvme_ns;
	struct nvme_ctrl   *nvme_ctrl = nvme_ns->ctrl;
	struct page		   *ppage;
	strom_prps_item	   *pitem;
	ssize_t				total_nbytes;
	long				dest_offset;
	long				i, j, k;
	int					retval;
	u64					tv1, tv2;

	WARN_ON(nvme_ctrl->page_size < PAGE_SIZE);
	WARN_ON((dtask->dest_offset & (PAGE_SIZE - 1)) != 0);

	total_nbytes = SECTOR_SIZE * dtask->nr_sectors;
	if (!total_nbytes || total_nbytes > STROM_DMA_SSD2GPU_MAXLEN)
		return -EINVAL;
	if (dtask->dest_offset < 0 ||
		dtask->dest_offset + total_nbytes > sd_buf->length)
		return -ERANGE;

	tv1 = rdtsc();
	pitem = strom_prps_item_alloc();
	if (!pitem)
		return -ENOMEM;

	/* setup PRPS item */
	dest_offset = dtask->dest_offset;
	for (i=0; total_nbytes > 0; i++)
	{
		size_t	len = Min(total_nbytes, nvme_ctrl->page_size);

		Assert(i < pitem->nrooms);
		j = (dest_offset >> PAGE_SHIFT) / sd_buf->segment_sz;
		k = (dest_offset >> PAGE_SHIFT) % sd_buf->segment_sz;
		ppage = sd_buf->dma_segments[j] + k;
		pitem->prps_list[i] = page_to_phys(ppage);
		dest_offset += len;
		total_nbytes -= len;
	}
	pitem->nitems = i;

	if (stat_info)
	{
		tv2 = rdtsc();
		atomic64_inc(&stat_nr_setup_prps);
		atomic64_add((u64)(tv2 > tv1 ? tv2 - tv1 : 0), &stat_clk_setup_prps);
	}

	tv1 = rdtsc();
	retval = __submit_async_read_cmd(dtask, pitem);
	if (retval)
		strom_prps_item_free(pitem);
	if (stat_info)
	{
		long	curval;

		tv2 = rdtsc();
		atomic64_inc(&stat_nr_submit_dma);
		atomic64_add((u64)(tv2 > tv1 ? tv2 - tv1 : 0), &stat_clk_submit_dma);

		curval = atomic64_inc_return(&stat_cur_dma_count);
		atomic64_max_return(curval, &stat_max_dma_count);
	}
	return retval;
}

/*
 * do_memcpy_ssd2ram - main part of SSD-to-RAM DMA
 */
static int
do_memcpy_ssd2ram(StromCmd__MemCpySsdToRam *karg,
				  strom_dma_task *dtask,
				  size_t dest_offset, uint32_t *chunk_ids)
{
	strom_dma_buffer   *sd_buf = dtask->sd_buf;
	struct file		   *filp = dtask->filp;
	struct inode	   *f_inode = filp->f_inode;
	struct super_block *i_sb = f_inode->i_sb;
	char __user		   *dest_uaddr = karg->dest_uaddr;
	unsigned int		nr_pages = (karg->chunk_sz >> PAGE_CACHE_SHIFT);
	int					threshold = nr_pages / 2;
	size_t				i_size;
	size_t				dest_segment_sz;
	long				i, j, k;
	int					retval = 0;

	/* sanity checks */
	if ((karg->chunk_sz & (PAGE_CACHE_SIZE - 1)) != 0 ||	/* alignment */
		karg->chunk_sz < PAGE_CACHE_SIZE ||					/* >= 4KB */
		karg->chunk_sz > STROM_DMA_SSD2GPU_MAXLEN ||		/* <= 128KB */
		(dest_offset & (PAGE_CACHE_SIZE - 1)) != 0)			/* alignment */
		return -EINVAL;
	if (dest_offset + ((size_t)karg->chunk_sz *
					   (size_t)karg->nr_chunks) > sd_buf->length)
	{
		prError("dest_offset=%lu-%lu buflen=%zu",
				dest_offset,
				dest_offset + ((size_t)karg->chunk_sz *
							   (size_t)karg->nr_chunks),
				sd_buf->length);
		return -ERANGE;
	}

	dest_segment_sz = ((size_t)sd_buf->segment_sz *
					   (size_t)PAGE_SIZE *
					   (size_t)sd_buf->nr_segments);
	i_size = i_size_read(f_inode);
	for (i=0; i < karg->nr_chunks; i++)
	{
		loff_t			chunk_id = chunk_ids[i];
		loff_t			fpos;
		struct page	   *fpage;
		int				score = 0;

		if (karg->relseg_sz == 0)
			fpos = chunk_id * (size_t)karg->chunk_sz;
		else
			fpos = (chunk_id % karg->relseg_sz) * (size_t)karg->chunk_sz;
		Assert((fpos & (PAGE_CACHE_SIZE - 1)) == 0);
		if (fpos > i_size)
		{
			prError("fpos=%ld i_size=%zu", (long)fpos, i_size);
			return -ERANGE;
		}

		for (j=0, k=(fpos >> PAGE_CACHE_SHIFT); j < nr_pages; j++, k++)
		{
			fpage = find_lock_page(filp->f_mapping, k);
			dtask->file_pages[j] = fpage;
			if (fpage)
				score += (PageDirty(fpage) ? threshold + 1 : 1);
		}

		if (score > threshold)
		{
			retval = memcpy_pgcache_to_ubuffer(dtask,
											   filp,
											   fpos,
											   nr_pages,
											   dest_uaddr);
			karg->nr_ram2ram++;
		}
		else
		{
			retval = memcpy_from_nvme_ssd(dtask,
										  f_inode,
										  i_sb->s_bdev,
										  fpos,
										  nr_pages,
										  dest_offset,
										  dest_segment_sz,
										  submit_ssd2ram_memcpy,
										  &karg->nr_dma_submit,
										  &karg->nr_dma_blocks);
			karg->nr_ssd2ram++;
		}

		/*
		 * MEMO: score==0 means no pages were cached, so we can skip loop
		 * to unlock/release pages. It is a small optimization.
		 */
		if (score > 0)
		{
			for (j=0; j < nr_pages; j++)
			{
				fpage = dtask->file_pages[j];
				if (fpage)
				{
					unlock_page(fpage);
					page_cache_release(fpage);
				}
			}
		}

		if (retval)
			return retval;
		dest_uaddr += (size_t)karg->chunk_sz;
		dest_offset += (size_t)karg->chunk_sz;
	}
	/* submit pending SSD2RAM DMA request, if any */
	if (dtask->nr_sectors > 0)
	{
		karg->nr_dma_submit++;
		karg->nr_dma_blocks += dtask->nr_sectors;
		retval = submit_ssd2ram_memcpy(dtask);
	}
	Assert(karg->nr_ram2ram + karg->nr_ssd2ram == karg->nr_chunks);

	return retval;
}

/*
 * ioctl_memcpy_ssd2ram - handler for STROM_IOCTL__MEMCPY_SSD2RAM
 */
static int
ioctl_memcpy_ssd2ram(StromCmd__MemCpySsdToRam __user *uarg,
					 struct file *ioctl_filp)
{
	StromCmd__MemCpySsdToRam karg;
	struct vm_area_struct  *vma = NULL;
	struct mm_struct	   *mm = current->mm;
	strom_dma_buffer	   *sd_buf;
	strom_dma_task		   *dtask;
	uint32_t			   *chunk_ids;
	unsigned long			dest_uaddr;
	size_t					dest_offset;
	int						retval = 0;

	/* copy ioctl arguments from the userspace */
	if (copy_from_user(&karg, uarg, sizeof(karg)))
		return -EFAULT;
	chunk_ids = kmalloc(sizeof(uint32_t) * karg.nr_chunks, GFP_KERNEL);
	if (!chunk_ids)
		return -ENOMEM;
	if (copy_from_user(chunk_ids, karg.chunk_ids,
					   sizeof(uint32_t) * karg.nr_chunks))
	{
		retval = -EFAULT;
		goto out;
	}

	/*
	 * lookup destination buffer; which should be mapped DMA buffer
	 * and range is preliminary mapped to user application.
	 */
	down_read(&mm->mmap_sem);
	dest_uaddr = (unsigned long)uarg->dest_uaddr;
	vma = find_vma(mm, dest_uaddr);
	if (!vma || !vma->vm_file ||
		vma->vm_file->f_op != &strom_dma_buffer_fops)
	{
		up_read(&mm->mmap_sem);
		retval = -EINVAL;
		goto out;
	}

	if (dest_uaddr < vma->vm_start ||
		dest_uaddr + ((size_t)karg.nr_chunks *
					  (size_t)karg.chunk_sz) > vma->vm_end)
	{
		up_read(&mm->mmap_sem);
		retval = -ERANGE;
		prError("uaddr(%p-%p) vm(%p-%p)",
				(void *)(dest_uaddr),
				(void *)(dest_uaddr + (size_t)karg.nr_chunks * (size_t)karg.chunk_sz),
				(void *)vma->vm_start,
				(void *)vma->vm_end);
		goto out;
	}
	dest_offset = vma->vm_pgoff * PAGE_SIZE + (dest_uaddr - vma->vm_start);
	sd_buf = get_strom_dma_buffer(vma->vm_private_data);
	up_read(&mm->mmap_sem);

	/* setup DMA task with mapped host DMA buffer */
	dtask = strom_create_dma_task(karg.file_desc,
								  NULL, sd_buf, ioctl_filp);
	if (IS_ERR(dtask))
	{
		retval = PTR_ERR(dtask);
		goto out;
	}
	karg.dma_task_id = dtask->dma_task_id;
	karg.nr_ram2ram = 0;
	karg.nr_ssd2ram = 0;

	retval = do_memcpy_ssd2ram(&karg, dtask, dest_offset, chunk_ids);
	/* no more async task shall acquire the @dtask any more */
	dtask->frozen = true;
	barrier();

	strom_put_dma_task(dtask, 0);

	/* write back the results */
	if (!retval)
	{
		if (copy_to_user(uarg, &karg,
						 offsetof(StromCmd__MemCpySsdToRam, dest_uaddr)))
			retval = -EFAULT;
	}
	/* synchronization of completion if any error */
	if (retval)
		strom_dma_task_wait(karg.dma_task_id, NULL,
							TASK_UNINTERRUPTIBLE);
out:
	kfree(chunk_ids);
	return retval;
}

/*
 * STROM_IOCTL__STAT_INFO - Run-time statistics support
 */
static int
ioctl_stat_info_command(StromCmd__StatInfo __user *uarg)
{
	StromCmd__StatInfo	karg;

	if (copy_from_user(&karg, uarg, sizeof(StromCmd__StatInfo)))
		return -EFAULT;
	if (karg.version != 1)
		return -EINVAL;
	if (!stat_info)
		return -ENODATA;

	karg.tsc			= rdtsc();
	karg.nr_ssd2gpu		= atomic64_read(&stat_nr_ssd2gpu);
	karg.clk_ssd2gpu	= atomic64_read(&stat_clk_ssd2gpu);
	karg.nr_setup_prps	= atomic64_read(&stat_nr_setup_prps);
	karg.clk_setup_prps	= atomic64_read(&stat_clk_setup_prps);
	karg.nr_submit_dma	= atomic64_read(&stat_nr_submit_dma);
	karg.clk_submit_dma	= atomic64_read(&stat_clk_submit_dma);
	karg.nr_wait_dtask	= atomic64_read(&stat_nr_wait_dtask);
	karg.clk_wait_dtask	= atomic64_read(&stat_clk_wait_dtask);
	karg.nr_wrong_wakeup = atomic64_read(&stat_nr_wrong_wakeup);
	karg.cur_dma_count	= atomic64_read(&stat_cur_dma_count);
	karg.max_dma_count	= atomic64_xchg(&stat_max_dma_count, 0UL);
	if (stat_info == 1)
		karg.has_debug	= 0;
	else
	{
		karg.has_debug	= 1;
		karg.nr_debug1	= atomic64_read(&stat_nr_debug1);
		karg.clk_debug1	= atomic64_read(&stat_clk_debug1);
		karg.nr_debug2	= atomic64_read(&stat_nr_debug2);
		karg.clk_debug2	= atomic64_read(&stat_clk_debug2);
		karg.nr_debug3	= atomic64_read(&stat_nr_debug3);
		karg.clk_debug3	= atomic64_read(&stat_clk_debug3);
		karg.nr_debug4	= atomic64_read(&stat_nr_debug4);
		karg.clk_debug4	= atomic64_read(&stat_clk_debug4);
	}
	if (copy_to_user(uarg, &karg, sizeof(StromCmd__StatInfo)))
		return -EFAULT;

	return 0;
}

/* ================================================================
 *
 * file_operations of '/proc/nvme-strom' entry
 *
 * ================================================================
 */
static const char  *strom_proc_signature =		\
	"version: " NVME_STROM_VERSION "\n"			\
	"target: " UTS_RELEASE "\n"					\
	"build: " NVME_STROM_BUILD_TIMESTAMP "\n";

static int
strom_proc_open(struct inode *inode, struct file *filp)
{
	return 0;
}

static ssize_t
strom_proc_read(struct file *filp, char __user *buf, size_t len, loff_t *pos)
{
	size_t		sig_len = strlen(strom_proc_signature);

	if (*pos >= sig_len)
		return 0;
	if (*pos + len >= sig_len)
		len = sig_len - *pos;
	if (copy_to_user(buf, strom_proc_signature + *pos, len))
		return -EFAULT;
	*pos += len;

	return len;
}

static int
strom_proc_release(struct inode *inode, struct file *filp)
{
	int			i;

	for (i=0; i < STROM_DMA_TASK_NSLOTS; i++)
	{
		spinlock_t		   *lock = &strom_dma_task_locks[i];
		struct list_head   *slot = &failed_dma_task_slots[i];
		unsigned long		flags;
		strom_dma_task	   *dtask;
		strom_dma_task	   *dnext;

		spin_lock_irqsave(lock, flags);
		list_for_each_entry_safe(dtask, dnext, slot, chain)
		{
			if (dtask->ioctl_filp == filp)
			{
				prNotice("Unreferenced asynchronous SSD2GPU DMA error "
						 "(dma_task_id: %lu, status=%ld)",
						 dtask->dma_task_id, dtask->dma_status);
				list_del_rcu(&dtask->chain);
				kfree(dtask);
			}
		}
		spin_unlock_irqrestore(lock, flags);
	}
	return 0;
}

static long
strom_proc_ioctl(struct file *ioctl_filp,
				 unsigned int cmd,
				 unsigned long arg)
{
	long		retval;

	switch (cmd)
	{
		case STROM_IOCTL__CHECK_FILE:
			retval = ioctl_check_file((void __user *) arg);
			break;

		case STROM_IOCTL__MAP_GPU_MEMORY:
			retval = ioctl_map_gpu_memory((void __user *) arg);
			break;

		case STROM_IOCTL__UNMAP_GPU_MEMORY:
			retval = ioctl_unmap_gpu_memory((void __user *) arg);
			break;

		case STROM_IOCTL__LIST_GPU_MEMORY:
			retval = ioctl_list_gpu_memory((void __user *) arg);
			break;

		case STROM_IOCTL__INFO_GPU_MEMORY:
			retval = ioctl_info_gpu_memory((void __user *) arg);
			break;

		case STROM_IOCTL__ALLOC_DMA_BUFFER:
			retval = ioctl_alloc_dma_buffer((void __user *) arg);
			break;

		case STROM_IOCTL__MEMCPY_SSD2GPU:
			retval = ioctl_memcpy_ssd2gpu((void __user *) arg, ioctl_filp);
			break;

		case STROM_IOCTL__MEMCPY_SSD2RAM:
			retval = ioctl_memcpy_ssd2ram((void __user *) arg, ioctl_filp);
			break;

		case STROM_IOCTL__MEMCPY_WAIT:
			retval = ioctl_memcpy_wait((void __user *) arg, ioctl_filp);
			break;

		case STROM_IOCTL__STAT_INFO:
			retval = ioctl_stat_info_command((void __user *) arg);
			break;

		default:
			retval = -EINVAL;
			break;
	}
	return retval;
}

/* device file operations */
static const struct file_operations nvme_strom_fops = {
	.owner			= THIS_MODULE,
	.open			= strom_proc_open,
	.read			= strom_proc_read,
	.release		= strom_proc_release,
	.unlocked_ioctl	= strom_proc_ioctl,
	.compat_ioctl	= strom_proc_ioctl,
};

/* module init handler */
int	__init nvme_strom_init(void)
{
	int			i, rc;

	/* init strom_mgmem_mutex/slots */
	for (i=0; i < MAPPED_GPU_MEMORY_NSLOTS; i++)
	{
		spin_lock_init(&strom_mgmem_locks[i]);
		INIT_LIST_HEAD(&strom_mgmem_slots[i]);
	}

	/* init strom_dma_task_locks/slots */
	for (i=0; i < STROM_DMA_TASK_NSLOTS; i++)
	{
		spin_lock_init(&strom_dma_task_locks[i]);
		INIT_LIST_HEAD(&strom_dma_task_slots[i]);
		INIT_LIST_HEAD(&failed_dma_task_slots[i]);
		init_waitqueue_head(&strom_dma_task_waitq[i]);
	}
	/* solve mandatory symbols */
	rc = strom_init_extra_symbols();
	if (rc)
		goto error_1;
	/* setup own (less concurrent) PRPs infrastructure */
	rc = strom_init_prps_item_buffer();
	if (rc)
		goto error_2;
	/* make "/proc/nvme-strom" entry */
	nvme_strom_proc = proc_create("nvme-strom",
								  0444,
								  NULL,
								  &nvme_strom_fops);
	if (!nvme_strom_proc)
	{
		rc = -ENOMEM;
		goto error_3;
	}
	prNotice("/proc/nvme-strom entry was registered");

	return 0;

error_3:
	strom_exit_prps_item_buffer();
error_2:
	strom_exit_extra_symbols();
error_1:
	return rc;
}
module_init(nvme_strom_init);

void __exit nvme_strom_exit(void)
{
	strom_exit_prps_item_buffer();
	strom_exit_extra_symbols();
	proc_remove(nvme_strom_proc);
	prNotice("/proc/nvme-strom entry was unregistered");
}
module_exit(nvme_strom_exit);

MODULE_AUTHOR("KaiGai Kohei <kaigai@kaigai.gr.jp>");
MODULE_DESCRIPTION("SSD-to-GPU Direct Stream Module");
MODULE_LICENSE("GPL");
MODULE_VERSION("0.6");
