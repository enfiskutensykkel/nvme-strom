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
#include <linux/buffer_head.h>
#include <linux/file.h>
#include <linux/fs.h>
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
#include <generated/utsrelease.h>
#include "nv-p2p.h"
#include "nvme_strom.h"

/* determine the target kernel to build */
#if defined(RHEL_MAJOR) && (RHEL_MAJOR == 7)
#define STROM_TARGET_KERNEL_RHEL7		1
#include "md.rhel7.h"
#include "raid0.rhel7.h"
#else
#error Not a supported Linux kernel
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
#define EXTRA_KSYMS_NEEDS_NVIDIA	1
#include "extra_ksyms.c"

/*
 * extra filesystem signature
 */
#define XFS_SB_MAGIC	0x58465342

/*
 * for boundary alignment requirement
 */
#define GPU_BOUND_SHIFT		16
#define GPU_BOUND_SIZE		((u64)1 << GPU_BOUND_SHIFT)
#define GPU_BOUND_OFFSET	(GPU_BOUND_SIZE-1)
#define GPU_BOUND_MASK		(~GPU_BOUND_OFFSET)

/*
 * Macro definition for sector
 */
#define SECTOR_SHIFT		(9)
#define SECTOR_SIZE			(1UL << SECTOR_SHIFT)

/* procfs entry of "/proc/nvme-strom" */
static struct proc_dir_entry  *nvme_strom_proc = NULL;

/*
 * ================================================================
 *
 * Routines to map/unmap GPU device memory segment
 *
 * ================================================================
 */
struct mapped_gpu_memory
{
	struct list_head	chain;		/* chain to the strom_mgmem_slots[] */
	int					hindex;		/* index of the hash slot */
	int					refcnt;		/* number of the concurrent tasks */
	kuid_t				owner;		/* effective user-id who mapped this
									 * device memory */
	unsigned long		handle;		/* identifier of this entry */
	unsigned long		map_address;/* virtual address of the device memory
									 * (note: just for message output) */
	unsigned long		map_offset;	/* offset from the H/W page boundary */
	unsigned long		map_length;	/* length of the mapped area */
	struct task_struct *wait_task;	/* task waiting for DMA completion */
	size_t				gpu_page_sz;/* page size in bytes; note that
									 * 'page_size' of nvidia_p2p_page_table_t
									 * is one of NVIDIA_P2P_PAGE_SIZE_* */
	size_t				gpu_page_shift;	/* log2 of gpu_page_sz */
	nvidia_p2p_page_table_t *page_table;

	/*
	 * NOTE: User supplied virtual address of device memory may not be
	 * aligned to the hardware page boundary of GPUs. So, we may need to
	 * map the least device memory that wraps the region (vaddress ...
	 * vaddress + length) entirely.
	 * The 'map_offset' is offset of the 'vaddress' from the head of H/W
	 * page boundary. So, if application wants to kick DMA to the location
	 * where handle=1234 and offset=2000 and map_offset=500, the driver
	 * will set up DMA towards the offset=2500 from the head of mapped
	 * physical pages.
	 */

	/*
	 * NOTE: Once a mapped_gpu_memory is registered, it can be released
	 * on random timing, by cuFreeMem(), process termination and etc...
	 * If refcnt > 0, it means someone's P2P DMA is in-progress, so
	 * cleanup routine (that shall be called by nvidia driver) has to
	 * wait for completion of these operations. However, mapped_gpu_memory
	 * shall be released immediately not to use this region any more.
	 */
};
typedef struct mapped_gpu_memory	mapped_gpu_memory;

#define MAPPED_GPU_MEMORY_NSLOTS	48
static spinlock_t		strom_mgmem_locks[MAPPED_GPU_MEMORY_NSLOTS];
static struct list_head	strom_mgmem_slots[MAPPED_GPU_MEMORY_NSLOTS];

/*
 * strom_mapped_gpu_memory_index - index of strom_mgmem_mutex/slots
 */
static inline int
strom_mapped_gpu_memory_index(unsigned long handle)
{
	u32		hash = arch_fast_hash(&handle, sizeof(unsigned long),
								  0x20140702);
	return hash % MAPPED_GPU_MEMORY_NSLOTS;
}

/*
 * strom_get_mapped_gpu_memory
 */
static mapped_gpu_memory *
strom_get_mapped_gpu_memory(unsigned long handle)
{
	int					index = strom_mapped_gpu_memory_index(handle);
	spinlock_t		   *lock = &strom_mgmem_locks[index];
	struct list_head   *slot = &strom_mgmem_slots[index];
	unsigned long		flags;
	mapped_gpu_memory  *mgmem;

	spin_lock_irqsave(lock, flags);
	list_for_each_entry(mgmem, slot, chain)
	{
		if (mgmem->handle == handle &&
			uid_eq(mgmem->owner, current_euid()))
		{
			/* sanity checks */
			Assert((unsigned long)mgmem == handle);
			Assert(mgmem->hindex == index);

			mgmem->refcnt++;
			spin_unlock_irqrestore(lock, flags);

			return mgmem;
		}
	}
	spin_unlock_irqrestore(lock, flags);

	prError("P2P GPU Memory (handle=%lx) not found", handle);

	return NULL;	/* not found */
}

/*
 * strom_put_mapped_gpu_memory
 */
static void
strom_put_mapped_gpu_memory(mapped_gpu_memory *mgmem)
{
	int				index = mgmem->hindex;
	spinlock_t	   *lock = &strom_mgmem_locks[index];
	unsigned long	flags;

	spin_lock_irqsave(lock, flags);
	Assert(mgmem->refcnt > 0);
	if (--mgmem->refcnt == 0)
	{
		if (mgmem->wait_task)
			wake_up_process(mgmem->wait_task);
		mgmem->wait_task = NULL;
	}
	spin_unlock_irqrestore(lock, flags);
}

/*
 * callback_release_mapped_gpu_memory
 */
static void
callback_release_mapped_gpu_memory(void *private)
{
	mapped_gpu_memory  *mgmem = private;
	spinlock_t		   *lock = &strom_mgmem_locks[mgmem->hindex];
	unsigned long		handle = mgmem->handle;
	unsigned long		flags;
	int					rc;

	/* sanity check */
	Assert((unsigned long)mgmem == handle);

	spin_lock_irqsave(lock, flags);
	/*
	 * Detach this mapped GPU memory from the global list first, if
	 * application didn't unmap explicitly.
	 */
	if (mgmem->chain.next || mgmem->chain.prev)
	{
		list_del(&mgmem->chain);
		memset(&mgmem->chain, 0, sizeof(struct list_head));
	}

	/*
	 * wait for completion of the concurrent DMA tasks, if any tasks
	 * are running.
	 */
	if (mgmem->refcnt > 0)
	{
		struct task_struct *wait_task_saved = mgmem->wait_task;

		mgmem->wait_task = current;
		/* sleep until refcnt == 0 */
		set_current_state(TASK_UNINTERRUPTIBLE);
		spin_unlock_irqrestore(lock, flags);

		schedule();

		if (wait_task_saved)
			wake_up_process(wait_task_saved);

		spin_lock_irqsave(lock, flags);
		Assert(mgmem->refcnt == 0);
	}
	spin_unlock_irqrestore(lock, flags);

	/*
	 * OK, no concurrent task does not use this mapped GPU memory region
	 * at this point. So, we can release the page table and relevant safely.
	 */
	rc = __nvidia_p2p_free_page_table(mgmem->page_table);
	if (rc)
		prError("nvidia_p2p_free_page_table (handle=0x%lx, rc=%d)",
				handle, rc);
	kfree(mgmem);

	prNotice("P2P GPU Memory (handle=%p) was released", (void *)handle);

	module_put(THIS_MODULE);
}

/*
 * ioctl_map_gpu_memory
 *
 * ioctl(2) handler for STROM_IOCTL__MAP_GPU_MEMORY
 */
static int
ioctl_map_gpu_memory(StromCmd__MapGpuMemory __user *uarg)
{
	StromCmd__MapGpuMemory karg;
	mapped_gpu_memory  *mgmem;
	unsigned long		map_address;
	unsigned long		map_offset;
	unsigned long		handle;
	unsigned long		flags;
	uint64_t			curr_paddr;
	uint64_t			next_paddr;
	uint32_t			entries;
	int					i, rc;

	if (copy_from_user(&karg, uarg, sizeof(karg)))
		return -EFAULT;

	mgmem = kmalloc(sizeof(mapped_gpu_memory), GFP_KERNEL);
	if (!mgmem)
		return -ENOMEM;

	map_address = karg.vaddress & GPU_BOUND_MASK;
	map_offset  = karg.vaddress & GPU_BOUND_OFFSET;
	handle = (unsigned long) mgmem;

	INIT_LIST_HEAD(&mgmem->chain);
	mgmem->hindex		= strom_mapped_gpu_memory_index(handle);
	mgmem->refcnt		= 0;
	mgmem->owner		= current_euid();
	mgmem->handle		= handle;
	mgmem->map_address  = map_address;
	mgmem->map_offset	= map_offset;
	mgmem->map_length	= map_offset + karg.length;
	mgmem->wait_task	= NULL;

	rc = __nvidia_p2p_get_pages(0,	/* p2p_token; deprecated */
								0,	/* va_space_token; deprecated */
								mgmem->map_address,
								mgmem->map_length,
								&mgmem->page_table,
								callback_release_mapped_gpu_memory,
								mgmem);
	if (rc)
	{
		prError("failed on nvidia_p2p_get_pages(addr=%p, len=%zu), rc=%d",
				(void *)map_address, (size_t)map_offset + karg.length, rc);
		goto error_1;
	}

	/* page size in bytes */
	switch (mgmem->page_table->page_size)
	{
		case NVIDIA_P2P_PAGE_SIZE_4KB:
			mgmem->gpu_page_sz = 4 * 1024;
			mgmem->gpu_page_shift = 12;
			break;
		case NVIDIA_P2P_PAGE_SIZE_64KB:
			mgmem->gpu_page_sz = 64 * 1024;
			mgmem->gpu_page_shift = 16;
			break;
		case NVIDIA_P2P_PAGE_SIZE_128KB:
			mgmem->gpu_page_sz = 128 * 1024;
			mgmem->gpu_page_shift = 17;
			break;
		default:
			rc = -EINVAL;
			goto error_2;
	}

	/* ensure mapped physical addresses are continuous */
	curr_paddr = mgmem->page_table->pages[0]->physical_address;
	for (i=1; i < mgmem->page_table->entries; i++)
	{
		next_paddr = mgmem->page_table->pages[i]->physical_address;
		if (curr_paddr + mgmem->gpu_page_sz != next_paddr)
		{
			prError("Mapped P2P GPU Memory was no continuous");
			rc = -EINVAL;
			goto error_2;
		}
		curr_paddr = next_paddr;
	}

	/* return the handle of mapped_gpu_memory */
	entries = mgmem->page_table->entries;
	if (put_user(mgmem->handle, &uarg->handle) ||
		put_user(mgmem->gpu_page_sz, &uarg->gpu_page_sz) ||
		put_user(entries, &uarg->gpu_npages))
	{
		rc = -EFAULT;
		goto error_2;
	}

	prNotice("P2P GPU Memory (handle=%p) mapped "
			 "(version=%u, page_size=%zu, entries=%u)",
			 (void *)mgmem->handle,
			 mgmem->page_table->version,
			 mgmem->gpu_page_sz,
			 mgmem->page_table->entries);

	/*
	 * Warning message if mapped device memory is not aligned well
	 */
	if ((mgmem->map_offset & (PAGE_SIZE - 1)) != 0 ||
		(mgmem->map_length & (PAGE_SIZE - 1)) != 0)
	{
		prWarn("Gpu memory mapping (handle=%lx) is not aligned well "
			   "(map_offset=%lx map_length=%lx). "
			   "It may be inconvenient to submit DMA requests",
			   mgmem->handle,
			   mgmem->map_offset,
			   mgmem->map_length);
	}
	__module_get(THIS_MODULE);

	/* attach this mapped_gpu_memory */
	spin_lock_irqsave(&strom_mgmem_locks[mgmem->hindex], flags);
	list_add(&mgmem->chain, &strom_mgmem_slots[mgmem->hindex]);
	spin_unlock_irqrestore(&strom_mgmem_locks[mgmem->hindex], flags);

	return 0;

error_2:
	__nvidia_p2p_put_pages(0, 0, mgmem->map_address, mgmem->page_table);
error_1:
	kfree(mgmem);

	return rc;
}

/*
 * ioctl_unmap_gpu_memory
 *
 * ioctl(2) handler for STROM_IOCTL__UNMAP_GPU_MEMORY
 */
static int
ioctl_unmap_gpu_memory(StromCmd__UnmapGpuMemory __user *uarg)
{
	StromCmd__UnmapGpuMemory karg;
	mapped_gpu_memory  *mgmem;
	spinlock_t		   *lock;
	struct list_head   *slot;
	unsigned long		flags;
	int					i, rc;

	if (copy_from_user(&karg, uarg, sizeof(karg)))
		return -EFAULT;

	i = strom_mapped_gpu_memory_index(karg.handle);
	lock = &strom_mgmem_locks[i];
	slot = &strom_mgmem_slots[i];

	spin_lock_irqsave(lock, flags);
	list_for_each_entry(mgmem, slot, chain)
	{
		/*
		 * NOTE: I'm not 100% certain whether UID is the right check to
		 * determine availability of the virtual address of GPU device.
		 * So, this behavior may be changed in the later version.
		 */
		if (mgmem->handle == karg.handle &&
			uid_eq(mgmem->owner, current_euid()))
		{
			list_del(&mgmem->chain);
			memset(&mgmem->chain, 0, sizeof(struct list_head));
			spin_unlock_irqrestore(lock, flags);

			rc = __nvidia_p2p_put_pages(0, 0,
										mgmem->map_address,
										mgmem->page_table);
			if (rc)
				prError("failed on nvidia_p2p_put_pages: %d", rc);
			return rc;
		}
	}
	spin_unlock_irqrestore(lock, flags);

	prError("no mapped GPU memory found (handle: %lx)", karg.handle);
	return -ENOENT;
}

/*
 * ioctl_list_gpu_memory
 *
 * ioctl(2) handler for STROM_IOCTL__LIST_GPU_MEMORY
 */
static int
ioctl_list_gpu_memory(StromCmd__ListGpuMemory __user *uarg)
{
	StromCmd__ListGpuMemory karg;
	spinlock_t		   *lock;
	struct list_head   *slot;
	unsigned long		flags;
	mapped_gpu_memory  *mgmem;
	int					i, j;
	int					retval = 0;

	if (copy_from_user(&karg, uarg,
					   offsetof(StromCmd__ListGpuMemory, handles)))
		return -EFAULT;

	karg.nitems = 0;
	for (i=0; i < MAPPED_GPU_MEMORY_NSLOTS; i++)
	{
		lock = &strom_mgmem_locks[i];
		slot = &strom_mgmem_slots[i];

		spin_lock_irqsave(lock, flags);
		list_for_each_entry(mgmem, slot, chain)
		{
			j = karg.nitems++;
			if (j < karg.nrooms)
			{
				if (put_user(mgmem->handle, &uarg->handles[j]))
					retval = -EFAULT;
			}
			else
				retval = -ENOBUFS;
		}
		spin_unlock_irqrestore(lock, flags);
	}
	/* write back */
	if (copy_to_user(uarg, &karg,
					 offsetof(StromCmd__ListGpuMemory, handles)))
		retval = -EFAULT;

	return retval;
}

/*
 * ioctl_info_gpu_memory
 *
 * ioctl(2) handler for STROM_IOCTL__INFO_GPU_MEMORY
 */
static int
ioctl_info_gpu_memory(StromCmd__InfoGpuMemory __user *uarg)
{
	StromCmd__InfoGpuMemory karg;
	mapped_gpu_memory *mgmem;
	nvidia_p2p_page_table_t *page_table;
	size_t		length;
	int			i, rc = 0;

	length = offsetof(StromCmd__InfoGpuMemory, paddrs);
	if (copy_from_user(&karg, uarg, length))
		return -EFAULT;

	mgmem = strom_get_mapped_gpu_memory(karg.handle);
	if (!mgmem)
		return -ENOENT;

	page_table       = mgmem->page_table;
	karg.nitems      = page_table->entries;
	karg.version     = page_table->version;
	karg.gpu_page_sz = mgmem->gpu_page_sz;
	karg.owner       = __kuid_val(mgmem->owner);
	karg.map_offset  = mgmem->map_offset;
	karg.map_length  = mgmem->map_length;
	if (copy_to_user((void __user *)uarg, &karg, length))
		rc = -EFAULT;
	else
	{
		for (i=0; i < page_table->entries; i++)
		{
			if (i >= karg.nrooms)
			{
				rc = -ENOBUFS;
				break;
			}
			if (put_user(page_table->pages[i]->physical_address,
						 &uarg->paddrs[i]))
			{
				rc = -EFAULT;
				break;
			}
		}
	}
	strom_put_mapped_gpu_memory(mgmem);

	return rc;
}

/*
 * strom_get_block - a generic version of get_block_t for the supported
 * filesystems. It assumes the target filesystem is already checked by
 * file_is_supported_nvme, so we have minimum checks here.
 */
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
__extblock_is_supported_nvme(struct block_device *blkdev)
{
	struct gendisk *bd_disk = blkdev->bd_disk;
	const char	   *dname;
	int				rc;

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
	return 0;	/* OK, we can assume this volume is raw NVMe-SSD */
}

/*
 * __mdblock_is_supported_nvme - checker for BLOCK_EXT_MAJOR
 */
static int
__mdblock_is_supported_nvme(struct block_device *blkdev,
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

	if (!mddev || !mddev->pers || !mddev->ready)
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
		rc = __extblock_is_supported_nvme(rdev->bdev);
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
file_is_supported_nvme(struct file *filp, bool is_writable,
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
	if ((filp->f_mode & (is_writable ? FMODE_WRITE : FMODE_READ)) == 0)
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
	if (!is_writable)
	{
		spin_lock(&f_inode->i_lock);
		if (f_inode->i_size < PAGE_SIZE)
		{
			size_t		i_size = f_inode->i_size;
			spin_unlock(&f_inode->i_lock);
			prError("file size too small (%zu bytes), not suitable", i_size);
			return -ENOTSUPP;
		}
		spin_unlock(&f_inode->i_lock);
	}

	/*
	 * check whether the block device is either of:
	 * 1. physical NVMe-SSD device, or
	 * 2. logical MD RAID-0 device which consists of only NVMe-SSDs
	 */
	if (bd_disk->major == BLOCK_EXT_MAJOR)
		return __extblock_is_supported_nvme(s_bdev);
	else if (bd_disk->major == MD_MAJOR)
		return __mdblock_is_supported_nvme(s_bdev, p_raid0_conf);

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
	int				rc;

	if (copy_from_user(&karg, uarg, sizeof(karg)))
		return -EFAULT;

	filp = fget(karg.fdesc);
	if (!filp)
		return -EBADF;

	rc = file_is_supported_nvme(filp, false, NULL);

	fput(filp);

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

struct strom_dma_task
{
	struct list_head	chain;
	unsigned long		dma_task_id;/* ID of this DMA task */
	int					hindex;		/* index of hash slot */
	atomic_t			refcnt;		/* reference counter */
	bool				frozen;		/* (DEBUG) no longer newly referenced */
	mapped_gpu_memory  *mgmem;		/* destination GPU memory segment */
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

#define STROM_DMA_TASK_NSLOTS		240
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
	u32		hash = arch_fast_hash(&dma_task_id, sizeof(unsigned long),
								  0x20120106);
	return hash % STROM_DMA_TASK_NSLOTS;
}

/*
 * strom_create_dma_task
 */
static strom_dma_task *
strom_create_dma_task(unsigned long handle,
					  int fdesc,
					  struct file *ioctl_filp)
{
	mapped_gpu_memory	   *mgmem;
	strom_dma_task		   *dtask;
	struct file			   *filp;
	struct super_block	   *i_sb;
	struct block_device	   *s_bdev;
	struct r0conf		   *raid0_conf = NULL;
	long					retval;
	unsigned long			flags;

	/* ensure the source file is supported */
	filp = fget(fdesc);
	if (!filp)
	{
		prError("file descriptor %d of process %u is not available",
				fdesc, current->tgid);
		retval = -EBADF;
		goto error_0;
	}
	retval = file_is_supported_nvme(filp, false, &raid0_conf);
	if (retval < 0)
		goto error_1;
	i_sb = filp->f_inode->i_sb;
	s_bdev = i_sb->s_bdev;

	/* get destination GPU memory */
	mgmem = strom_get_mapped_gpu_memory(handle);
	if (!mgmem)
	{
		retval = -ENOENT;
		goto error_1;
	}

	/* allocate strom_dma_task object */
	dtask = kzalloc(sizeof(strom_dma_task), GFP_KERNEL);
	if (!dtask)
	{
		retval = -ENOMEM;
		goto error_2;
	}
	dtask->dma_task_id	= (unsigned long) dtask;
	dtask->hindex		= strom_dma_task_index(dtask->dma_task_id);
    atomic_set(&dtask->refcnt, 1);
	dtask->frozen		= false;
    dtask->mgmem		= mgmem;
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

error_2:
	strom_put_mapped_gpu_memory(mgmem);
error_1:
	fput(filp);
error_0:
	return ERR_PTR(retval);
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
		mapped_gpu_memory *mgmem = dtask->mgmem;
		struct file	   *ioctl_filp = dtask->ioctl_filp;
		struct file	   *data_filp = dtask->filp;
		long			dma_status;

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

			list_add_tail_rcu(&dtask->chain, &failed_dma_task_slots[hindex]);
		}
		spin_unlock_irqrestore(&strom_dma_task_locks[hindex], flags);
		/* wake up all the waiting tasks, if any */
		wake_up_all(&strom_dma_task_waitq[hindex]);

		/* release the dtask object, if no error */
		if (likely(!dma_status))
			kfree(dtask);
		strom_put_mapped_gpu_memory(mgmem);
		fput(data_filp);
		fput(ioctl_filp);

		prDebug("DMA task (id=%p) was completed", dtask);
	}
	else if (has_spinlock)
		spin_unlock_irqrestore(&strom_dma_task_locks[hindex], flags);
}

/*
 * MD RAID-0 Support
 *
 *
 *
 *
 *
 *
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
 * In case of NVMe-Strom, length of PRPs list is about (128KB / PAGE_SIZE
 * + alpha for GPU page boundary) entries. So, we can pre-allocate fixed-
 * length PRPs-list buffer, and we can look-up inactive buffer with one step.
 */
struct strom_prps_item
{
	struct list_head	chain;
	struct nvme_dev	   *nvme_dev;
	dma_addr_t			pitem_dma;	/* physical address of this structure */
	unsigned int		nrooms;	/* size of prps_list[] array */
	unsigned int		nitems;	/* usage count of prps_list[] array */
	__le64				prps_list[0];
};
typedef struct strom_prps_item		strom_prps_item;
#define STROM_PRPS_ITEMS_NSLOTS		32
static spinlock_t		strom_prps_locks[STROM_PRPS_ITEMS_NSLOTS];
static struct list_head	strom_prps_slots[STROM_PRPS_ITEMS_NSLOTS];

static inline int
strom_prps_item_hash(struct nvme_dev *nvme_dev)
{
	unsigned long	val = ((unsigned long)nvme_dev) >> 3;

	return (((val      ) & 0xffff) |
			((val >> 16) & 0xffff) |
			((val >> 32) & 0xffff) |
			((val >> 48) & 0xffff)) % STROM_PRPS_ITEMS_NSLOTS;
}

static void
strom_prps_items_release(struct nvme_dev *nvme_dev)
{
	int		index, max;

	if (nvme_dev)
		index = max = strom_prps_item_hash(nvme_dev);
	else
	{
		index = 0;
		max = STROM_PRPS_ITEMS_NSLOTS - 1;
	}

	while (index <= max)
	{
		spinlock_t		   *lock = &strom_prps_locks[index];
		struct list_head   *slot = &strom_prps_slots[index];
		unsigned long		flags;
		size_t				length;
		strom_prps_item	   *pitem;

		spin_lock_irqsave(lock, flags);
		list_for_each_entry(pitem, slot, chain)
		{
			if (!nvme_dev || pitem->nvme_dev == nvme_dev)
			{
				length = offsetof(strom_prps_item,
								  prps_list[pitem->nrooms]);
				list_del(&pitem->chain);
				dma_free_coherent(&nvme_dev->pci_dev->dev,
								  length,
								  pitem,
								  pitem->pitem_dma);
			}
		}
		spin_unlock_irqrestore(lock, flags);
	}
}

static strom_prps_item *
strom_prps_item_alloc(mapped_gpu_memory *mgmem, struct nvme_dev *nvme_dev)
{
	int					index = strom_prps_item_hash(nvme_dev);
	spinlock_t		   *lock = &strom_prps_locks[index];
	struct list_head   *slot = &strom_prps_slots[index];
	unsigned long		flags;
	dma_addr_t			pitem_dma;
	unsigned int		nrooms;
	size_t				length;
	strom_prps_item	   *pitem;

	spin_lock_irqsave(lock, flags);
	list_for_each_entry(pitem, slot, chain)
	{
		if (pitem->nvme_dev == nvme_dev)
		{
			list_del(&pitem->chain);
			memset(&pitem->chain, 0, sizeof(struct list_head));
			spin_unlock_irqrestore(lock, flags);
			return pitem;
		}
	}
	spin_unlock_irqrestore(lock, flags);
	/* no inactive buffer, allocate a new one */
	nrooms = ((STROM_DMA_SSD2GPU_MAXLEN +
			   nvme_dev->page_size - 1) / nvme_dev->page_size +
			  (STROM_DMA_SSD2GPU_MAXLEN +
			   mgmem->gpu_page_sz - 1) / mgmem->gpu_page_sz);
	length = offsetof(strom_prps_item, prps_list[nrooms]);
	pitem = dma_alloc_coherent(&nvme_dev->pci_dev->dev,
							   length,
							   &pitem_dma,
							   GFP_KERNEL);
	if (!pitem)
		return NULL;

	pitem->nvme_dev = nvme_dev;
	pitem->pitem_dma = pitem_dma;
	pitem->nrooms = nrooms;
	pitem->nitems = 0;

	return pitem;
}

static void
strom_prps_item_free(strom_prps_item *pitem)
{
	int					index = strom_prps_item_hash(pitem->nvme_dev);
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
#ifdef STROM_TARGET_KERNEL_RHEL7
#include "nvme_strom.rhel7.c"
#else
#error "no platform specific NVMe-SSD routines"
#endif










static int
submit_ssd2gpu_memcpy(strom_dma_task *dtask)
{
	mapped_gpu_memory  *mgmem = dtask->mgmem;
	nvidia_p2p_page_table_t *page_table = mgmem->page_table;
	struct nvme_ns	   *nvme_ns = dtask->nvme_ns;
	struct nvme_dev	   *nvme_dev = nvme_ns->dev;
	strom_prps_item	   *pitem;
	ssize_t				total_nbytes;
	dma_addr_t			curr_paddr;
	int					length;
	int					i, retval;

	/* sanity checks */
	Assert(nvme_ns != NULL);

	total_nbytes = SECTOR_SIZE * dtask->nr_sectors;
	if (!total_nbytes || total_nbytes > STROM_DMA_SSD2GPU_MAXLEN)
		return -EINVAL;
	if (dtask->dest_offset < mgmem->map_offset ||
		dtask->dest_offset + total_nbytes > (mgmem->map_offset +
											 mgmem->map_length))
		return -ERANGE;

	pitem = strom_prps_item_alloc(mgmem, nvme_dev);
	if (!pitem)
		return -ENOMEM;

	i =  (dtask->dest_offset >> mgmem->gpu_page_shift);
	curr_paddr = (page_table->pages[i]->physical_address +
				  (dtask->dest_offset & (mgmem->gpu_page_sz - 1)));
	length = nvme_dev->page_size - (curr_paddr & (nvme_dev->page_size - 1));
	for (i=0; total_nbytes > 0; i++)
	{
		pitem->prps_list[i] = curr_paddr;
		curr_paddr += length;
		total_nbytes -= length;

		length = Min(total_nbytes, nvme_dev->page_size);
	}
	pitem->nitems = i;

	retval = nvme_submit_async_read_cmd(dtask, pitem);
	if (retval)
		strom_prps_item_free(pitem);

	return retval;
}

/*
 * strom_memcpy_ssd2gpu_wait - synchronization of a dma_task
 */
static int
strom_memcpy_ssd2gpu_wait(unsigned long dma_task_id,
						  long *p_dma_task_status,
						  int task_state)
{
	int					hindex = strom_dma_task_index(dma_task_id);
	spinlock_t		   *lock = &strom_dma_task_locks[hindex];
	wait_queue_head_t  *waitq = &strom_dma_task_waitq[hindex];
	unsigned long		flags = 0;
	strom_dma_task	   *dtask;
	struct list_head   *slot;
	int					retval = 0;

	DEFINE_WAIT(__wait);
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
	}
out:
	finish_wait(waitq, &__wait);

	return retval;
}

/*
 * ioctl(2) handler for STROM_IOCTL__MEMCPY_SSD2GPU_WAIT
 */
static int
ioctl_memcpy_ssd2gpu_wait(StromCmd__MemCpySsdToGpuWait __user *uarg,
						  struct file *ioctl_filp)
{
	StromCmd__MemCpySsdToGpuWait karg;
	long		retval;

	if (copy_from_user(&karg, uarg, sizeof(StromCmd__MemCpySsdToGpuWait)))
		return -EFAULT;

	karg.status = 0;
	retval = strom_memcpy_ssd2gpu_wait(karg.dma_task_id,
									   &karg.status,
									   TASK_INTERRUPTIBLE);
	if (copy_to_user(uarg, &karg, sizeof(StromCmd__MemCpySsdToGpuWait)))
		return -EFAULT;

	return retval;
}

/*
 * write back a chunk to user buffer
 */
static int
__memcpy_ssd2gpu_writeback(strom_dma_task *dtask,
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
 * Submit a P2P DMA request
 */
static int
__memcpy_ssd2gpu_submit_dma(strom_dma_task *dtask,
							struct file *filp,
							struct block_device *blkdev,
							loff_t fpos,
							int nr_pages,
							loff_t dest_offset,
							unsigned int *p_nr_dma_submit,
							unsigned int *p_nr_dma_blocks)
{
	struct inode   *f_inode = filp->f_inode;
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

		retval = strom_get_block(filp->f_inode,
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
			SECTOR_SIZE * dtask->nr_sectors == curr_offset)
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
				retval = submit_ssd2gpu_memcpy(dtask);
				if (retval)
				{
					prError("submit_ssd2gpu_memcpy: %d", retval);
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

/*
 * main logic of STROM_IOCTL__MEMCPY_SSD2GPU_WRITEBACK
 */
static int
memcpy_ssd2gpu_writeback(StromCmd__MemCpySsdToGpuWriteBack *karg,
						 strom_dma_task *dtask,
						 uint32_t *chunk_ids_in,
						 uint32_t *chunk_ids_out)
{
	mapped_gpu_memory  *mgmem = dtask->mgmem;
	struct file		   *filp = dtask->filp;
	struct inode	   *f_inode = filp->f_inode;
	struct super_block *i_sb = f_inode->i_sb;
	struct block_device	*blkdev = i_sb->s_bdev;
	char __user		   *dest_uaddr;
	size_t				dest_offset;
	unsigned int		nr_pages = (karg->chunk_sz >> PAGE_CACHE_SHIFT);
	int					threshold = nr_pages / 2;
	size_t				i_size;
	int					retval = 0;
	int					i, j, k;

	/* sanity checks */
	if ((karg->chunk_sz & (PAGE_CACHE_SIZE - 1)) != 0 ||	/* alignment */
		karg->chunk_sz < PAGE_CACHE_SIZE ||					/* >= 4KB */
		karg->chunk_sz > STROM_DMA_SSD2GPU_MAXLEN)			/* <= 128KB */
		return -EINVAL;

	dest_offset = mgmem->map_offset + karg->offset;
	if (dest_offset + karg->nr_chunks * karg->chunk_sz > mgmem->map_length)
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
			retval = __memcpy_ssd2gpu_writeback(dtask,
												filp,
												fpos,
												nr_pages,
												dest_uaddr);
			chunk_ids_out[karg->nr_chunks -
						  karg->nr_ram2gpu] = (uint32_t)chunk_id;
		}
		else
		{
			retval = __memcpy_ssd2gpu_submit_dma(dtask,
												 filp,
												 blkdev,
												 fpos,
												 nr_pages,
												 dest_offset,
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
		submit_ssd2gpu_memcpy(dtask);
	}
	Assert(karg->nr_ram2gpu + karg->nr_ssd2gpu == karg->nr_chunks);

	return 0;
}

/*
 * ioctl(2) handler for STROM_IOCTL__MEMCPY_SSD2GPU_WRITEBACK
 */
static int
ioctl_memcpy_ssd2gpu_writeback(StromCmd__MemCpySsdToGpuWriteBack __user *uarg,
							   struct file *ioctl_filp)
{
	StromCmd__MemCpySsdToGpuWriteBack karg;
	strom_dma_task *dtask;
	uint32_t	   *chunk_ids_in = NULL;
	uint32_t	   *chunk_ids_out = NULL;
	int				retval;

	if (copy_from_user(&karg, uarg, sizeof(StromCmd__MemCpySsdToGpuWriteBack)))
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

	/* setup DMA task */
	dtask = strom_create_dma_task(karg.handle,
								  karg.file_desc,
								  ioctl_filp);
	if (IS_ERR(dtask))
	{
		retval = PTR_ERR(dtask);
		goto out;
	}
	karg.dma_task_id = dtask->dma_task_id;
	karg.nr_ram2gpu = 0;
	karg.nr_ssd2gpu = 0;
	karg.nr_dma_submit = 0;
	karg.nr_dma_blocks = 0;
	
	retval = memcpy_ssd2gpu_writeback(&karg, dtask,
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
						 offsetof(StromCmd__MemCpySsdToGpuWriteBack, handle)))
			retval = -EFAULT;
		else if (copy_to_user(karg.chunk_ids, chunk_ids_out,
							  sizeof(uint32_t) * karg.nr_chunks))
			retval = -EFAULT;
	}
	/* synchronization of completion if any error */
	if (retval)
		strom_memcpy_ssd2gpu_wait(karg.dma_task_id, NULL,
								  TASK_UNINTERRUPTIBLE);
out:
	kfree(chunk_ids_in);
	return retval;
}

/*
 * STROM_IOCTL__DEBUG_COMMAND
 */

struct dma_pool {       /* the pool */
    struct list_head page_list;
    spinlock_t lock;
    size_t size;
    struct device *dev;
    size_t allocation;
    size_t boundary;
    char name[32];
    struct list_head pools;
};

struct dma_page {       /* cacheable header for 'allocation' bytes */
    struct list_head page_list;
    void *vaddr;
    dma_addr_t dma;
    unsigned int in_use;
    unsigned int offset;
};

static int
ioctl_debug_command(StromCmd__DebugCommand __user *uarg)
{
	StromCmd__DebugCommand	karg;
	struct file			   *filp;
	struct gendisk		   *bd_disk;
	struct nvme_ns		   *nvme_ns;
	struct nvme_dev		   *nvme_dev;
	struct dma_pool		   *pool;
	struct dma_page		   *page;
	struct r0conf		   *raid0conf = NULL;
	int						page_count = 0;
	int						small_count = 0;
	unsigned long			flags;
	int						retval;

	if (copy_from_user(&karg, uarg, sizeof(StromCmd__DebugCommand)))
		return -EFAULT;

	filp = fget(karg.file_desc);
	if (!filp)
	{
		prError("file descriptor %d of process %u is not available",
				karg.file_desc, current->tgid);
		return -EBADF;
	}
	retval = file_is_supported_nvme(filp, false, &raid0conf);
	if (retval < 0 || raid0conf != NULL)
	{
		prError("file descriptor %d of process %u is not supported",
				karg.file_desc, current->tgid);
		goto out;
	}
	bd_disk = filp->f_inode->i_sb->s_bdev->bd_disk;
	nvme_ns = (struct nvme_ns *) bd_disk->private_data;
	nvme_dev = nvme_ns->dev;

	pool = nvme_dev->prp_page_pool;
	spin_lock_irqsave(&pool->lock, flags);
	list_for_each_entry(page, &pool->page_list, page_list)
		page_count++;
	spin_unlock_irqrestore(&pool->lock, flags);

	pool = nvme_dev->prp_small_pool;
	spin_lock_irqsave(&pool->lock, flags);
	list_for_each_entry(page, &pool->page_list, page_list)
		small_count++;
	spin_unlock_irqrestore(&pool->lock, flags);

	karg.values[0] = page_count;
	karg.values[1] = small_count;

	if (copy_to_user(uarg, &karg, sizeof(StromCmd__DebugCommand)))
		retval = -EFAULT;
out:
	fput(filp);
	return retval;
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

		case STROM_IOCTL__MEMCPY_SSD2GPU_WRITEBACK:
			retval = ioctl_memcpy_ssd2gpu_writeback((void __user *) arg,
													ioctl_filp);
			break;

		case STROM_IOCTL__MEMCPY_SSD2GPU_WAIT:
			retval = ioctl_memcpy_ssd2gpu_wait((void __user *) arg,
											   ioctl_filp);
			break;

		case STROM_IOCTL__DEBUG_COMMAND:
			retval = ioctl_debug_command((void __user *) arg);
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

/*
 * Injection of PCI driver hook
 */
static void (*nvme_saved_on_pci_remove)(struct pci_dev *pdev) = NULL;

static void
nvme_strom_on_pci_remove(struct pci_dev *pdev)
{
	struct nvme_dev	   *nvme_dev = pci_get_drvdata(pdev);

	/* cleanup resources relevant to nvme_dev */
	strom_prps_items_release(nvme_dev);

	if (nvme_saved_on_pci_remove)
		nvme_saved_on_pci_remove(pdev);
}

static int
nvme_strom_init_pci_hook(void)
{
	struct pci_driver  *nvme_driver;
	struct device_driver *dev_driver;

	dev_driver = driver_find("nvme", &pci_bus_type);
	if (!dev_driver)
	{
		prError("PCI driver for NVMe SSD is not installed");
		return -ENOENT;
	}
	if (dev_driver->owner != mod_nvme_submit_io_cmd)
	{
		prError("NVMe SSD driver has strange module ownership");
		return -EINVAL;
	}
	/* inject nvme_strom_on_pci_remove as PCI remove hook */
	nvme_driver = container_of(dev_driver, struct pci_driver, driver);
	nvme_saved_on_pci_remove = nvme_driver->remove;
	mb();
	nvme_driver->remove = nvme_strom_on_pci_remove;

	prNotice("PCI remove hook nvme_strom_on_pci_remove() injected");

	return 0;
}

static void
nvme_strom_exit_pci_hook(void)
{
	struct pci_driver  *nvme_driver;
	struct device_driver *dev_driver;

	dev_driver = driver_find("nvme", &pci_bus_type);
	BUG_ON(!dev_driver || dev_driver->owner != mod_nvme_submit_io_cmd);
	nvme_driver = container_of(dev_driver, struct pci_driver, driver);
	BUG_ON(nvme_driver->remove == nvme_strom_on_pci_remove);
	nvme_driver->remove = nvme_saved_on_pci_remove;
	mb();
}

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

	/* init strom_prps_locks/slots */
	for (i=0; i < STROM_PRPS_ITEMS_NSLOTS; i++)
	{
		spin_lock_init(&strom_prps_locks[i]);
		INIT_LIST_HEAD(&strom_prps_slots[i]);
	}

	/* solve mandatory symbols */
	rc = strom_init_extra_symbols();
	if (rc)
		goto error_1;
	/* inject PCI remove hook */
	rc = nvme_strom_init_pci_hook();
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
	nvme_strom_exit_pci_hook();
error_2:
	strom_exit_extra_symbols();
error_1:
	return rc;
}
module_init(nvme_strom_init);

void __exit nvme_strom_exit(void)
{
	strom_prps_items_release(NULL);
	nvme_strom_exit_pci_hook();
	strom_exit_extra_symbols();
	proc_remove(nvme_strom_proc);
	prNotice("/proc/nvme-strom entry was unregistered");
}
module_exit(nvme_strom_exit);

MODULE_AUTHOR("KaiGai Kohei <kaigai@kaigai.gr.jp>");
MODULE_DESCRIPTION("SSD-to-GPU Direct Stream Module");
MODULE_LICENSE("GPL");
MODULE_VERSION("0.5");
