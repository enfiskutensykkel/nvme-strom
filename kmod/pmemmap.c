/*
 * pmemmap.c
 *
 * Routines to support physical memory mapping for NVMe-Strom
 *
 * Copyright (C) 2017 KaiGai Kohei <kaigai@kaigai.gr.jp>
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

/*
 * ================================================================
 *
 * Routines to map/unmap GPU device memory segment
 *
 * ================================================================
 */

/* boundary alignment parameters for GPU device memory */
#define GPU_BOUND_SHIFT		16
#define GPU_BOUND_SIZE		((u64)1 << GPU_BOUND_SHIFT)
#define GPU_BOUND_OFFSET	(GPU_BOUND_SIZE-1)
#define GPU_BOUND_MASK		(~GPU_BOUND_OFFSET)

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

#define MAPPED_GPU_MEMORY_NSLOTS_BITS	6
#define MAPPED_GPU_MEMORY_NSLOTS		(1UL << MAPPED_GPU_MEMORY_NSLOTS_BITS)
static spinlock_t		strom_mgmem_locks[MAPPED_GPU_MEMORY_NSLOTS];
static struct list_head	strom_mgmem_slots[MAPPED_GPU_MEMORY_NSLOTS];

/*
 * strom_mapped_gpu_memory_index - index of strom_mgmem_mutex/slots
 */
static inline int
strom_mapped_gpu_memory_index(unsigned long handle)
{
	return hash_long(handle, MAPPED_GPU_MEMORY_NSLOTS_BITS);
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

/* ================================================================
 *
 * Routines to manage NUMA aware DMA buffer
 *
 * NOTE: STROM_IOCTL__ALLOCATE_DMA_BUFFER makes an anonymous file handler
 * which allows application to mmap(2) preliminaty acquired NUMA aware DMA
 * buffer on user space. Then, application can kick asynchronous DMA call
 * onto the buffer.
 * ================================================================ */
struct strom_dma_buffer
{
	size_t			length;		/* size required on allocation */
	atomic_t		refcnt;		/* reference count */
	int				node_id;	/* NUMA node id */
	unsigned int	segment_sz;	/* # of pages per DMA segment */
	unsigned int	nr_segments;/* # of DMA segments */
	struct page	   *dma_segments[1];
};
typedef struct strom_dma_buffer		strom_dma_buffer;

/*
 * get_strom_dma_buffer
 */
static strom_dma_buffer *
get_strom_dma_buffer(strom_dma_buffer *sd_buf)
{
	int		refcnt		__attribute__((unused));

	refcnt = atomic_inc_return(&sd_buf->refcnt);
	Assert(refcnt > 1);
	return sd_buf;
}

/*
 * put_strom_dma_buffer
 */
static void
put_strom_dma_buffer(strom_dma_buffer *sd_buf)
{
	if (atomic_dec_and_test(&sd_buf->refcnt))
	{
		int		i, j;

		for (i=0; i < sd_buf->nr_segments; i++)
		{
			for (j=0; j < sd_buf->segment_sz; j++)
				__free_page(sd_buf->dma_segments[i] + j);
		}
		kfree(sd_buf);
	}
}

/*
 * strom_dma_buffer_fault - fault handler of DMA buffer
 */
static int
strom_dma_buffer_fault(struct vm_area_struct *vma, struct vm_fault *vmf)
{
	strom_dma_buffer *sd_buf = vma->vm_private_data;
	struct page	   *dma_page;
	unsigned int	index;
	unsigned int	offset;

	if (!sd_buf)
		return VM_FAULT_NOPAGE;
	index  = vmf->pgoff / sd_buf->segment_sz;
	offset = vmf->pgoff % sd_buf->segment_sz;
	if (index >= sd_buf->nr_segments)
		return VM_FAULT_SIGBUS;

	dma_page = sd_buf->dma_segments[index] + offset;
	get_page(dma_page);
	vmf->page = dma_page;

	return 0;
}

static const struct vm_operations_struct strom_dma_buffer_vm_ops = {
	.fault		= strom_dma_buffer_fault,
};

static int
strom_dma_buffer_mmap(struct file *filp, struct vm_area_struct *vma)
{
	strom_dma_buffer   *sd_buf = filp->private_data;
	unsigned long		vm_len = vma->vm_end - vma->vm_start;

	/* sanity checks */
	WARN_ON(vma->vm_file != filp);
	/* available only if MAP_SHARED */
	if ((vma->vm_flags & (VM_SHARED|VM_MAYSHARE)) == 0)
	{
		prError("Only MAP_SHARED is available on mmap(2) to Strom DMA Buffer");
		return -EINVAL;
	}
	/* range must be contained in the physical DMA buffer pages */
	if ((vma->vm_pgoff << PAGE_SHIFT) + vm_len > sd_buf->length)
	{
		prError("vma (%p-%p on (%zu...%zu) is out of range (size=%zu)",
				(void *)vma->vm_start,
				(void *)vma->vm_end - 1,
				(size_t)(vma->vm_pgoff << PAGE_SHIFT),
				(size_t)(vm_len),
				(size_t)(sd_buf->length));
		return -EINVAL;
	}
	vma->vm_flags |= VM_IO;
	vma->vm_ops = &strom_dma_buffer_vm_ops;
	vma->vm_private_data = sd_buf;
	return 0;
}

static int
strom_dma_buffer_release(struct inode *inode, struct file *filp)
{
	put_strom_dma_buffer((strom_dma_buffer *) filp->private_data);

	return 0;
}

static const struct file_operations strom_dma_buffer_fops = {
	.mmap		= strom_dma_buffer_mmap,
	.release	= strom_dma_buffer_release,
};

/*
 * STROM_IOCTL__ALLOCATE_DMA_BUFFER
 */
static int
ioctl_alloc_dma_buffer(StromCmd__AllocateDMABuffer __user *uarg)
{
	StromCmd__AllocateDMABuffer karg;
	strom_dma_buffer *sd_buf;
	unsigned int	order = (MAX_ORDER - 1);
	unsigned int	segment_sz = (1U << order);
	unsigned int	i, j, nr_segments;
	char			namebuf[80];
	int				fdesc = -ENOMEM;

	if (copy_from_user(&karg, uarg, sizeof(StromCmd__AllocateDMABuffer)))
		return -EFAULT;

	/* sanity checks */
	if (karg.length < PAGE_SIZE || (karg.length & (PAGE_SIZE - 1)) != 0)
	{
		prError("DMA buffer length is 0 or unaligned (len=%zu)",
				(size_t)karg.length);
		return -EINVAL;
	}
	if (karg.node_id >= 0 && karg.node_id >= nr_node_ids)
	{
		prError("Numa node ID is out of range (node_id=%d)", karg.node_id);
		return -EINVAL;
	}

	/* allocate DMA buffer from normal zone */
	nr_segments = (karg.length + (segment_sz << PAGE_SHIFT) - 1)
							   / (segment_sz << PAGE_SHIFT);
	sd_buf = kzalloc(offsetof(strom_dma_buffer,
							  dma_segments[nr_segments]), GFP_KERNEL);
	if (!sd_buf)
		return -ENOMEM;

	sd_buf->length  = karg.length;
	atomic_set(&sd_buf->refcnt, 1);
	sd_buf->node_id = karg.node_id;
	sd_buf->segment_sz = segment_sz;
	sd_buf->nr_segments = nr_segments;
	for (i=0; i < nr_segments; i++)
	{
		struct page *dma_segment = alloc_pages_node(sd_buf->node_id,
													GFP_KERNEL,
													order);
		if (!dma_segment)
			goto error;
		split_page(dma_segment, order);
		sd_buf->dma_segments[i] = dma_segment;
	}

	/* construction of an anonymous file */
	if (karg.length < (8UL << 20))
		snprintf(namebuf, sizeof(namebuf), "dmabuf%u:%zuKB",
				 sd_buf->node_id, (size_t)(karg.length >> 10));
	else if (karg.length < (8UL << 30))
		snprintf(namebuf, sizeof(namebuf), "dmabuf%u:%zuMB",
				 sd_buf->node_id, (size_t)(karg.length >> 20));
	else
		snprintf(namebuf, sizeof(namebuf), "dmabuf%u:%zuGB",
				 sd_buf->node_id, (size_t)(karg.length >> 30));
	fdesc = anon_inode_getfd(namebuf,
							 &strom_dma_buffer_fops,
							 sd_buf, O_RDWR);
	if (fdesc < 0)
		goto error;
	karg.dmabuf_fdesc = fdesc;

	/* back to the userspace */
	if (copy_to_user(uarg, &karg, sizeof(StromCmd__AllocateDMABuffer)))
	{
		struct file *filp = fcheck(fdesc);

		put_unused_fd(fdesc);
		if (filp)
			fput(filp);
		fdesc = -EFAULT;
		goto error;
	}
	return 0;

error:
	for (i=0; i < sd_buf->nr_segments; i++)
	{
		for (j=0; j < sd_buf->segment_sz; j++)
			__free_page(sd_buf->dma_segments[i] + j);
	}
	kfree(sd_buf);
	return fdesc;
}
