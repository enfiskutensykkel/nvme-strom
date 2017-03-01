/*
 * ssd2ram.c
 *
 * Routines to support SSD-to-RAM direct load with NVMe-Strom
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
 * memcpy_ssd2ram_async - submit DMA requests
 */
static int
memcpy_ssd2ram_async(StromCmd__MemCpySsdToRamAsync *karg,
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
		return -ERANGE;

	dest_segment_sz = ((size_t)sd_buf->segment_sz *
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
			return -ERANGE;

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
 * ioctl_memcpy_ssd2ram_async - handler for STROM_IOCTL__MEMCPY_SSD2RAM_ASYNC
 */
static int
ioctl_memcpy_ssd2ram_async(StromCmd__MemCpySsdToRamAsync __user *uarg,
						   struct file *ioctl_filp)
{
	StromCmd__MemCpySsdToRamAsync karg;
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

	retval = memcpy_ssd2ram_async(&karg, dtask, dest_offset, chunk_ids);
	/* no more async task shall acquire the @dtask any more */
	dtask->frozen = true;
	barrier();

	strom_put_dma_task(dtask, 0);

	/* write back the results */
	if (!retval)
	{
		if (copy_to_user(uarg, &karg,
						 offsetof(StromCmd__MemCpySsdToRamAsync, dest_uaddr)))
			retval = -EFAULT;
	}
	/* synchronization of completion if any error */
	if (retval)
		strom_memcpy_ssd2gpu_wait(karg.dma_task_id, NULL,
								  TASK_UNINTERRUPTIBLE);
out:
	kfree(chunk_ids);
	return retval;
}
