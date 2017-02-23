/*
 * RHEL7 specific portion for the NVMe-Strom driver
 *
 * RHEL7 kernel does not have non-static function to enqueue NVME-SSD command
 * with asynchronous manner.
 * nvme_submit_sync_cmd() is a thin wrapper for nvme_submit_cmd(), but we
 * cannot avoid synchronize the caller's context. So, we partially copy the
 * code and structure from the kernel source. Once RHEL7 kernel updated its
 * implementation, we have to follow these update....
 */

struct strom_async_cmd_context {
	strom_prps_item	   *pitem;
	strom_dma_task	   *dtask;
	struct nvme_command	cmd;	/* NVMe command */
	uint64_t			tv1;	/* TSC value when DMA submit */
};
typedef struct strom_async_cmd_context strom_async_cmd_context;

/*
 * nvme_callback_async_read_cmd - callback of async READ command
 */
static void
nvme_callback_async_read_cmd(struct request *req, int error)
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
 * nvme_submit_io_cmd_async - It submits an I/O command of NVME-SSD, and then
 * returns to the caller immediately. Callback will put the strom_dma_task,
 * thus, strom_memcpy_ssd2gpu_wait() allows synchronization of DMA completion.
 */
static int
nvme_submit_async_read_cmd(strom_dma_task *dtask, strom_prps_item *pitem)
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
						  nvme_callback_async_read_cmd);
	return 0;
}
