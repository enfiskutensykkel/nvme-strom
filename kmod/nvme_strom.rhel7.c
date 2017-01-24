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
#include <linux/kthread.h>


struct strom_ssd2gpu_request {
	struct request	   *req;
	strom_prps_item	   *pitem;
	strom_dma_task	   *dtask;
	uint64_t			tv1;	/* TSC value when DMA submit */
};
typedef struct strom_ssd2gpu_request	strom_ssd2gpu_request;

struct async_cmd_info {
	struct kthread_work work;
	struct kthread_worker *worker;
	struct request *req;
	u32 result;
	int status;
	void *ctx;
};

/*
 * An NVM Express queue.  Each device has at least two (one for admin
 * commands and one for I/O commands).
 */
struct nvme_queue {
	struct device *q_dmadev;
	struct nvme_dev *dev;
	char irqname[24];   /* nvme4294967295-65535\0 */
	spinlock_t q_lock;
	struct nvme_command *sq_cmds;
	volatile struct nvme_completion *cqes;
	struct blk_mq_tags **tags;
	dma_addr_t sq_dma_addr;
	dma_addr_t cq_dma_addr;
	u32 __iomem *q_db;
	u16 q_depth;
	s16 cq_vector;
	u16 sq_head;
	u16 sq_tail;
	u16 cq_head;
	u16 qid;
	u8 cq_phase;
	u8 cqe_seen;
	struct async_cmd_info cmdinfo;
};

typedef void (*nvme_completion_fn)(struct nvme_queue *, void *,
								   struct nvme_completion *);

struct nvme_cmd_info {
	nvme_completion_fn fn;
	void *ctx;
	int aborted;
	struct nvme_queue *nvmeq;
	struct nvme_iod iod[0];
};

static void
nvme_callback_async_read_cmd(struct nvme_queue *nvmeq, void *ctx,
							 struct nvme_completion *cqe)
{
	strom_ssd2gpu_request *ssd2gpu_req = (strom_ssd2gpu_request *) ctx;
	int		dma_status = le16_to_cpup(&cqe->status) >> 1;
	u32		dma_result = le32_to_cpup(&cqe->result);
	u64		tv1 = ssd2gpu_req->tv1;
	u64		tv2 = rdtsc();

	/*
	 * FIXME: dma_status is one of NVME_SC_* (like NVME_SC_SUCCESS)
	 * We have to translate it to host understandable error code
	 */
	prDebug("DMA Req Completed status=%d result=%u", dma_status, dma_result);

	/* update statistics */
	if (stat_info)
	{
		atomic64_inc(&stat_nr_ssd2gpu);
		atomic64_add((u64)(tv2 > tv1 ? tv2 - tv1 : 0), &stat_clk_ssd2gpu);
		atomic64_dec(&stat_cur_dma_count);
	}

	/* release resources and wake up waiter */
	strom_prps_item_free(ssd2gpu_req->pitem);
	blk_mq_free_request(ssd2gpu_req->req);
	strom_put_dma_task(ssd2gpu_req->dtask, dma_status);
	kfree(ssd2gpu_req);
}

/* -- copy from nvme-core.c -- */

/*
 * nvme_submit_cmd() - Copy a command into a queue and ring the doorbell
 * @nvmeq: The queue to use
 * @cmd: The command to send
 *
 * Safe to use from interrupt context
 */
static inline int
__nvme_submit_cmd(struct nvme_queue *nvmeq, struct nvme_command *cmd)
{
	u16 tail = nvmeq->sq_tail;

	memcpy(&nvmeq->sq_cmds[tail], cmd, sizeof(*cmd));
	if (++tail == nvmeq->q_depth)
		tail = 0;
	writel(tail, nvmeq->q_db);
	nvmeq->sq_tail = tail;

	return 0;
}

static inline int
nvme_submit_cmd(struct nvme_queue *nvmeq, struct nvme_command *cmd)
{
	unsigned long flags;
	int ret;

	spin_lock_irqsave(&nvmeq->q_lock, flags);
	ret = __nvme_submit_cmd(nvmeq, cmd);
	spin_unlock_irqrestore(&nvmeq->q_lock, flags);
	return ret;
}

static void
nvme_set_info(struct nvme_cmd_info *cmd, void *ctx, nvme_completion_fn handler)
{
	cmd->fn = handler;
	cmd->ctx = ctx;
	cmd->aborted = 0;
	blk_mq_start_request(blk_mq_rq_from_pdu(cmd));
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
	struct nvme_dev		   *nvme_dev = nvme_ns->dev;
	struct request		   *req;
	struct nvme_cmd_info   *cmd_rq;
	struct nvme_command		cmd;
	strom_ssd2gpu_request  *ssd2gpu_req;
	size_t					length;
	u16						control = 0;
	u32						dsmgmt = 0;
	u32						nblocks;
	u64						slba;
	dma_addr_t				prp1, prp2;
	int						npages;
	int						retval = 0;

	/* setup scatter-gather list */
	length = (dtask->nr_sectors << SECTOR_SHIFT);
	nblocks = (dtask->nr_sectors << (SECTOR_SHIFT - nvme_ns->lba_shift)) - 1;
	if (nblocks > 0xffff)
		return -EINVAL;
	slba = dtask->head_sector << (SECTOR_SHIFT - nvme_ns->lba_shift);

	prp1 = pitem->prps_list[0];
	npages = ((prp1 & (nvme_dev->page_size - 1)) +
			  length - 1) / nvme_dev->page_size;
	if (npages < 1)
		prp2 = 0;	/* reserved */
	else if (npages < 2)
		prp2 = pitem->prps_list[1];
	else
		prp2 = pitem->pitem_dma + offsetof(strom_prps_item, prps_list[1]);

	/* private datum of async DMA call */
	ssd2gpu_req = kzalloc(sizeof(strom_ssd2gpu_request), GFP_KERNEL);
	if (!ssd2gpu_req)
		return -ENOMEM;

	/* submit an asynchronous command */
	req = blk_mq_alloc_request(nvme_ns->queue,
							   WRITE,
							   GFP_KERNEL|__GFP_WAIT,
							   false);
	if (IS_ERR(req))
	{
		kfree(ssd2gpu_req);
		return PTR_ERR(req);
	}
	ssd2gpu_req->req = req;
	ssd2gpu_req->pitem = pitem;
	ssd2gpu_req->dtask = strom_get_dma_task(dtask);
	ssd2gpu_req->tv1 = rdtsc();

	/* setup READ command */
	if (req->cmd_flags & REQ_FUA)
		control |= NVME_RW_FUA;
	if (req->cmd_flags & (REQ_FAILFAST_DEV | REQ_RAHEAD))
		control |= NVME_RW_LR;
	if (req->cmd_flags & REQ_RAHEAD)
		dsmgmt |= NVME_RW_DSM_FREQ_PREFETCH;

	memset(&cmd, 0, sizeof(struct nvme_command));
	cmd.rw.opcode		= nvme_cmd_read;
	cmd.rw.flags		= 0;	/* we use PRPs, rather than SGL */
	cmd.rw.command_id	= req->tag;
	cmd.rw.nsid			= cpu_to_le32(nvme_ns->ns_id);
	cmd.rw.prp1			= cpu_to_le64(prp1);
	cmd.rw.prp2			= cpu_to_le64(prp2);
	cmd.rw.metadata		= 0;	/* XXX integrity check, if needed */
	cmd.rw.slba			= cpu_to_le64(slba);
	cmd.rw.length		= cpu_to_le16(nblocks);
	cmd.rw.control		= cpu_to_le16(control);
	cmd.rw.dsmgmt		= cpu_to_le32(dsmgmt);
	/*
	 * 'reftag', 'apptag' and 'appmask' fields are used only when nvme-
	 * namespace is formatted to use end-to-end protection information.
	 * Linux kernel of RHEL7 does not use these fields.
	 */
	cmd_rq = blk_mq_rq_to_pdu(req);
	nvme_set_info(cmd_rq, ssd2gpu_req, nvme_callback_async_read_cmd);
	nvme_submit_cmd(cmd_rq->nvmeq, &cmd);

	return retval;
}
