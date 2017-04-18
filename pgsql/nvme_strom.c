/*
 * nvme_strom.c
 *
 * Fast relation scan for large tables using SSD-to-RAM direct transfer.
 *
 * Copyright 2017 (C) KaiGai Kohei <kaigai@kaigai.gr.jp>
 * Copyright 2017 (C) HeteroDB, Inc
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */
#include "postgres.h"
#include "access/visibilitymap.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/subselect.h"
#include "storage/buf_internals.h"
#include "storage/predicate.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/pg_crc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/spccache.h"
#include "utils/syscache.h"
#include "nvme_strom.h"
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <unistd.h>

PG_MODULE_MAGIC;
void	_PG_init(void);

static set_rel_pathlist_hook_type set_rel_pathlist_next;
static CustomPathMethods	nvmestrom_path_methods;
static CustomScanMethods	nvmestrom_plan_methods;
static CustomExecMethods	nvmestrom_exec_methods;
static int					nvmestrom_enabled;			/* GUC */
static int					nvmestrom_chunk_size_kb;	/* GUC */
static int					nvmestrom_buffer_size_kb;	/* GUC */
static double				nvmestrom_seq_page_cost;	/* GUC */
static long					sysconf_pagesize;	/* _SC_PAGESIZE */
static long					sysconf_phys_pages;	/* _SC_PHYS_PAGES */
static long					nvmestrom_nblocks_threshold;
static HTAB				   *vfs_nvme_htable = NULL;
static Oid					nvme_last_tablespace_oid = InvalidOid;
static bool					nvme_last_tablespace_supported;
static int					nvme_last_numa_node_id = -1;

/* options for nvme_strom.enabled */
static struct config_enum_entry nvmestrom_enabled_options[] = {
	{ "on",      1, false },
	{ "off",     0, false },
	{ "true",    1,  true },
	{ "false",   0,  true },
	{ "1",       1,  true },
	{ "0",       0,  true },
	{ "always", -1,  true },
	{ NULL, 0, false }
};

/*
 * NVMEStromChunkBuffer
 */
typedef struct NVMEStromDMAChunk
{
	unsigned long	dma_task_id;/* handler of DMA task */
	BlockNumber		block_pos;	/* block number begin to read */
	unsigned int	num_blocks;	/* number of blocks to read */
	char		   *chunk_buf;	/* mapped DMA buffer */
	uint32_t	   *chunk_ids;	/* argument buffer for ioctl command */
} NVMEStromDMAChunk;

/* see storage/smgr/md.c */
typedef struct _MdfdVec
{
	File			mdfd_vfd;       /* fd number in fd.c's pool */
	BlockNumber		mdfd_segno;     /* segment number, from 0 */
	struct _MdfdVec *mdfd_chain;    /* next segment, or NULL */
} MdfdVec;

/*
 * Parallel Scan State of NVMEStrom
 */
typedef struct NVMEStromParallelDesc
{
	Oid			nsp_relid;		/* OID of relation to scan */
	BlockNumber	nsp_nblocks;	/* # blocks in relation at start of scan */
	pg_atomic_uint64 nsp_cblock;/* current block number */

#if PG_VERSION_NUM >= 100000
	/* statistics are available since PostgreSQL v10.0 */
	pg_atomic_uint64	nr_ram2ram;
	pg_atomic_uint64	nr_ssd2ram;
	pg_atomic_uint64	nr_dma_submit;
	pg_atomic_uint64	nr_dma_blocks;
#endif
	char		nsp_snapshot_data[FLEXIBLE_ARRAY_MEMBER];
} NVMEStromParallelDesc;

/*
 * Executor State of NVMEStrom
 */
typedef struct NVMEStromState {
	CustomScanState css;
	/* scan descriptor */
	NVMEStromParallelDesc *nsp_desc; /* NULL, if not under Gather */
	NVMEStromParallelDesc __nsp_desc_private; /* private buffer if not
											   * parallel execution */
	/* properties of chunk buffer */
	int			numa_node_id;	/* numa node id of NVMe-SSD device */
	int			num_chunks;		/* number of chunk buffers */
	size_t		chunk_sz;		/* nvme_strom.chunk_size in bytes */

	/* reference to system resources */
	void	   *mmap_dma_buf;	/* mapped DMA buffers */
	File	   *mdfd;			/* quick lookup table of relation's fd */
	Snapshot	worker_snapshot;/* snapshot that is registered */

	/* state of relation scan */
	NVMEStromDMAChunk *curr_dchunk;
	int			curr_bindex;
	int			curr_lineoff;
	HeapTupleData curr_tuple;

	/* state of asynchronous scan with DMA */
	bool		scan_done;		/* true, if already end of relation */
	int			dma_rindex;		/* index to read next */
	int			dma_windex;		/* index to write next */
	int			free_chunks;	/* number of available chunks */
	Buffer		vm_buffer;		/* buffer for visibility map */
	NVMEStromDMAChunk dma_chunks[FLEXIBLE_ARRAY_MEMBER];
} NVMEStromState;

/*
 * Misc utility functions for multi version supports
 */
#include "compatible.c"

/*
 * nvme_strom_ioctl - entrypoint of NVME-Strom
 */
static int
nvme_strom_ioctl(int cmd, const void *arg)
{
	static int		fdesc_nvme_strom = -1;

	if (fdesc_nvme_strom < 0)
	{
		fdesc_nvme_strom = open(NVME_STROM_IOCTL_PATHNAME, O_RDONLY);
		if (fdesc_nvme_strom < 0)
		{
			if (errno != ENOENT)
				elog(ERROR, "failed to open \"%s\": %m",
					 NVME_STROM_IOCTL_PATHNAME);
			return errno;
		}
	}
	return ioctl(fdesc_nvme_strom, cmd, arg);
}

/*
 * status of NVMe-Strom for each filesystem volume
 */
typedef struct
{
	Oid		tablespace_oid;
	bool	nvme_strom_supported;
	int		numa_node_id;
} vfs_nvme_status;

static void
vfs_nvme_cache_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	/* invalidate all the cached status */
	if (vfs_nvme_htable)
	{
		hash_destroy(vfs_nvme_htable);
		vfs_nvme_htable = NULL;
		nvme_last_tablespace_oid = InvalidOid;
	}
}

/*
 * tablespace_can_use_nvme_strom
 */
static bool
tablespace_can_use_nvme_strom(Oid tablespace_oid, int *p_numa_node_id,
							  bool be_quiet)
{
	vfs_nvme_status *entry;
	const char *pathname;
	int			fdesc;
	bool		found;

	if (!nvmestrom_enabled)
		return false;

	if (!OidIsValid(tablespace_oid))
		tablespace_oid = MyDatabaseTableSpace;

	/* quick lookup but sufficient for more than 99.99% cases */
	if (OidIsValid(nvme_last_tablespace_oid) &&
		nvme_last_tablespace_oid == tablespace_oid)
	{
		if (p_numa_node_id)
			*p_numa_node_id = nvme_last_numa_node_id;
		return nvme_last_tablespace_supported;
	}

	/* hash table to track status of NVMe-Strom */
	if (!vfs_nvme_htable)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(HASHCTL));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(vfs_nvme_status);
		vfs_nvme_htable = hash_create("VFS:NVMe-Strom status", 64,
									  &ctl, HASH_ELEM | HASH_BLOBS);
		CacheRegisterSyscacheCallback(TABLESPACEOID,
									  vfs_nvme_cache_callback, (Datum) 0);
	}
	entry = (vfs_nvme_status *) hash_search(vfs_nvme_htable,
											&tablespace_oid,
											HASH_ENTER,
											&found);
	if (found)
	{
		nvme_last_tablespace_oid = tablespace_oid;
		nvme_last_tablespace_supported = entry->nvme_strom_supported;
		nvme_last_numa_node_id = entry->numa_node_id;
		if (p_numa_node_id)
			*p_numa_node_id = entry->numa_node_id;
		return entry->nvme_strom_supported;
	}

	/* check whether the tablespace is supported */
	entry->tablespace_oid = tablespace_oid;
	entry->nvme_strom_supported = false;
	entry->numa_node_id = -1;

	pathname = GetDatabasePath(MyDatabaseId, tablespace_oid);
	fdesc = open(pathname, O_RDONLY | O_DIRECTORY);
	if (fdesc < 0)
	{
		elog(WARNING, "failed to open \"%s\" of tablespace \"%s\": %m",
			 pathname, get_tablespace_name(tablespace_oid));
    }
    else
    {
		StromCmd__CheckFile cmd;

		cmd.fdesc = fdesc;
		if (nvme_strom_ioctl(STROM_IOCTL__CHECK_FILE, &cmd) == 0 &&
			cmd.support_dma64 != 0)
		{
			entry->nvme_strom_supported = true;
			entry->numa_node_id = cmd.numa_node_id;
		}
		else
		{
			ereport((be_quiet ? DEBUG1 : NOTICE),
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("tablespace \"%s\" does not support NVMe-Strom",
							get_tablespace_name(tablespace_oid))));
		}
	}
	nvme_last_tablespace_oid = tablespace_oid;
	nvme_last_tablespace_supported = entry->nvme_strom_supported;
	nvme_last_numa_node_id = entry->numa_node_id;
	if (p_numa_node_id)
		*p_numa_node_id = entry->numa_node_id;
	return entry->nvme_strom_supported;
}

/*
 * relation_can_use_nvme_strom
 */
static bool
relation_can_use_nvme_strom(Relation relation,
							int *p_numa_node_id,
							bool be_quiet)
{
	Oid		tablespace_oid = RelationGetForm(relation)->reltablespace;

	return tablespace_can_use_nvme_strom(tablespace_oid,
										 p_numa_node_id,
										 be_quiet);
}

/*
 * cost_nvmestrom_scan
 */
static void
cost_nvmestrom_scan(Path *path,
					PlannerInfo *root,
					RelOptInfo *rel,
					ParamPathInfo *param_info)
{
	Cost		startup_cost = 0;
	Cost		cpu_run_cost;
	Cost		disk_run_cost;
	QualCost	qpqual_cost;
	Cost		cpu_per_tuple;
	Cost		startup_latency;
	int			nblocks_per_chunk = (nvmestrom_chunk_size_kb << 10) / BLCKSZ;

	/* Should only be applied to base relations */
	Assert(rel->relid > 0);
	Assert(rel->rtekind == RTE_RELATION);

	/* Mark the path with the correct row estimate */
	if (param_info)
		path->rows = param_info->ppi_rows;
    else
		path->rows = rel->rows;

    /*
     * disk costs
     */
	disk_run_cost = nvmestrom_seq_page_cost * rel->pages;
	startup_latency = nvmestrom_seq_page_cost * Min(nblocks_per_chunk,
													rel->pages);
    /* CPU costs */
    get_restriction_qual_cost(root, rel, param_info, &qpqual_cost);

	startup_cost += qpqual_cost.startup;
	cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
	cpu_run_cost = cpu_per_tuple * rel->tuples;
	/* tlist eval costs are paid per output row, not per tuple scanned */
	startup_cost += path->pathtarget->cost.startup;
	cpu_run_cost += path->pathtarget->cost.per_tuple * path->rows;

	/* Adjust costing for parallelism, if used. */
	if (path->parallel_workers > 0)
	{
		double		parallel_divisor = get_parallel_divisor(path);

		/* The CPU cost is divided among all the workers. */
		cpu_run_cost /= parallel_divisor;

		/*
		 * It may be possible to amortize some of the I/O cost, but probably
		 * not very much, because most operating systems already do aggressive
		 * prefetching.  For now, we assume that the disk run cost can't be
		 * amortized at all.
		 */

		/*
		 * In the case of a parallel plan, the row count needs to represent
		 * the number of tuples processed per worker.
		 */
		path->rows = clamp_row_est(path->rows / parallel_divisor);
	}
    path->startup_cost = startup_cost + startup_latency;
    path->total_cost = startup_cost + cpu_run_cost + disk_run_cost;
}

/*
 * create_nvmestrom_scan_path
 */
static CustomPath *
create_nvmestrom_scan_path(PlannerInfo *root,
						   RelOptInfo *rel,
						   Relids required_outer,
						   int parallel_workers)
{
	CustomPath	   *cpath = makeNode(CustomPath);

	/* construction of CustomPath */
	cpath->path.pathtype	= T_CustomScan;
	cpath->path.parent		= rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.param_info = get_baserel_parampathinfo(root, rel,
													   required_outer);
	cpath->path.parallel_aware = parallel_workers > 0 ? true : false;
	cpath->path.parallel_safe = rel->consider_parallel;
	cpath->path.parallel_workers = parallel_workers;
	cpath->path.pathkeys	= NIL;
	cpath->flags			= 0;
	cpath->custom_paths		= NIL;
	cpath->custom_private	= NIL;
	cpath->methods			= &nvmestrom_path_methods;
	cost_nvmestrom_scan(&cpath->path, root, rel, cpath->path.param_info);

	return cpath;
}

/*
 * nvmestrom_add_scan_path
 */
static void
nvmestrom_add_scan_path(PlannerInfo *root,
						RelOptInfo *rel,
						Index rti,
						RangeTblEntry *rte)
{
	CustomPath	   *cpath;
	Relids			required_outer = rel->lateral_relids;

	if (set_rel_pathlist_next)
		set_rel_pathlist_next(root, rel, rti, rte);

	if (!nvmestrom_enabled)
		return;
	if (rel->reloptkind != RELOPT_BASEREL ||
		rel->rtekind != RTE_RELATION)
		return;
	if (!tablespace_can_use_nvme_strom(rel->reltablespace, NULL, true))
		return;
	if (nvmestrom_enabled > 0 &&	/* nvme_strom.enabled != always */
		rel->pages < nvmestrom_nblocks_threshold)
		return;

	/* consider sequential ssd-scan */
	cpath = create_nvmestrom_scan_path(root, rel, required_outer, 0);
	add_path(rel, &cpath->path);

	/* consider parallel ssd-scan */
	if (rel->consider_parallel && required_outer == NULL)
	{
		int		parallel_workers = compute_parallel_worker(rel, rel->pages, 0);

		if (parallel_workers > 0)
		{
			cpath = create_nvmestrom_scan_path(root, rel,
											   required_outer,
											   parallel_workers);
			add_partial_path(rel, &cpath->path);
			generate_gather_paths(root, rel);
		}
	}
}

/*
 * PlanNVMEStromPath
 */
static Plan *
PlanNVMEStromPath(PlannerInfo *root,
				  RelOptInfo *rel,
				  struct CustomPath *best_path,
				  List *tlist,
				  List *clauses,
				  List *custom_plans)
{
	CustomScan *cscan;
	Index		scan_relid = rel->relid;

	/* it should be a base rel... */
	Assert(scan_relid > 0);
	Assert(rel->rtekind == RTE_RELATION);

	/* Sort clauses into best execution order */
	clauses = order_qual_clauses(root, clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	clauses = extract_actual_clauses(clauses, false);

	/* make a custom scan node */
	cscan = makeNode(CustomScan);
	cscan->scan.plan.targetlist	= tlist;
	cscan->scan.plan.qual		= clauses;
	cscan->scan.plan.lefttree	= NULL;
	cscan->scan.plan.righttree	= NULL;
	cscan->scan.scanrelid		= scan_relid;
	cscan->flags				= best_path->flags;
	cscan->methods				= &nvmestrom_plan_methods;

	return &cscan->scan.plan;
}

/*
 * CreateNVMEStromState
 */
static Node *
CreateNVMEStromState(CustomScan *cscan)
{
	NVMEStromState *nss;
	int				num_chunks = (nvmestrom_buffer_size_kb /
								  nvmestrom_chunk_size_kb);
	nss = palloc0(offsetof(NVMEStromState, dma_chunks[num_chunks]));
	NodeSetTag(nss, T_CustomScanState);
	nss->css.methods = &nvmestrom_exec_methods;

	return (Node *) nss;
}

/*
 * ExecInitNVMEStrom
 */
static void
ExecInitNVMEStrom(CustomScanState *node,
				  EState *estate,
				  int eflags)
{
	NVMEStromState *nss = (NVMEStromState *) node;
	Relation		relation = nss->css.ss.ss_currentRelation;

	if (!relation_can_use_nvme_strom(relation, &nss->numa_node_id, false))
		elog(ERROR, "Bug? unable to run NVMe-Strom on relation \"%s\"",
			 RelationGetRelationName(relation));

	nss->nsp_desc = NULL;		/* to be set later */
	nss->mmap_dma_buf = NULL;	/* to be mapped later */
	nss->mdfd = NULL;			/* to be set later */
	nss->worker_snapshot = NULL;/* to be set on demand */
	nss->chunk_sz = nvmestrom_chunk_size_kb << 10;
	nss->num_chunks = nvmestrom_buffer_size_kb / nvmestrom_chunk_size_kb;
	nss->curr_dchunk = NULL;
	nss->curr_bindex = 0;
	nss->curr_lineoff = FirstOffsetNumber;

	nss->dma_rindex = 0;
	nss->dma_windex = 0;
	nss->free_chunks = nss->num_chunks;
	nss->vm_buffer = InvalidBuffer;
}


#define DMABUFFER_TRACK_HASHSIZE	23
static dlist_head	dma_buffer_tracker_list[DMABUFFER_TRACK_HASHSIZE];
typedef struct dma_buffer_tracker
{
	dlist_node		chain;
	pg_crc32		crc;
	ResourceOwner	owner;
	void		   *dma_buffer;
	size_t			buffer_len;
} dma_buffer_tracker;

/*
 * NVMEStromRememberDMABuffer
 */
static void
NVMEStromRememberDMABuffer(ResourceOwner owner,
						   void *dma_buffer, size_t buffer_len)
{
	dma_buffer_tracker *tracker;
	pg_crc32	crc;
	int			index;

	tracker = MemoryContextAlloc(CacheMemoryContext,
								 sizeof(dma_buffer_tracker));
	INIT_LEGACY_CRC32(crc);
	COMP_LEGACY_CRC32(crc, &dma_buffer, sizeof(void *));
	FIN_LEGACY_CRC32(crc);
	index = crc % DMABUFFER_TRACK_HASHSIZE;

	tracker->crc = crc;
	tracker->owner = owner;
	tracker->dma_buffer = dma_buffer;
	tracker->buffer_len = buffer_len;

	dlist_push_tail(&dma_buffer_tracker_list[index], &tracker->chain);
}

/*
 * NVMEStromForgetDMABuffer
 */
static void
NVMEStromForgetDMABuffer(void *dma_buffer)
{
	pg_crc32	crc;
	int			index;
	dlist_iter	iter;

	INIT_LEGACY_CRC32(crc);
	COMP_LEGACY_CRC32(crc, &dma_buffer, sizeof(void *));
	FIN_LEGACY_CRC32(crc);
	index = crc % DMABUFFER_TRACK_HASHSIZE;

	dlist_foreach(iter, &dma_buffer_tracker_list[index])
	{
		dma_buffer_tracker *tracker = (dma_buffer_tracker *)
			dlist_container(dma_buffer_tracker, chain, iter.cur);

		if (tracker->dma_buffer == dma_buffer)
		{
			dlist_delete(&tracker->chain);
			if (munmap(tracker->dma_buffer, tracker->buffer_len))
				elog(WARNING, "failed on munmap(2): %m");
			pfree(tracker);
			return;
		}
	}
	elog(WARNING, "Bug? DMA buffer %p was not tracked, just munmap(2)",
		 dma_buffer );
	if (munmap(dma_buffer, nvmestrom_buffer_size_kb << 10))
		elog(WARNING, "failed on munmap(2): %m");
}

/*
 * NVMEStromCleanupDMABuffer
 */
static void
NVMEStromCleanupDMABuffer(ResourceReleasePhase phase,
						  bool is_commit,
						  bool is_toplevel,
						  void *arg)
{
	int			index;
	dlist_mutable_iter iter;

	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;

	for (index=0; index < DMABUFFER_TRACK_HASHSIZE; index++)
	{
		dlist_foreach_modify(iter, &dma_buffer_tracker_list[index])
		{
			dma_buffer_tracker *tracker = (dma_buffer_tracker *)
				dlist_container(dma_buffer_tracker, chain, iter.cur);

			if (tracker->owner == CurrentResourceOwner)
			{
				if (is_commit)
					elog(WARNING, "DMA buffer reference leak (%p-%p)",
						 (char *)tracker->dma_buffer,
						 (char *)tracker->dma_buffer + tracker->buffer_len);
				dlist_delete(&tracker->chain);
				if (munmap(tracker->dma_buffer, tracker->buffer_len))
					elog(WARNING, "failed on munmap(2): %m");
				pfree(tracker);
			}
		}
	}
}

/*
 * ExecInitNVMEStromLater
 *  -- initialization of exec-state on the first call of ExecProcNode().
 *
 * TODO: NUMA binding according to the storage location
 */
static void
ExecInitNVMEStromLater(NVMEStromState *nss)
{
	StromCmd__AllocDMABuffer cmd;
	Relation	relation = nss->css.ss.ss_currentRelation;
	EState	   *estate = nss->css.ss.ps.state;
	BlockNumber	nr_blocks;
	BlockNumber	nr_segs;
	MdfdVec	   *vec;
	char	   *dma_buffer = NULL;
	int			i, dma_fdesc = -1;

	if (nss->mmap_dma_buf != NULL)
		elog(FATAL, "Bug? DMA buffer is already allocated");

	/*
	 * Setup quick lookup table of relation's file descriptor
	 */
	nr_blocks = RelationGetNumberOfBlocks(relation);
	nr_segs = (nr_blocks + (BlockNumber) RELSEG_SIZE - 1) / RELSEG_SIZE;
	nss->mdfd = MemoryContextAlloc(estate->es_query_cxt,
								   sizeof(File) * nr_segs);
	memset(nss->mdfd, -1, sizeof(File) * nr_segs);
	vec = relation->rd_smgr->md_fd[MAIN_FORKNUM];
	while (vec)
	{
		if (vec->mdfd_vfd < 0 ||
			vec->mdfd_segno >= nr_segs)
			elog(ERROR, "Bug? MdfdVec {vfd=%d segno=%u} is out of range",
				 vec->mdfd_vfd, vec->mdfd_segno);
		nss->mdfd[vec->mdfd_segno] = vec->mdfd_vfd;
		vec = vec->mdfd_chain;
	}
	/* sanity checks */
	for (i=0; i < nr_segs; i++)
	{
		if (nss->mdfd[i] < 0)
			elog(ERROR, "Bug? there is a hole segment which was not open");
	}

	PG_TRY();
	{
		/* allocation of dma buffer */
		memset(&cmd, 0, sizeof(StromCmd__AllocDMABuffer));
		cmd.length = nss->chunk_sz * (size_t)nss->num_chunks;
		cmd.node_id = -1;
		if (nvme_strom_ioctl(STROM_IOCTL__ALLOC_DMA_BUFFER, &cmd))
			elog(ERROR, "failed on ioctl(STROM_IOCTL__ALLOC_DMA_BUFFER)");
		dma_fdesc = cmd.dmabuf_fdesc;

		/* map dma buffer */
		dma_buffer = mmap(NULL, cmd.length,
						  PROT_READ | PROT_WRITE,
						  MAP_SHARED,
						  dma_fdesc, 0);
		if (dma_buffer == MAP_FAILED)
			elog(ERROR, "failed on mmap(2) with DMA buffer FD");
		close(dma_fdesc);
		dma_fdesc = -1;

		/* track dma_buffer for error handling */
		NVMEStromRememberDMABuffer(CurrentResourceOwner,
								   dma_buffer, cmd.length);
		for (i=0; i < nss->num_chunks; i++)
		{
			NVMEStromDMAChunk  *dchunk = &nss->dma_chunks[i];

			dchunk->chunk_buf = dma_buffer + (size_t)i * nss->chunk_sz;
			dchunk->chunk_ids = palloc0(sizeof(uint32_t) *
										(nss->chunk_sz / BLCKSZ));
		}
	}
	PG_CATCH();
	{
		if (dma_buffer != NULL && dma_buffer != MAP_FAILED)
		{
			if (munmap(dma_buffer, cmd.length))
				elog(FATAL, "failed on munmap(2) during error handling: %m");
		}
		if (dma_fdesc >= 0)
			close(dma_fdesc);
		PG_RE_THROW();
	}
	PG_END_TRY();
	nss->mmap_dma_buf = dma_buffer;
}

/*
 * nvmestrom_load_chunk
 */
static void
nvmestrom_load_chunk(NVMEStromState *nss,
					 NVMEStromDMAChunk *dchunk)
{
	Relation		relation = nss->css.ss.ss_currentRelation;
	SMgrRelation	smgr = relation->rd_smgr;
	Snapshot		snapshot = nss->css.ss.ps.state->es_snapshot;
	BlockNumber		block_pos = dchunk->block_pos;
	unsigned int	num_blocks = dchunk->num_blocks;
	int				i, j, k, l;

	for (i=0, j=0, k=0; i < num_blocks; i++, block_pos++)
	{
		Buffer		buffer;
		Page		spage;
		Page		dpage;
		bool		all_visible;

		if (VM_ALL_VISIBLE(relation, block_pos, &nss->vm_buffer))
		{
			BufferTag	newTag;
			uint32		newHash;
			LWLock	   *newPartitionLock = NULL;
			int			buf_id;

			/* Does the source block exists on the shared buffer? */
			INIT_BUFFERTAG(newTag, smgr->smgr_rnode.node,
						   MAIN_FORKNUM, block_pos + i);
			newHash = BufTableHashCode(&newTag);
			newPartitionLock = BufMappingPartitionLock(newHash);

			LWLockAcquire(newPartitionLock, LW_SHARED);
			buf_id = BufTableLookup(&newTag, newHash);
			LWLockRelease(newPartitionLock);
			if (buf_id < 0)
			{
				/* ok, not cached and all-visible block */
				dchunk->chunk_ids[j++] = block_pos;
				continue;
			}
		}
		/*
		 * Elsewhere, block is cached or not all-visible
		 */
		l = num_blocks - (++k);
		buffer = ReadBufferExtended(relation,
									MAIN_FORKNUM,
									block_pos,
									RBM_NORMAL,
									NULL);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		spage = (Page) BufferGetPage(buffer);
		dpage = (Page) dchunk->chunk_buf + BLCKSZ * l;
		memcpy(dpage, spage, BLCKSZ);

		all_visible = (PageIsAllVisible(spage) &&
					   !snapshot->takenDuringRecovery);
		if (!all_visible)
		{
			int				lines = PageGetMaxOffsetNumber(dpage);
			OffsetNumber	lineoff;
			ItemId			lpp;

			for (lineoff = FirstOffsetNumber, lpp = PageGetItemId(dpage,
																  lineoff);
				 lineoff <= lines;
				 lineoff++, lpp++)
			{
				HeapTupleData	tup;
				bool			valid;

				if (!ItemIdIsNormal(lpp))
					continue;

				tup.t_tableOid = RelationGetRelid(relation);
				tup.t_data = (HeapTupleHeader) PageGetItem((Page) dpage, lpp);
				tup.t_len = ItemIdGetLength(lpp);
				ItemPointerSet(&tup.t_self, block_pos, lineoff);

				valid = HeapTupleSatisfiesVisibility(&tup, snapshot, buffer);
				CheckForSerializableConflictOut(valid, relation, &tup,
												buffer, snapshot);
				if (!valid)
					ItemIdSetUnused(lpp);
			}
		}
		dchunk->chunk_ids[l] = block_pos;
		UnlockReleaseBuffer(buffer);
		PageSetAllVisible(dpage);
	}
	Assert(num_blocks == j + k);

	if (j == 0)
	{
		/* no need to issue DMA requests */
		dchunk->dma_task_id = ~0UL;
	}
	else
	{
		StromCmd__MemCopySsdToRam cmd;
		File	vfd = nss->mdfd[dchunk->block_pos / RELSEG_SIZE];

		memset(&cmd, 0, offsetof(StromCmd__MemCopySsdToRam, dest_uaddr));
		cmd.dest_uaddr = dchunk->chunk_buf;
		cmd.file_desc = FileGetRawDesc(vfd);
		cmd.nr_chunks = j;
		cmd.chunk_sz = BLCKSZ;
		cmd.relseg_sz = RELSEG_SIZE;
		cmd.chunk_ids = dchunk->chunk_ids;
		if (nvme_strom_ioctl(STROM_IOCTL__MEMCPY_SSD2RAM, &cmd))
			elog(ERROR, "failed on ioctl(STROM_IOCTL__MEMCPY_SSD2RAM) : %m");
		dchunk->dma_task_id = cmd.dma_task_id;
#if PG_VERSION_NUM >= 100000
		pg_atomic_fetch_add_u64(&nsp_desc->nr_ram2ram, cmd.nr_ram2ram);
		pg_atomic_fetch_add_u64(&nsp_desc->nr_ssd2ram, cmd.nr_ssd2ram);
		pg_atomic_fetch_add_u64(&nsp_desc->nr_dma_submit, cmd.nr_dma_submit);
		pg_atomic_fetch_add_u64(&nsp_desc->nr_dma_blocks, cmd.nr_dma_blocks);
#endif
	}
}

/*
 * nvmestrom_next_chunk
 */
static bool
nvmestrom_next_chunk(NVMEStromState *nss)
{
	NVMEStromParallelDesc *nsp_desc = nss->nsp_desc;
	NVMEStromDMAChunk *dchunk;
	BlockNumber		block_pos;
	unsigned int	num_blocks = nss->chunk_sz / BLCKSZ;

	/* release the last chunk which is already read */
	if (nss->curr_dchunk)
	{
		nss->curr_dchunk = NULL;
		nss->free_chunks++;
	}

	/* enqueue request until fill of dma chunks */
	while (!nss->scan_done && nss->free_chunks > 0)
	{
		dchunk = &nss->dma_chunks[nss->dma_windex % nss->num_chunks];

		/* Identify the next range of blocks to read */
		block_pos = pg_atomic_fetch_add_u64(&nsp_desc->nsp_cblock,
											num_blocks);
		if (block_pos >= nsp_desc->nsp_nblocks)
		{
			nss->scan_done = true;
			break;
		}
		if (block_pos + num_blocks > nsp_desc->nsp_nblocks)
		{
			num_blocks = nsp_desc->nsp_nblocks - block_pos;
			nss->scan_done = true;
		}
		/* sanity checks */
		Assert(num_blocks > 0);
		Assert((block_pos / RELSEG_SIZE) ==
			   (block_pos + num_blocks - 1) / RELSEG_SIZE);
		/* load a chunk with sync or async manner */
		dchunk->block_pos = block_pos;
		dchunk->num_blocks = num_blocks;
		nvmestrom_load_chunk(nss, dchunk);
		/* Increment chunk usage */
		nss->dma_windex++;
		nss->free_chunks--;
	}

	/* OK, we have no chunks to be returned any more */
	if (nss->dma_windex == nss->dma_rindex)
	{
		Assert(nss->scan_done);
		Assert(nss->free_chunks == nss->num_chunks);
		return false;
	}

	/* Wait for the next available chunk */
	dchunk = &nss->dma_chunks[nss->dma_rindex++ % nss->num_chunks];
	while (dchunk->dma_task_id != ~0UL)
	{
		StromCmd__MemCopyWait cmd;

		cmd.dma_task_id = dchunk->dma_task_id;
		if (nvme_strom_ioctl(STROM_IOCTL__MEMCPY_WAIT, &cmd) == 0)
			dchunk->dma_task_id = ~0UL;
		else if (errno == EINTR)
			CHECK_FOR_INTERRUPTS();
		else
			elog(ERROR, "failed on ioctl(STROM_IOCTL__MEMCPY_WAIT) : %m");
	}
	nss->curr_dchunk = dchunk;
	nss->curr_bindex = 0;
	nss->curr_lineoff = FirstOffsetNumber;

	return true;
}

/*
 * nvmestrom_next_tuple
 */
static TupleTableSlot *
nvmestrom_next_tuple(NVMEStromState *nss)
{
	NVMEStromDMAChunk *dchunk = nss->curr_dchunk;
	Relation		relation = nss->css.ss.ss_currentRelation;
	TupleTableSlot *slot = nss->css.ss.ss_ScanTupleSlot;
	HeapTuple		tuple = &nss->curr_tuple;
	BlockNumber		curr_blkid;
	PageHeader		hpage;
	int				lines;

	while (nss->curr_bindex < dchunk->num_blocks)
	{
		curr_blkid = dchunk->chunk_ids[nss->curr_bindex];
		hpage = (PageHeader)(dchunk->chunk_buf + nss->curr_bindex * BLCKSZ);
		lines = PageGetMaxOffsetNumber(hpage);

		while (nss->curr_lineoff <= lines)
		{
			int		lineoff = nss->curr_lineoff++;
			ItemId	lpp = PageGetItemId(hpage, lineoff);

			if (!ItemIdIsNormal(lpp))
				continue;
			Assert(PageIsAllVisible(hpage));
			tuple->t_tableOid = RelationGetRelid(relation);
			tuple->t_data = (HeapTupleHeader) PageGetItem(hpage, lpp);
			tuple->t_len = ItemIdGetLength(lpp);
			ItemPointerSet(&tuple->t_self, curr_blkid, lineoff);

			ExecStoreTuple(tuple, slot, InvalidBuffer, false);

			return slot;
		}
		nss->curr_bindex++;
		nss->curr_lineoff = FirstOffsetNumber;
	}
	return NULL;
}

/*
 * ExecNVMEStromNext
 */
static TupleTableSlot *
ExecNVMEStromNext(CustomScanState *node)
{
	NVMEStromState *nss = (NVMEStromState *) node;
	TupleTableSlot *slot = NULL;

	/* Init private scan descriptor if not under Gatger node */
	if (!nss->nsp_desc)
	{
		Relation	relation = nss->css.ss.ss_currentRelation;

		memset(&nss->__nsp_desc_private, 0, sizeof(NVMEStromParallelDesc));
		nss->nsp_desc = &nss->__nsp_desc_private;
		nss->nsp_desc->nsp_relid = RelationGetRelid(relation);
		nss->nsp_desc->nsp_nblocks = RelationGetNumberOfBlocks(relation);
	}
	/* Map DMA buffer on userspace */
	if (!nss->mmap_dma_buf)
		ExecInitNVMEStromLater(nss);

	while (!nss->curr_dchunk || !(slot = nvmestrom_next_tuple(nss)))
	{
		if (!nvmestrom_next_chunk(nss))
			break;
	}
	return slot;
}

/*
 * ExecNVMEStromRecheck
 */
static bool
ExecNVMEStromRecheck(CustomScanState *node, TupleTableSlot *slot)
{
	return true;
}

/*
 * ExecNVMEStrom
 */
static TupleTableSlot *
ExecNVMEStrom(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) ExecNVMEStromNext,
					(ExecScanRecheckMtd) ExecNVMEStromRecheck);
}

/*
 * ExecEndNVMEStrom
 */
static void
ExecEndNVMEStrom(CustomScanState *node)
{
	NVMEStromState *nss = (NVMEStromState *) node;

	if (nss->worker_snapshot)
		UnregisterSnapshot(nss->worker_snapshot);
	if (nss->mmap_dma_buf)
	{
		NVMEStromForgetDMABuffer(nss->mmap_dma_buf);
		nss->mmap_dma_buf = NULL;
	}
	if (BufferIsValid(nss->vm_buffer))
		ReleaseBuffer(nss->vm_buffer);
}

/*
 * ExecReScanNVMEStrom
 */
static void
ExecReScanNVMEStrom(CustomScanState *node)
{
	NVMEStromState *nss = (NVMEStromState *) node;

	/* revert scan block */
	if (nss->nsp_desc)
		pg_atomic_init_u64(&nss->nsp_desc->nsp_cblock, 0);
}

/*
 * ExecNVMEStromEstimateDSM
 */
static Size
ExecNVMEStromEstimateDSM(CustomScanState *node,
						 ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	return add_size(offsetof(NVMEStromParallelDesc,
							 nsp_snapshot_data),
					EstimateSnapshotSpace(estate->es_snapshot));
}

/*
 * ExecNVMEStromInitDSM
 */
static void
NVMEStromInitDSM(CustomScanState *node,
				 ParallelContext *pcxt,
				 void *coordinate)
{
	NVMEStromState *nss = (NVMEStromState *) node;
	NVMEStromParallelDesc *nsp_desc = coordinate;
	Relation		relation = nss->css.ss.ss_currentRelation;
	Snapshot		snapshot = nss->css.ss.ps.state->es_snapshot;

	/* init NVMEStromParallelDesc */
	memset(nsp_desc, 0, sizeof(NVMEStromParallelDesc));
	nsp_desc->nsp_relid = RelationGetRelid(relation);
	nsp_desc->nsp_nblocks = RelationGetNumberOfBlocks(relation);
	SerializeSnapshot(snapshot, nsp_desc->nsp_snapshot_data);
	nss->nsp_desc = nsp_desc;
}

/*
 * NVMEStromInitWorker
 */
static void
NVMEStromInitWorker(CustomScanState *node,
					shm_toc *toc,
					void *coordinate)
{
	NVMEStromState *nss = (NVMEStromState *) node;
	NVMEStromParallelDesc  *nsp_desc = coordinate;
	Relation		relation = nss->css.ss.ss_currentRelation;
	Snapshot		snapshot;

	/* restore the snapshot */
	Assert(RelationGetRelid(relation) == nsp_desc->nsp_relid);
	snapshot = RestoreSnapshot(nsp_desc->nsp_snapshot_data);
	RegisterSnapshot(snapshot);
	nss->worker_snapshot = snapshot;
	nss->nsp_desc = nsp_desc;
}

/*
 * ExplainNVMEStrom
 */
static void
ExplainNVMEStrom(CustomScanState *node,
				 List *ancestors,
				 ExplainState *es)
{}

/*
 * Main entrypoint of NVMe-Strom.
 */
void
_PG_init(void)
{
	Size	shared_buffer_size = (Size)NBuffers * (Size)BLCKSZ;
	int		i;

	/*
	 * MEMO: Threshold of table's physical size to use NVMe-Strom:
	 *   ((System RAM size) -
	 *    (shared_buffer size)) * 0.67 + (shared_buffer size)
	 */
	sysconf_pagesize = sysconf(_SC_PAGESIZE);
	if (sysconf_pagesize < 0)
		elog(ERROR, "failed on sysconf(_SC_PAGESIZE): %m");
	sysconf_phys_pages = sysconf(_SC_PHYS_PAGES);
	if (sysconf_phys_pages < 0)
		elog(ERROR, "failed on sysconf(_SC_PHYS_PAGES): %m");
	if (sysconf_pagesize * sysconf_phys_pages < shared_buffer_size)
		elog(ERROR, "Bug? shared_buffer is larger than system RAM");
    nvmestrom_nblocks_threshold = ((sysconf_pagesize * sysconf_phys_pages -
									shared_buffer_size) * 2 / 3 +
								   shared_buffer_size) / BLCKSZ;

	/* nvme_strom.enabled */
	DefineCustomEnumVariable("nvme_strom.enabled",
							 "enables/disabled direct SSD scan by NVMe-Strom",
							 NULL,
							 &nvmestrom_enabled,
							 1,
							 nvmestrom_enabled_options,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);
	/* nvme_strom.chunk_size */
	DefineCustomIntVariable("nvme_strom.chunk_size",
							"Unit size of userspace mapped DMA buffer",
							NULL,
							&nvmestrom_chunk_size_kb,
							32768,			/* 32MB */
							BLCKSZ / 1024,	/* 8KB */
							INT_MAX,
							PGC_USERSET,
							GUC_NOT_IN_SAMPLE | GUC_UNIT_KB,
							NULL, NULL, NULL);
	/* nvme_strom.buffer_size */
	DefineCustomIntVariable("nvme_strom.buffer_size",
							"Total size of userspace mapped DMA buffer",
							NULL,
							&nvmestrom_buffer_size_kb,
							262144,			/* 256MB */
							BLCKSZ / 1024,	/* 8KB */
							INT_MAX,
							PGC_USERSET,
							GUC_NOT_IN_SAMPLE | GUC_UNIT_KB,
							NULL, NULL, NULL);
	//TODO: nvme_strom.buffer_size_limit ... default: 2GB

	/* nvme_strom.seq_page_cost */
	DefineCustomRealVariable("nvme_strom.seq_page_cost",
							 "Sets the planner's estimate of the cost of "
							 "a sequentially fetched disk page by NVMe-Strom",
							 NULL,
							 &nvmestrom_seq_page_cost,
							 DEFAULT_SEQ_PAGE_COST / 4,
							 0.0,
							 DBL_MAX,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);
	if (nvmestrom_chunk_size_kb % (BLCKSZ / 1024) != 0)
		elog(ERROR, "nvme_strom.chunk_size must be multiple of BLCKSZ");
	if (nvmestrom_buffer_size_kb % nvmestrom_chunk_size_kb != 0)
		elog(ERROR, "nvme_strom.buffer_size must be multiple of nvme_strom.chunk_size");

	/* setup path methods */
	memset(&nvmestrom_path_methods, 0, sizeof(CustomPathMethods));
	nvmestrom_path_methods.CustomName			= "NVMe Strom";
	nvmestrom_path_methods.PlanCustomPath		= PlanNVMEStromPath;

	/* setup plan methods */
	memset(&nvmestrom_plan_methods, 0, sizeof(CustomScanMethods));
	nvmestrom_plan_methods.CustomName			= "NVMe Strom";
	nvmestrom_plan_methods.CreateCustomScanState = CreateNVMEStromState;
	RegisterCustomScanMethods(&nvmestrom_plan_methods);

	/* setup exec methods */
	memset(&nvmestrom_exec_methods, 0, sizeof(CustomExecMethods));
	nvmestrom_exec_methods.CustomName			= "NVMe Strom";
	nvmestrom_exec_methods.BeginCustomScan		= ExecInitNVMEStrom;
	nvmestrom_exec_methods.ExecCustomScan		= ExecNVMEStrom;
	nvmestrom_exec_methods.EndCustomScan		= ExecEndNVMEStrom;
	nvmestrom_exec_methods.ReScanCustomScan		= ExecReScanNVMEStrom;
	nvmestrom_exec_methods.EstimateDSMCustomScan = ExecNVMEStromEstimateDSM;
	nvmestrom_exec_methods.InitializeDSMCustomScan = NVMEStromInitDSM;
	nvmestrom_exec_methods.InitializeWorkerCustomScan = NVMEStromInitWorker;
	nvmestrom_exec_methods.ExplainCustomScan	= ExplainNVMEStrom;

	/* hook registration */
	set_rel_pathlist_next = set_rel_pathlist_hook;
	set_rel_pathlist_hook = nvmestrom_add_scan_path;

	/* misc initialization */
	for (i=0; i < DMABUFFER_TRACK_HASHSIZE; i++)
		dlist_init(&dma_buffer_tracker_list[i]);
	RegisterResourceReleaseCallback(NVMEStromCleanupDMABuffer, NULL);
}
