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
#include "nodes/extensible.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/subselect.h"
#include "storage/bufmgr.h"
#include "utils/guc.h"
#include "utils/spccache.h"
#include "nvme_strom.h"
#include <unistd.h>

PG_MODULE_MAGIC;
void	_PG_init(void);

static set_rel_pathlist_hook_type set_rel_pathlist_next;
static CustomPathMethods	nvmestrom_path_methods;
static CustomScanMethods	nvmestrom_plan_methods;
static CustomExecMethods	nvmestrom_exec_methods;
static bool					nvmestrom_enabled;			/* GUC */
static int					nvmestrom_chunk_size_kb;	/* GUC */
static int					nvmestrom_buffer_size_kb;	/* GUC */
static long					sysconf_pagesize;	/* _SC_PAGESIZE */
static long					sysconf_phys_pages;	/* _SC_PHYS_PAGES */
static long					ssdscan_threshold;

/*
 * Parallel Scan State of NVMEStrom
 */
typedef struct NVMEStromParallelDesc
{
	Oid			nss_relid;		/* OID of relation to scan */
	BlockNumber	nss_nblocks;	/* # blocks in relation at start of scan */
	pg_atomic_uint64 nss_cblock;/* current block number */
	size_t		nss_chunk_sz;	/* nvme_strom.chunk_size in bytes */
	size_t		nss_buffer_sz;	/* nvme_strom.buffer_size in bytes */
	char		nss_snapshot_data[FLEXIBLE_ARRAY_MEMBER];
} NVMEStromParallelDesc;

/*
 * Executor State of NVMEStrom
 */
typedef struct NVMEStromState {
	CustomScanState css;
	NVMEStromParallelDesc *pdesc;
	BlockNumber	nss_nblocks;	/* # blocks in relation at start of scan */
	BlockNumber	nss_cblock;		/* current block number */


	File		ioctl_filp;
	size_t		chunk_sz;		/* unit size of chunks */
	int			chunk_nums;		/* number of chunks */
	char	   *chunk_buffer;	/* mapped DMA buffer */

	int			dma_rindex;		/* index to read next */
	int			dma_windex;		/* index to stpre next */
	unsigned long dma_tasks[FLEXIBLE_ARRAY_MEMBER];
} NVMEStromState;

/*
 * Misc utility functions for multi version supports
 */
#include "compatible.c"

/*
 * cost_ssdscan
 */
static void
cost_ssdscan(Path *path,
			 PlannerInfo *root,
			 RelOptInfo *rel,
			 ParamPathInfo *param_info)
{
	Cost		startup_cost = 0;
	Cost		cpu_run_cost;
	Cost		disk_run_cost;
	double		spc_seq_page_cost;
	QualCost	qpqual_cost;
	Cost		cpu_per_tuple;

	/* Should only be applied to base relations */
	Assert(rel->relid > 0);
	Assert(rel->rtekind == RTE_RELATION);

	/* Mark the path with the correct row estimate */
	if (param_info)
		path->rows = param_info->ppi_rows;
    else
		path->rows = rel->rows;

	/* fetch estimated page cost for tablespace containing table */
	get_tablespace_page_costs(rel->reltablespace,
							  NULL,
							  &spc_seq_page_cost);



    /*
     * disk costs
     */
    disk_run_cost = spc_seq_page_cost * rel->pages;
	//needs to discount disk costs


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
    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + cpu_run_cost + disk_run_cost;
}

/*
 * create_ssdscan_path
 */
static CustomPath *
create_ssdscan_path(PlannerInfo *root,
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
	cost_ssdscan(&cpath->path, root, rel, cpath->path.param_info);

	return cpath;
}

/*
 * ssdscan_add_scan_path
 */
static void
ssdscan_add_scan_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  Index rti,
					  RangeTblEntry *rte)
{
	CustomPath	   *cpath;
	Relids			required_outer = rel->lateral_relids;

	if (!nvmestrom_enabled)
		return;
	/* make no sense, if table size is capable to load RAM */
	if (rel->pages < ssdscan_threshold)
		return;

	/* consider sequential ssd-scan */
	cpath = create_ssdscan_path(root, rel, required_outer, 0);
	add_path(rel, &cpath->path);

	/* consider parallel ssd-scan */
	if (rel->consider_parallel && required_outer == NULL)
	{
		int		parallel_workers = compute_parallel_worker(rel, rel->pages, 0);

		if (parallel_workers > 0)
		{
			cpath = create_ssdscan_path(root, rel,
										required_outer,
										parallel_workers);
			add_partial_path(rel, &cpath->path);
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
	NVMEStromState *nss = palloc0(sizeof(NVMEStromState));

	NodeSetTag(nss, T_CustomScanState);
	nss->css.methods = &nvmestrom_exec_state;

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

	// init extra attributes here
	// common portions are already initialized by core
	
}

static void
InitNVMEStromScanDesc()
{}

/*
 * ExecNVMEStrom
 */
static TupleTableSlot *
ExecNVMEStrom(CustomScanState *node)
{
	// init scan descriptor on the first call



	return NULL;
}

/*
 * ExecEndNVMEStrom
 */
static void
ExecEndNVMEStrom(CustomScanState *node)
{
	// clear extra attributes
}

/*
 * ExecReScanNVMEStrom
 */
static void
ExecReScanNVMEStrom(CustomScanState *node)
{
	// revert scan pointer

}

/*
 * ExecNVMEStromEstimateDSM
 */
static Size
ExecNVMEStromEstimateDSM(CustomScanState *node,
						 ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->pscan_len = add_size(offsetof(NVMEStromParallelDesc,
										nss_snapshot_data),
							   EstimateSnapshotSpace(snapshot));
	shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	return 0;
}

/*
 * NVMEStromBeginParallelScan
 */
static void
NVMEStromBeginParallelScan(NVMEStromState *nss,
						   NVMEStromParallelDesc *nsp_desc)
{
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
	NVMEStromParallelDesc *nsp_desc;
	Relation		relation = nss->css.ss.ss_currentRelation;
	Snapshot		snapshot = nss->css.ss.ps.state->es_snapshot;

	nsp_desc = shm_toc_allocate(pcxt->toc, nss->css.pscan_len);
	/* init NVMEStromParallelDesc */
	nsp_desc->nss_relid = RelationGetRelid(relation);
	nsp_desc->nss_nblocks = RelationGetNumberOfBlocks(relation);
	pg_atomic_init_u64(&nsp_desc->nss_cblock, 0);
	nsp_desc->nss_chunk_sz = (size_t)nvmestrom_chunk_size_kb << 10;
	nsp_desc->nss_buffer_sz = (size_t)nvmestrom_buffer_size_kb << 10;
	SerializeSnapshot(snapshot, nsp_desc->nsp_snaphot_data);

	/* share with background workers */
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, nsp_desc);
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
	NVMEStromParallelDesc  *nsp_desc;
	Relation		relation = nss->css.ss.ss_currentRelation;
	Snapshot		snapshot;

	/* restore the snapshot */
	nsp_desc = shm_toc_lookup(toc, node->ss.ps.plan->plan_node_id);
	Assert(RelationGetRelid(relation) == nsp_desc->nsp_relid);
	snapshot = RestoreSnapshot(nsp_desc->nsp_snaphot_data);
	RegisterSnapshot(snapshot);
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
    ssdscan_threshold = ((sysconf_pagesize * sysconf_phys_pages -
						  shared_buffer_size) * 2 / 3 +
						 shared_buffer_size) / BLCKSZ;

	/* nvme_strom.enabled */
	DefineCustomBoolVariable("nvme_strom.enabled",
							 "Enables/disables direct SSD scan by NVMe-Strom",
							 NULL,
							 &nvmestrom_enabled,
							 true,
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
	set_rel_pathlist_hook = ssdscan_add_scan_path;
}
