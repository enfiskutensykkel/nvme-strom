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
static CustomPathMethods	ssdscan_path_methods;
static CustomScanMethods	ssdscan_plan_methods;
static CustomExecMethods	ssdscan_exec_methods;
static bool					enable_ssdscan;
static long					sysconf_pagesize;	/* _SC_PAGESIZE */
static long					sysconf_phys_pages;	/* _SC_PHYS_PAGES */
static long					ssdscan_threshold;

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
	Path		   *spath = makeNode(Path);

	/*
	 * NOTE: Some of optimizer functions are declared as static function,
	 * thus, extension cannot call them and need to cut & paste. However,
	 * these are just maintenance burden for us.
	 * So, we make optimizer construct a simple/equivalent plan, then
	 * pick up its structure for our use.
	 */
	spath->pathtype = T_SeqScan;
	spath->parent = rel;
	spath->pathtarget = rel->reltarget;
	spath->param_info = get_baserel_parampathinfo(root, rel,
												  required_outer);
    spath->parallel_aware = parallel_workers > 0 ? true : false;
    spath->parallel_safe = rel->consider_parallel;
    spath->parallel_workers = parallel_workers;
    spath->pathkeys = NIL;

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
	cpath->custom_paths		= list_make1(spath);
	cpath->custom_private	= NIL;
	cpath->methods			= &ssdscan_path_methods;
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

	if (!enable_ssdscan)
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
 * PlanSSDScanPath
 */
static Plan *
PlanSSDScanPath(PlannerInfo *root,
				RelOptInfo *rel,
				struct CustomPath *best_path,
				List *tlist,
				List *clauses,
				List *custom_plans)
{
	CustomScan *cscan;
	SeqScan	   *sscan;
	Index		scan_relid = rel->relid;

	/* it should be a base rel... */
	Assert(scan_relid > 0);
	Assert(rel->rtekind == RTE_RELATION);
	Assert(list_length(custom_plans) == 1);
	sscan = linitial(custom_plans);

	/* make a custom scan node */
	cscan = makeNode(CustomScan);
	cscan->scan.plan.targetlist	= tlist;
	cscan->scan.plan.qual		= sscan->plan.qual;
	cscan->scan.plan.lefttree	= NULL;
	cscan->scan.plan.righttree	= NULL;
	cscan->scan.scanrelid		= scan_relid;
	cscan->flags				= best_path->flags;
	cscan->methods				= &ssdscan_plan_methods;

	return &cscan->scan.plan;
}

/*
 * CreateSSDScanState
 */
static Node *
CreateSSDScanState(CustomScan *cscan)
{
	return NULL;
}

/*
 * ExecInitSSDScan
 */
static void
ExecInitSSDScan(CustomScanState *node,
				EState *estate,
				int eflags)
{}

/*
 * ExecSSDScan
 */
static TupleTableSlot *
ExecSSDScan(CustomScanState *node)
{
	return NULL;
}

/*
 * EndCustomScan
 */
static void
ExecEndSSDScan(CustomScanState *node)
{}

/*
 * ReScanCustomScan
 */
static void
ExecReScanSSDScan(CustomScanState *node)
{}

/*
 * ExecSSDScanEstimateDSM
 */
static Size
ExecSSDScanEstimateDSM(CustomScanState *node,
					   ParallelContext *pcxt)
{
	return 0;
}

/*
 * ExecSSDScanInitDSM
 */
static void
ExecSSDScanInitDSM(CustomScanState *node,
				   ParallelContext *pcxt,
				   void *coordinate)
{}

/*
 * ExecSSDScanInitWorker
 */
static void
ExecSSDScanInitWorker(CustomScanState *node,
					  shm_toc *toc,
					  void *coordinate)
{}

/*
 * ExplainSSDScan
 */
static void
ExplainSSDScan(CustomScanState *node,
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
	DefineCustomBoolVariable("nvme_strom.enable_ssdscan",
							 "Enables direct SSD scan on NVMe-Strom",
							 NULL,
							 &enable_ssdscan,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);
	/* setup path methods */
	memset(&ssdscan_path_methods, 0, sizeof(CustomPathMethods));
	ssdscan_path_methods.CustomName			= "NVMeSSD Scan";
	ssdscan_path_methods.PlanCustomPath		= PlanSSDScanPath;

	/* setup plan methods */
	memset(&ssdscan_plan_methods, 0, sizeof(CustomScanMethods));
	ssdscan_plan_methods.CustomName			= "NVMeSSD Scan";
	ssdscan_plan_methods.CreateCustomScanState = CreateSSDScanState;
	RegisterCustomScanMethods(&ssdscan_plan_methods);

	/* setup exec methods */
	memset(&ssdscan_exec_methods, 0, sizeof(CustomExecMethods));
	ssdscan_exec_methods.CustomName			= "NVMeSSD Scan";
	ssdscan_exec_methods.BeginCustomScan	= ExecInitSSDScan;
	ssdscan_exec_methods.ExecCustomScan		= ExecSSDScan;
	ssdscan_exec_methods.EndCustomScan		= ExecEndSSDScan;
	ssdscan_exec_methods.ReScanCustomScan	= ExecReScanSSDScan;
	ssdscan_exec_methods.EstimateDSMCustomScan = ExecSSDScanEstimateDSM;
	ssdscan_exec_methods.InitializeDSMCustomScan = ExecSSDScanInitDSM;
	ssdscan_exec_methods.InitializeWorkerCustomScan = ExecSSDScanInitWorker;
	ssdscan_exec_methods.ExplainCustomScan	= ExplainSSDScan;

	/* hook registration */
	set_rel_pathlist_next = set_rel_pathlist_hook;
	set_rel_pathlist_hook = ssdscan_add_scan_path;
}
