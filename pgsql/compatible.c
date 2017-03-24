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




/*
 * get_restriction_qual_cost - optimizer/path/costsize.c
 */
static void
get_restriction_qual_cost(PlannerInfo *root, RelOptInfo *baserel,
						  ParamPathInfo *param_info,
						  QualCost *qpqual_cost)
{
	if (param_info)
	{
		/* Include costs of pushed-down clauses */
		cost_qual_eval(qpqual_cost, param_info->ppi_clauses, root);

		qpqual_cost->startup += baserel->baserestrictcost.startup;
		qpqual_cost->per_tuple += baserel->baserestrictcost.per_tuple;
	}
	else
		*qpqual_cost = baserel->baserestrictcost;
}

/*
 * get_parallel_divisor - from optimizer/path/costsize.c
 */
static double
get_parallel_divisor(Path *path)
{
	double      parallel_divisor = path->parallel_workers;
	double      leader_contribution;

	/*
	 * Early experience with parallel query suggests that when there is only
	 * one worker, the leader often makes a very substantial contribution to
	 * executing the parallel portion of the plan, but as more workers are
	 * added, it does less and less, because it's busy reading tuples from the
	 * workers and doing whatever non-parallel post-processing is needed.  By
	 * the time we reach 4 workers, the leader no longer makes a meaningful
	 * contribution.  Thus, for now, estimate that the leader spends 30% of
	 * its time servicing each worker, and the remainder executing the
	 * parallel plan.
	 */
	leader_contribution = 1.0 - (0.3 * path->parallel_workers);
	if (leader_contribution > 0)
		parallel_divisor += leader_contribution;

	return parallel_divisor;
}

#if PG_VERSION_NUM < 100000
/*
 * compute_parallel_worker - at optimizer/path/allpaths.c; since v10.0
 */
static int
compute_parallel_worker(RelOptInfo *rel,
						BlockNumber heap_pages,
						BlockNumber index_pages)
{
	int		min_parallel_table_scan_size = (8 * 1024 * 1024) / BLCKSZ;
	int		min_parallel_index_scan_size = (512 * 1024) / BLCKSZ;
	int		parallel_workers = 0;
	int		heap_parallel_workers = 1;
	int		index_parallel_workers = 1;

	/*
	 * If the user has set the parallel_workers reloption, use that; otherwise
	 * select a default number of workers.
	 */
	if (rel->rel_parallel_workers != -1)
		parallel_workers = rel->rel_parallel_workers;
	else
	{
		int		heap_parallel_threshold;
		int		index_parallel_threshold;

		/*
		 * If this relation is too small to be worth a parallel scan, just
		 * return without doing anything ... unless it's an inheritance child.
		 * In that case, we want to generate a parallel path here anyway.  It
		 * might not be worthwhile just for this relation, but when combined
		 * with all of its inheritance siblings it may well pay off.
		 */
		if (heap_pages < (BlockNumber)(min_parallel_table_scan_size) &&
			index_pages < (BlockNumber)(min_parallel_index_scan_size) &&
			rel->reloptkind == RELOPT_BASEREL)
			return 0;

		if (heap_pages > 0)
		{
			/*
			 * Select the number of workers based on the log of the size of
			 * the relation.  This probably needs to be a good deal more
			 * sophisticated, but we need something here for now.  Note that
			 * the upper limit of the min_parallel_table_scan_size GUC is
			 * chosen to prevent overflow here.
			 */
			heap_parallel_threshold = Max(min_parallel_table_scan_size, 1);
			while (heap_pages >= (BlockNumber) (heap_parallel_threshold * 3))
			{
				heap_parallel_workers++;
				heap_parallel_threshold *= 3;
				if (heap_parallel_threshold > INT_MAX / 3)
					break;      /* avoid overflow */
			}
			parallel_workers = heap_parallel_workers;
		}

		if (index_pages > 0)
		{
			/* same calculation as for heap_pages above */
			index_parallel_threshold = Max(min_parallel_index_scan_size, 1);
			while (index_pages >= (BlockNumber) (index_parallel_threshold * 3))
			{
				index_parallel_workers++;
				index_parallel_threshold *= 3;
				if (index_parallel_threshold > INT_MAX / 3)
					break;		/* avoid overflow */
			}

			if (parallel_workers > 0)
				parallel_workers = Min(parallel_workers,
									   index_parallel_workers);
			else
				parallel_workers = index_parallel_workers;
		}
	}

	/*
	 * In no case use more than max_parallel_workers_per_gather workers.
	 */
	parallel_workers = Min(parallel_workers, max_parallel_workers_per_gather);

	return parallel_workers;
}
#endif





