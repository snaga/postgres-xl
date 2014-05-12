/*-------------------------------------------------------------------------
 *
 * pathnode.c
 *	  Routines to manipulate pathlists and create path nodes
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/pathnode.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#ifdef XCP
#include "access/heapam.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "utils/rel.h"
#endif


typedef enum
{
	COSTS_EQUAL,				/* path costs are fuzzily equal */
	COSTS_BETTER1,				/* first path is cheaper than second */
	COSTS_BETTER2,				/* second path is cheaper than first */
	COSTS_DIFFERENT				/* neither path dominates the other on cost */
} PathCostComparison;

static void add_parameterized_path(RelOptInfo *parent_rel, Path *new_path);
static List *translate_sub_tlist(List *tlist, int relid);
static bool query_is_distinct_for(Query *query, List *colnos, List *opids);
static Oid	distinct_col_search(int colno, List *colnos, List *opids);
#ifdef XCP
static void restrict_distribution(PlannerInfo *root, RestrictInfo *ri,
								  Path *pathnode);
static Path *redistribute_path(Path *subpath, char distributionType,
				  Bitmapset *nodes, Bitmapset *restrictNodes,
				  Node* distributionExpr);
static void set_scanpath_distribution(PlannerInfo *root, RelOptInfo *rel, Path *pathnode);
static List *set_joinpath_distribution(PlannerInfo *root, JoinPath *pathnode);
#endif

/*****************************************************************************
 *		MISC. PATH UTILITIES
 *****************************************************************************/

/*
 * compare_path_costs
 *	  Return -1, 0, or +1 according as path1 is cheaper, the same cost,
 *	  or more expensive than path2 for the specified criterion.
 */
int
compare_path_costs(Path *path1, Path *path2, CostSelector criterion)
{
	if (criterion == STARTUP_COST)
	{
		if (path1->startup_cost < path2->startup_cost)
			return -1;
		if (path1->startup_cost > path2->startup_cost)
			return +1;

		/*
		 * If paths have the same startup cost (not at all unlikely), order
		 * them by total cost.
		 */
		if (path1->total_cost < path2->total_cost)
			return -1;
		if (path1->total_cost > path2->total_cost)
			return +1;
	}
	else
	{
		if (path1->total_cost < path2->total_cost)
			return -1;
		if (path1->total_cost > path2->total_cost)
			return +1;

		/*
		 * If paths have the same total cost, order them by startup cost.
		 */
		if (path1->startup_cost < path2->startup_cost)
			return -1;
		if (path1->startup_cost > path2->startup_cost)
			return +1;
	}
	return 0;
}

/*
 * compare_path_fractional_costs
 *	  Return -1, 0, or +1 according as path1 is cheaper, the same cost,
 *	  or more expensive than path2 for fetching the specified fraction
 *	  of the total tuples.
 *
 * If fraction is <= 0 or > 1, we interpret it as 1, ie, we select the
 * path with the cheaper total_cost.
 */
int
compare_fractional_path_costs(Path *path1, Path *path2,
							  double fraction)
{
	Cost		cost1,
				cost2;

	if (fraction <= 0.0 || fraction >= 1.0)
		return compare_path_costs(path1, path2, TOTAL_COST);
	cost1 = path1->startup_cost +
		fraction * (path1->total_cost - path1->startup_cost);
	cost2 = path2->startup_cost +
		fraction * (path2->total_cost - path2->startup_cost);
	if (cost1 < cost2)
		return -1;
	if (cost1 > cost2)
		return +1;
	return 0;
}

/*
 * compare_path_costs_fuzzily
 *	  Compare the costs of two paths to see if either can be said to
 *	  dominate the other.
 *
 * We use fuzzy comparisons so that add_path() can avoid keeping both of
 * a pair of paths that really have insignificantly different cost.
 *
 * The fuzz_factor argument must be 1.0 plus delta, where delta is the
 * fraction of the smaller cost that is considered to be a significant
 * difference.	For example, fuzz_factor = 1.01 makes the fuzziness limit
 * be 1% of the smaller cost.
 *
 * The two paths are said to have "equal" costs if both startup and total
 * costs are fuzzily the same.	Path1 is said to be better than path2 if
 * it has fuzzily better startup cost and fuzzily no worse total cost,
 * or if it has fuzzily better total cost and fuzzily no worse startup cost.
 * Path2 is better than path1 if the reverse holds.  Finally, if one path
 * is fuzzily better than the other on startup cost and fuzzily worse on
 * total cost, we just say that their costs are "different", since neither
 * dominates the other across the whole performance spectrum.
 */
static PathCostComparison
compare_path_costs_fuzzily(Path *path1, Path *path2, double fuzz_factor)
{
	/*
	 * Check total cost first since it's more likely to be different; many
	 * paths have zero startup cost.
	 */
	if (path1->total_cost > path2->total_cost * fuzz_factor)
	{
		/* path1 fuzzily worse on total cost */
		if (path2->startup_cost > path1->startup_cost * fuzz_factor)
		{
			/* ... but path2 fuzzily worse on startup, so DIFFERENT */
			return COSTS_DIFFERENT;
		}
		/* else path2 dominates */
		return COSTS_BETTER2;
	}
	if (path2->total_cost > path1->total_cost * fuzz_factor)
	{
		/* path2 fuzzily worse on total cost */
		if (path1->startup_cost > path2->startup_cost * fuzz_factor)
		{
			/* ... but path1 fuzzily worse on startup, so DIFFERENT */
			return COSTS_DIFFERENT;
		}
		/* else path1 dominates */
		return COSTS_BETTER1;
	}
	/* fuzzily the same on total cost */
	if (path1->startup_cost > path2->startup_cost * fuzz_factor)
	{
		/* ... but path1 fuzzily worse on startup, so path2 wins */
		return COSTS_BETTER2;
	}
	if (path2->startup_cost > path1->startup_cost * fuzz_factor)
	{
		/* ... but path2 fuzzily worse on startup, so path1 wins */
		return COSTS_BETTER1;
	}
	/* fuzzily the same on both costs */
	return COSTS_EQUAL;
}

/*
 * set_cheapest
 *	  Find the minimum-cost paths from among a relation's paths,
 *	  and save them in the rel's cheapest-path fields.
 *
 * Only unparameterized paths are considered candidates for cheapest_startup
 * and cheapest_total.	The cheapest_parameterized_paths list collects paths
 * that are cheapest-total for their parameterization (i.e., there is no
 * cheaper path with the same or weaker parameterization).	This list always
 * includes the unparameterized cheapest-total path, too.
 *
 * This is normally called only after we've finished constructing the path
 * list for the rel node.
 */
void
set_cheapest(RelOptInfo *parent_rel)
{
	Path	   *cheapest_startup_path;
	Path	   *cheapest_total_path;
	bool		have_parameterized_paths;
	ListCell   *p;

	Assert(IsA(parent_rel, RelOptInfo));

	cheapest_startup_path = cheapest_total_path = NULL;
	have_parameterized_paths = false;

	foreach(p, parent_rel->pathlist)
	{
		Path	   *path = (Path *) lfirst(p);
		int			cmp;

		/* We only consider unparameterized paths in this step */
		if (path->param_info)
		{
			have_parameterized_paths = true;
			continue;
		}

		if (cheapest_total_path == NULL)
		{
			cheapest_startup_path = cheapest_total_path = path;
			continue;
		}

		/*
		 * If we find two paths of identical costs, try to keep the
		 * better-sorted one.  The paths might have unrelated sort orderings,
		 * in which case we can only guess which might be better to keep, but
		 * if one is superior then we definitely should keep that one.
		 */
		cmp = compare_path_costs(cheapest_startup_path, path, STARTUP_COST);
		if (cmp > 0 ||
			(cmp == 0 &&
			 compare_pathkeys(cheapest_startup_path->pathkeys,
							  path->pathkeys) == PATHKEYS_BETTER2))
			cheapest_startup_path = path;

		cmp = compare_path_costs(cheapest_total_path, path, TOTAL_COST);
		if (cmp > 0 ||
			(cmp == 0 &&
			 compare_pathkeys(cheapest_total_path->pathkeys,
							  path->pathkeys) == PATHKEYS_BETTER2))
			cheapest_total_path = path;
	}

	if (cheapest_total_path == NULL)
		elog(ERROR, "could not devise a query plan for the given query");

	parent_rel->cheapest_startup_path = cheapest_startup_path;
	parent_rel->cheapest_total_path = cheapest_total_path;
	parent_rel->cheapest_unique_path = NULL;	/* computed only if needed */

	/* Seed the parameterized-paths list with the cheapest total */
	parent_rel->cheapest_parameterized_paths = list_make1(cheapest_total_path);

	/* And, if there are any parameterized paths, add them in one at a time */
	if (have_parameterized_paths)
	{
		foreach(p, parent_rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(p);

			if (path->param_info)
				add_parameterized_path(parent_rel, path);
		}
	}
}

/*
 * add_path
 *	  Consider a potential implementation path for the specified parent rel,
 *	  and add it to the rel's pathlist if it is worthy of consideration.
 *	  A path is worthy if it has a better sort order (better pathkeys) or
 *	  cheaper cost (on either dimension), or generates fewer rows, than any
 *	  existing path that has the same or superset parameterization rels.
 *
 *	  We also remove from the rel's pathlist any old paths that are dominated
 *	  by new_path --- that is, new_path is cheaper, at least as well ordered,
 *	  generates no more rows, and requires no outer rels not required by the
 *	  old path.
 *
 *	  In most cases, a path with a superset parameterization will generate
 *	  fewer rows (since it has more join clauses to apply), so that those two
 *	  figures of merit move in opposite directions; this means that a path of
 *	  one parameterization can seldom dominate a path of another.  But such
 *	  cases do arise, so we make the full set of checks anyway.
 *
 *	  There is one policy decision embedded in this function, along with its
 *	  sibling add_path_precheck: we treat all parameterized paths as having
 *	  NIL pathkeys, so that they compete only on cost.	This is to reduce
 *	  the number of parameterized paths that are kept.	See discussion in
 *	  src/backend/optimizer/README.
 *
 *	  The pathlist is kept sorted by total_cost, with cheaper paths
 *	  at the front.  Within this routine, that's simply a speed hack:
 *	  doing it that way makes it more likely that we will reject an inferior
 *	  path after a few comparisons, rather than many comparisons.
 *	  However, add_path_precheck relies on this ordering to exit early
 *	  when possible.
 *
 *	  NOTE: discarded Path objects are immediately pfree'd to reduce planner
 *	  memory consumption.  We dare not try to free the substructure of a Path,
 *	  since much of it may be shared with other Paths or the query tree itself;
 *	  but just recycling discarded Path nodes is a very useful savings in
 *	  a large join tree.  We can recycle the List nodes of pathlist, too.
 *
 *	  BUT: we do not pfree IndexPath objects, since they may be referenced as
 *	  children of BitmapHeapPaths as well as being paths in their own right.
 *
 * 'parent_rel' is the relation entry to which the path corresponds.
 * 'new_path' is a potential path for parent_rel.
 *
 * Returns nothing, but modifies parent_rel->pathlist.
 */
void
add_path(RelOptInfo *parent_rel, Path *new_path)
{
	bool		accept_new = true;		/* unless we find a superior old path */
	ListCell   *insert_after = NULL;	/* where to insert new item */
	List	   *new_path_pathkeys;
	ListCell   *p1;
	ListCell   *p1_prev;
	ListCell   *p1_next;

	/*
	 * This is a convenient place to check for query cancel --- no part of the
	 * planner goes very long without calling add_path().
	 */
	CHECK_FOR_INTERRUPTS();

	/* Pretend parameterized paths have no pathkeys, per comment above */
	new_path_pathkeys = new_path->param_info ? NIL : new_path->pathkeys;

	/*
	 * Loop to check proposed new path against old paths.  Note it is possible
	 * for more than one old path to be tossed out because new_path dominates
	 * it.
	 *
	 * We can't use foreach here because the loop body may delete the current
	 * list cell.
	 */
	p1_prev = NULL;
	for (p1 = list_head(parent_rel->pathlist); p1 != NULL; p1 = p1_next)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		bool		remove_old = false; /* unless new proves superior */
		PathCostComparison costcmp;
		PathKeysComparison keyscmp;
		BMS_Comparison outercmp;

		p1_next = lnext(p1);

		/*
		 * Do a fuzzy cost comparison with 1% fuzziness limit.	(XXX does this
		 * percentage need to be user-configurable?)
		 */
		costcmp = compare_path_costs_fuzzily(new_path, old_path, 1.01);

		/*
		 * If the two paths compare differently for startup and total cost,
		 * then we want to keep both, and we can skip comparing pathkeys and
		 * required_outer rels.  If they compare the same, proceed with the
		 * other comparisons.  Row count is checked last.  (We make the tests
		 * in this order because the cost comparison is most likely to turn
		 * out "different", and the pathkeys comparison next most likely.  As
		 * explained above, row count very seldom makes a difference, so even
		 * though it's cheap to compare there's not much point in checking it
		 * earlier.)
		 */
		if (costcmp != COSTS_DIFFERENT)
		{
			/* Similarly check to see if either dominates on pathkeys */
			List	   *old_path_pathkeys;

			old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
			keyscmp = compare_pathkeys(new_path_pathkeys,
									   old_path_pathkeys);
			if (keyscmp != PATHKEYS_DIFFERENT)
			{
				switch (costcmp)
				{
					case COSTS_EQUAL:
						outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
												   PATH_REQ_OUTER(old_path));
						if (keyscmp == PATHKEYS_BETTER1)
						{
							if ((outercmp == BMS_EQUAL ||
								 outercmp == BMS_SUBSET1) &&
								new_path->rows <= old_path->rows)
								remove_old = true;		/* new dominates old */
						}
						else if (keyscmp == PATHKEYS_BETTER2)
						{
							if ((outercmp == BMS_EQUAL ||
								 outercmp == BMS_SUBSET2) &&
								new_path->rows >= old_path->rows)
								accept_new = false;		/* old dominates new */
						}
						else	/* keyscmp == PATHKEYS_EQUAL */
						{
							if (outercmp == BMS_EQUAL)
							{
								/*
								 * Same pathkeys and outer rels, and fuzzily
								 * the same cost, so keep just one; to decide
								 * which, first check rows and then do a fuzzy
								 * cost comparison with very small fuzz limit.
								 * (We used to do an exact cost comparison,
								 * but that results in annoying
								 * platform-specific plan variations due to
								 * roundoff in the cost estimates.)  If things
								 * are still tied, arbitrarily keep only the
								 * old path.  Notice that we will keep only
								 * the old path even if the less-fuzzy
								 * comparison decides the startup and total
								 * costs compare differently.
								 */
								if (new_path->rows < old_path->rows)
									remove_old = true;	/* new dominates old */
								else if (new_path->rows > old_path->rows)
									accept_new = false; /* old dominates new */
								else if (compare_path_costs_fuzzily(new_path, old_path,
											  1.0000000001) == COSTS_BETTER1)
									remove_old = true;	/* new dominates old */
								else
									accept_new = false; /* old equals or
														 * dominates new */
							}
							else if (outercmp == BMS_SUBSET1 &&
									 new_path->rows <= old_path->rows)
								remove_old = true;		/* new dominates old */
							else if (outercmp == BMS_SUBSET2 &&
									 new_path->rows >= old_path->rows)
								accept_new = false;		/* old dominates new */
							/* else different parameterizations, keep both */
						}
						break;
					case COSTS_BETTER1:
						if (keyscmp != PATHKEYS_BETTER2)
						{
							outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
												   PATH_REQ_OUTER(old_path));
							if ((outercmp == BMS_EQUAL ||
								 outercmp == BMS_SUBSET1) &&
								new_path->rows <= old_path->rows)
								remove_old = true;		/* new dominates old */
						}
						break;
					case COSTS_BETTER2:
						if (keyscmp != PATHKEYS_BETTER1)
						{
							outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
												   PATH_REQ_OUTER(old_path));
							if ((outercmp == BMS_EQUAL ||
								 outercmp == BMS_SUBSET2) &&
								new_path->rows >= old_path->rows)
								accept_new = false;		/* old dominates new */
						}
						break;
					case COSTS_DIFFERENT:

						/*
						 * can't get here, but keep this case to keep compiler
						 * quiet
						 */
						break;
				}
			}
		}

		/*
		 * Remove current element from pathlist if dominated by new.
		 */
		if (remove_old)
		{
			parent_rel->pathlist = list_delete_cell(parent_rel->pathlist,
													p1, p1_prev);

			/*
			 * Delete the data pointed-to by the deleted cell, if possible
			 */
			if (!IsA(old_path, IndexPath))
				pfree(old_path);
			/* p1_prev does not advance */
		}
		else
		{
			/* new belongs after this old path if it has cost >= old's */
			if (new_path->total_cost >= old_path->total_cost)
				insert_after = p1;
			/* p1_prev advances */
			p1_prev = p1;
		}

		/*
		 * If we found an old path that dominates new_path, we can quit
		 * scanning the pathlist; we will not add new_path, and we assume
		 * new_path cannot dominate any other elements of the pathlist.
		 */
		if (!accept_new)
			break;
	}

	if (accept_new)
	{
		/* Accept the new path: insert it at proper place in pathlist */
		if (insert_after)
			lappend_cell(parent_rel->pathlist, insert_after, new_path);
		else
			parent_rel->pathlist = lcons(new_path, parent_rel->pathlist);
	}
	else
	{
		/* Reject and recycle the new path */
		if (!IsA(new_path, IndexPath))
			pfree(new_path);
	}
}

/*
 * add_path_precheck
 *	  Check whether a proposed new path could possibly get accepted.
 *	  We assume we know the path's pathkeys and parameterization accurately,
 *	  and have lower bounds for its costs.
 *
 * Note that we do not know the path's rowcount, since getting an estimate for
 * that is too expensive to do before prechecking.	We assume here that paths
 * of a superset parameterization will generate fewer rows; if that holds,
 * then paths with different parameterizations cannot dominate each other
 * and so we can simply ignore existing paths of another parameterization.
 * (In the infrequent cases where that rule of thumb fails, add_path will
 * get rid of the inferior path.)
 *
 * At the time this is called, we haven't actually built a Path structure,
 * so the required information has to be passed piecemeal.
 */
bool
add_path_precheck(RelOptInfo *parent_rel,
				  Cost startup_cost, Cost total_cost,
				  List *pathkeys, Relids required_outer)
{
	List	   *new_path_pathkeys;
	ListCell   *p1;

	/* Pretend parameterized paths have no pathkeys, per add_path comment */
	new_path_pathkeys = required_outer ? NIL : pathkeys;

	foreach(p1, parent_rel->pathlist)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		PathKeysComparison keyscmp;

		/*
		 * We are looking for an old_path with the same parameterization (and
		 * by assumption the same rowcount) that dominates the new path on
		 * pathkeys as well as both cost metrics.  If we find one, we can
		 * reject the new path.
		 *
		 * For speed, we make exact rather than fuzzy cost comparisons. If an
		 * old path dominates the new path exactly on both costs, it will
		 * surely do so fuzzily.
		 */
		if (total_cost >= old_path->total_cost)
		{
			if (startup_cost >= old_path->startup_cost)
			{
				List	   *old_path_pathkeys;

				old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
				keyscmp = compare_pathkeys(new_path_pathkeys,
										   old_path_pathkeys);
				if (keyscmp == PATHKEYS_EQUAL ||
					keyscmp == PATHKEYS_BETTER2)
				{
					if (bms_equal(required_outer, PATH_REQ_OUTER(old_path)))
					{
						/* Found an old path that dominates the new one */
						return false;
					}
				}
			}
		}
		else
		{
			/*
			 * Since the pathlist is sorted by total_cost, we can stop looking
			 * once we reach a path with a total_cost larger than the new
			 * path's.
			 */
			break;
		}
	}

	return true;
}

/*
 * add_parameterized_path
 *	  Consider a parameterized implementation path for the specified rel,
 *	  and add it to the rel's cheapest_parameterized_paths list if it
 *	  belongs there, removing any old entries that it dominates.
 *
 *	  This is essentially a cut-down form of add_path(): we do not care
 *	  about startup cost or sort ordering, only total cost, rowcount, and
 *	  parameterization.  Also, we must not recycle rejected paths, since
 *	  they will still be present in the rel's pathlist.
 *
 * 'parent_rel' is the relation entry to which the path corresponds.
 * 'new_path' is a parameterized path for parent_rel.
 *
 * Returns nothing, but modifies parent_rel->cheapest_parameterized_paths.
 */
static void
add_parameterized_path(RelOptInfo *parent_rel, Path *new_path)
{
	bool		accept_new = true;		/* unless we find a superior old path */
	ListCell   *insert_after = NULL;	/* where to insert new item */
	ListCell   *p1;
	ListCell   *p1_prev;
	ListCell   *p1_next;

	/*
	 * Loop to check proposed new path against old paths.  Note it is possible
	 * for more than one old path to be tossed out because new_path dominates
	 * it.
	 *
	 * We can't use foreach here because the loop body may delete the current
	 * list cell.
	 */
	p1_prev = NULL;
	for (p1 = list_head(parent_rel->cheapest_parameterized_paths);
		 p1 != NULL; p1 = p1_next)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		bool		remove_old = false; /* unless new proves superior */
		int			costcmp;
		BMS_Comparison outercmp;

		p1_next = lnext(p1);

		costcmp = compare_path_costs(new_path, old_path, TOTAL_COST);
		outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
									  PATH_REQ_OUTER(old_path));
		if (outercmp != BMS_DIFFERENT)
		{
			if (costcmp < 0)
			{
				if (outercmp != BMS_SUBSET2 &&
					new_path->rows <= old_path->rows)
					remove_old = true;	/* new dominates old */
			}
			else if (costcmp > 0)
			{
				if (outercmp != BMS_SUBSET1 &&
					new_path->rows >= old_path->rows)
					accept_new = false; /* old dominates new */
			}
			else if (outercmp == BMS_SUBSET1 &&
					 new_path->rows <= old_path->rows)
				remove_old = true;		/* new dominates old */
			else if (outercmp == BMS_SUBSET2 &&
					 new_path->rows >= old_path->rows)
				accept_new = false;		/* old dominates new */
			else if (new_path->rows < old_path->rows)
				remove_old = true;		/* new dominates old */
			else
			{
				/* Same cost, rows, and param rels; arbitrarily keep old */
				accept_new = false;		/* old equals or dominates new */
			}
		}

		/*
		 * Remove current element from cheapest_parameterized_paths if
		 * dominated by new.
		 */
		if (remove_old)
		{
			parent_rel->cheapest_parameterized_paths =
				list_delete_cell(parent_rel->cheapest_parameterized_paths,
								 p1, p1_prev);
			/* p1_prev does not advance */
		}
		else
		{
			/* new belongs after this old path if it has cost >= old's */
			if (costcmp >= 0)
				insert_after = p1;
			/* p1_prev advances */
			p1_prev = p1;
		}

		/*
		 * If we found an old path that dominates new_path, we can quit
		 * scanning the list; we will not add new_path, and we assume new_path
		 * cannot dominate any other elements of the list.
		 */
		if (!accept_new)
			break;
	}

	if (accept_new)
	{
		/* Accept the new path: insert it at proper place in list */
		if (insert_after)
			lappend_cell(parent_rel->cheapest_parameterized_paths,
						 insert_after, new_path);
		else
			parent_rel->cheapest_parameterized_paths =
				lcons(new_path, parent_rel->cheapest_parameterized_paths);
	}
}


/*****************************************************************************
 *		PATH NODE CREATION ROUTINES
 *****************************************************************************/
#ifdef XCP
/*
 * restrict_distribution
 *    Analyze the RestrictInfo and decide if it is possible to restrict
 *    distribution nodes
 */
static void
restrict_distribution(PlannerInfo *root, RestrictInfo *ri,
								  Path *pathnode)
{
	Distribution   *distribution = pathnode->distribution;
	Oid				keytype;
	Const		   *constExpr = NULL;
	bool			found_key = false;

	/*
	 * Can not restrict - not distributed or key is not defined
	 */
	if (distribution == NULL ||
			distribution->distributionExpr == NULL)
		return;

	/*
	 * We do not support OR'ed conditions yet
	 */
	if (ri->orclause)
		return;

	keytype = exprType(distribution->distributionExpr);
	if (ri->left_ec)
	{
		EquivalenceClass *ec = ri->left_ec;
		ListCell *lc;
		foreach(lc, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
			if (equal(em->em_expr, distribution->distributionExpr))
				found_key = true;
			else if (bms_is_empty(em->em_relids))
			{
				Expr *cexpr = (Expr *) eval_const_expressions(root,
													   (Node *) em->em_expr);
				if (IsA(cexpr, Const) &&
						((Const *) cexpr)->consttype == keytype)
					constExpr = (Const *) cexpr;
			}
		}
	}
	if (ri->right_ec)
	{
		EquivalenceClass *ec = ri->right_ec;
		ListCell *lc;
		foreach(lc, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
			if (equal(em->em_expr, distribution->distributionExpr))
				found_key = true;
			else if (bms_is_empty(em->em_relids))
			{
				Expr *cexpr = (Expr *) eval_const_expressions(root,
													   (Node *) em->em_expr);
				if (IsA(cexpr, Const) &&
						((Const *) cexpr)->consttype == keytype)
					constExpr = (Const *) cexpr;
			}
		}
	}
	if (IsA(ri->clause, OpExpr))
	{
		OpExpr *opexpr = (OpExpr *) ri->clause;
		if (opexpr->args->length == 2 &&
				op_mergejoinable(opexpr->opno, exprType(linitial(opexpr->args))))
		{
			Expr *arg1 = (Expr *) linitial(opexpr->args);
			Expr *arg2 = (Expr *) lsecond(opexpr->args);
			Expr *other = NULL;
			if (equal(arg1, distribution->distributionExpr))
				other = arg2;
			else if (equal(arg2, distribution->distributionExpr))
				other = arg1;
			if (other)
			{
				found_key = true;
				other = (Expr *) eval_const_expressions(root, (Node *) other);
				if (IsA(other, Const) &&
						((Const *) other)->consttype == keytype)
					constExpr = (Const *) other;
			}
		}
	}
	if (found_key && constExpr)
	{
		List 	   *nodeList = NIL;
		Bitmapset  *tmpset = bms_copy(distribution->nodes);
		Bitmapset  *restrictinfo = NULL;
		Locator    *locator;
		int		   *nodenums;
		int 		i, count;

		while((i = bms_first_member(tmpset)) >= 0)
			nodeList = lappend_int(nodeList, i);
		bms_free(tmpset);

		locator = createLocator(distribution->distributionType,
								RELATION_ACCESS_READ,
								keytype,
								LOCATOR_LIST_LIST,
								0,
								(void *) nodeList,
								(void **) &nodenums,
								false);
		count = GET_NODES(locator, constExpr->constvalue,
						  constExpr->constisnull, NULL);

		for (i = 0; i < count; i++)
			restrictinfo = bms_add_member(restrictinfo, nodenums[i]);
		if (distribution->restrictNodes)
			distribution->restrictNodes = bms_intersect(distribution->restrictNodes,
														restrictinfo);
		else
			distribution->restrictNodes = restrictinfo;
		list_free(nodeList);
		freeLocator(locator);
	}
}

/*
 * set_scanpath_distribution
 *	  Assign distribution to the path which is a base relation scan.
 */
static void
set_scanpath_distribution(PlannerInfo *root, RelOptInfo *rel, Path *pathnode)
{
	RangeTblEntry   *rte;
	RelationLocInfo *rel_loc_info;

	rte = planner_rt_fetch(rel->relid, root);
	rel_loc_info = GetRelationLocInfo(rte->relid);
	if (rel_loc_info)
	{
		ListCell *lc;
		Distribution *distribution = makeNode(Distribution);
		distribution->distributionType = rel_loc_info->locatorType;
		foreach(lc, rel_loc_info->nodeList)
			distribution->nodes = bms_add_member(distribution->nodes,
												 lfirst_int(lc));
		distribution->restrictNodes = NULL;
		/*
		 * Distribution expression of the base relation is Var representing
		 * respective attribute.
		 */
		distribution->distributionExpr = NULL;
		if (rel_loc_info->partAttrNum)
		{
			Var 	   *var = NULL;
			ListCell   *lc;

			/* Look if the Var is already in the target list */
			foreach (lc, rel->reltargetlist)
			{
				var = (Var *) lfirst(lc);
				if (IsA(var, Var) && var->varno == rel->relid &&
						var->varattno == rel_loc_info->partAttrNum)
					break;
			}
			/* If not found we should look up the attribute and make the Var */
			if (!lc)
			{
				Relation 	relation = heap_open(rte->relid, NoLock);
				TupleDesc	tdesc = RelationGetDescr(relation);
				Form_pg_attribute att_tup;

				att_tup = tdesc->attrs[rel_loc_info->partAttrNum - 1];
				var = makeVar(rel->relid, rel_loc_info->partAttrNum,
							  att_tup->atttypid, att_tup->atttypmod,
							  att_tup->attcollation, 0);


				heap_close(relation, NoLock);
			}

			distribution->distributionExpr = (Node *) var;
		}
		pathnode->distribution = distribution;
	}
}


/*
 * Set a RemoteSubPath on top of the specified node and set specified
 * distribution to it
 */
static Path *
redistribute_path(Path *subpath, char distributionType,
				  Bitmapset *nodes, Bitmapset *restrictNodes,
				  Node* distributionExpr)
{
	Distribution   *distribution = NULL;
	RelOptInfo	   *rel = subpath->parent;
	RemoteSubPath  *pathnode;

 	if (distributionType != LOCATOR_TYPE_NONE)
	{
		distribution = makeNode(Distribution);
		distribution->distributionType = distributionType;
		distribution->nodes = nodes;
		distribution->restrictNodes = restrictNodes;
		distribution->distributionExpr = distributionExpr;
	}

	/*
	 * If inner path node is a MaterialPath pull it up to store tuples on
	 * the destination nodes and avoid sending them over the network.
	 */
	if (IsA(subpath, MaterialPath))
	{
		MaterialPath *mpath = (MaterialPath *) subpath;
		/* If subpath is already a RemoteSubPath, just replace distribution */
		if (IsA(mpath->subpath, RemoteSubPath))
		{
			pathnode = (RemoteSubPath *) mpath->subpath;
		}
		else
		{
			pathnode = makeNode(RemoteSubPath);
			pathnode->path.pathtype = T_RemoteSubplan;
			pathnode->path.parent = rel;
			pathnode->path.param_info = subpath->param_info;
			pathnode->path.pathkeys = subpath->pathkeys;
			pathnode->subpath = mpath->subpath;
			mpath->subpath = (Path *) pathnode;
		}
		subpath = pathnode->subpath;
		pathnode->path.distribution = distribution;
		mpath->path.distribution = (Distribution *) copyObject(distribution);
		/* (re)calculate costs */
		cost_remote_subplan((Path *) pathnode, subpath->startup_cost,
							subpath->total_cost, subpath->rows, rel->width,
							IsLocatorReplicated(distributionType) ?
									bms_num_members(nodes) : 1);
		mpath->subpath = (Path *) pathnode;
		cost_material(&mpath->path,
					  pathnode->path.startup_cost,
					  pathnode->path.total_cost,
					  pathnode->path.rows,
					  rel->width);
		return (Path *) mpath;
	}
	else
	{
		pathnode = makeNode(RemoteSubPath);
		pathnode->path.pathtype = T_RemoteSubplan;
		pathnode->path.parent = rel;
		pathnode->path.param_info = subpath->param_info;
		pathnode->path.pathkeys = subpath->pathkeys;
		pathnode->subpath = subpath;
		pathnode->path.distribution = distribution;
		cost_remote_subplan((Path *) pathnode, subpath->startup_cost,
							subpath->total_cost, subpath->rows, rel->width,
							IsLocatorReplicated(distributionType) ?
									bms_num_members(nodes) : 1);
		return (Path *) pathnode;
	}
}


static JoinPath *
flatCopyJoinPath(JoinPath *pathnode)
{
	JoinPath   *newnode;
	size_t 		size = 0;
	switch(nodeTag(pathnode))
	{
		case T_NestPath:
			size = sizeof(NestPath);
			break;
		case T_MergePath:
			size = sizeof(MergePath);
			break;
		case T_HashPath:
			size = sizeof(HashPath);
			break;
		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(pathnode));
			break;
	}
	newnode = (JoinPath *) palloc(size);
	memcpy(newnode, pathnode, size);
	return newnode;
}


/*
 * Analyze join parameters and set distribution of the join node.
 * If there are possible alternate distributions the respective pathes are
 * returned as a list so caller can cost all of them and choose cheapest to
 * continue.
 */
static List *
set_joinpath_distribution(PlannerInfo *root, JoinPath *pathnode)
{
	Distribution   *innerd = pathnode->innerjoinpath->distribution;
	Distribution   *outerd = pathnode->outerjoinpath->distribution;
	Distribution   *targetd;
	List		   *alternate = NIL;

	/* Catalog join */
	if (innerd == NULL && outerd == NULL)
		return NIL;

	/*
	 * If both subpaths are distributed by replication, the resulting
	 * distribution will be replicated on smallest common set of nodes.
	 * Catalog tables are the same on all nodes, so treat them as replicated
	 * on all nodes.
	 */
	if ((!innerd || IsLocatorReplicated(innerd->distributionType)) &&
		(!outerd || IsLocatorReplicated(outerd->distributionType)))
	{
		/* Determine common nodes */
		Bitmapset *common;

		if (innerd == NULL)
			common = bms_copy(outerd->nodes);
		else if (outerd == NULL)
			common = bms_copy(innerd->nodes);
		else
			common = bms_intersect(innerd->nodes, outerd->nodes);
		if (bms_is_empty(common))
			goto not_allowed_join;

		/*
		 * Join result is replicated on common nodes. Running query on any
		 * of them produce correct result.
		 */
		targetd = makeNode(Distribution);
		targetd->distributionType = LOCATOR_TYPE_REPLICATED;
		targetd->nodes = common;
		targetd->restrictNodes = NULL;
		pathnode->path.distribution = targetd;
		return alternate;
	}

	/*
	 * Check if we have inner replicated
	 * The "both replicated" case is already checked, so if innerd
	 * is replicated, then outerd is not replicated and it is not NULL.
	 * This case is not acceptable for some join types. If outer relation is
	 * nullable data nodes will produce joined rows with NULLs for cases when
	 * matching row exists, but on other data node.
	 */
	if ((!innerd || IsLocatorReplicated(innerd->distributionType)) &&
			(pathnode->jointype == JOIN_INNER ||
			 pathnode->jointype == JOIN_LEFT ||
			 pathnode->jointype == JOIN_SEMI ||
			 pathnode->jointype == JOIN_ANTI))
	{
		/* We need inner relation is defined on all nodes where outer is */
		if (innerd && !bms_is_subset(outerd->nodes, innerd->nodes))
			goto not_allowed_join;

		targetd = makeNode(Distribution);
		targetd->distributionType = outerd->distributionType;
		targetd->nodes = bms_copy(outerd->nodes);
		targetd->restrictNodes = bms_copy(outerd->restrictNodes);
		targetd->distributionExpr = outerd->distributionExpr;
		pathnode->path.distribution = targetd;
		return alternate;
	}


	/*
	 * Check if we have outer replicated
	 * The "both replicated" case is already checked, so if outerd
	 * is replicated, then innerd is not replicated and it is not NULL.
	 * This case is not acceptable for some join types. If inner relation is
	 * nullable data nodes will produce joined rows with NULLs for cases when
	 * matching row exists, but on other data node.
	 */
	if ((!outerd || IsLocatorReplicated(outerd->distributionType)) &&
			(pathnode->jointype == JOIN_INNER ||
			 pathnode->jointype == JOIN_RIGHT))
	{
		/* We need outer relation is defined on all nodes where inner is */
		if (outerd && !bms_is_subset(innerd->nodes, outerd->nodes))
			goto not_allowed_join;

		targetd = makeNode(Distribution);
		targetd->distributionType = innerd->distributionType;
		targetd->nodes = bms_copy(innerd->nodes);
		targetd->restrictNodes = bms_copy(innerd->restrictNodes);
		targetd->distributionExpr = innerd->distributionExpr;
		pathnode->path.distribution = targetd;
		return alternate;
	}


	/*
	 * This join is still allowed if inner and outer paths have
	 * equivalent distribution and joined along the distribution keys.
	 */
	if (innerd && outerd &&
			innerd->distributionType == outerd->distributionType &&
			innerd->distributionExpr &&
			outerd->distributionExpr &&
			bms_equal(innerd->nodes, outerd->nodes))
	{
		ListCell   *lc;

		/*
		 * Make sure distribution functions are the same, for now they depend
		 * on data type
		 */
		if (exprType((Node *) innerd->distributionExpr) != exprType((Node *) outerd->distributionExpr))
			goto not_allowed_join;

		/*
		 * Planner already did necessary work and if there is a join
		 * condition like left.key=right.key the key expressions
		 * will be members of the same equivalence class, and both
		 * sides of the corresponding RestrictInfo will refer that
		 * Equivalence Class.
		 * Try to figure out if such restriction exists.
		 */
		foreach(lc, pathnode->joinrestrictinfo)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
			ListCell   *emc;
			bool		found_outer, found_inner;

			/*
			 * Restriction operator is not equality operator ?
			 */
			if (ri->left_ec == NULL || ri->right_ec == NULL)
				continue;

			/*
			 * A restriction with OR may be compatible if all OR'ed
			 * conditions are compatible. For the moment we do not
			 * check this and skip restriction. The case if multiple
			 * OR'ed conditions are compatible is rare and probably
			 * do not worth doing at all.
			 */
			if (ri->orclause)
				continue;

			found_outer = false;
			found_inner = false;

			/*
			 * If parts belong to the same equivalence member check
			 * if both distribution keys are members of the class.
			 */
			if (ri->left_ec == ri->right_ec)
			{
				foreach(emc, ri->left_ec->ec_members)
				{
					EquivalenceMember *em = (EquivalenceMember *) lfirst(emc);
					Expr	   *var = (Expr *)em->em_expr;
					if (!found_outer)
						found_outer = equal(var, outerd->distributionExpr);

					if (!found_inner)
						found_inner = equal(var, innerd->distributionExpr);
				}
				if (found_outer && found_inner)
				{
					ListCell *tlc, *emc;

					targetd = makeNode(Distribution);
					targetd->distributionType = innerd->distributionType;
					targetd->nodes = bms_copy(innerd->nodes);
					targetd->restrictNodes = bms_copy(innerd->restrictNodes);
					targetd->distributionExpr = NULL;
					pathnode->path.distribution = targetd;

					/*
					 * Each member of the equivalence class may be a
					 * distribution expression, but we prefer some from the
					 * target list.
					 */
					foreach(tlc, pathnode->path.parent->reltargetlist)
					{
						Expr *var = (Expr *) lfirst(tlc);
						foreach(emc, ri->left_ec->ec_members)
						{
							EquivalenceMember *em;
							Expr *emvar;

							em = (EquivalenceMember *) lfirst(emc);
							emvar = (Expr *)em->em_expr;
							if (equal(var, emvar))
							{
								targetd->distributionExpr = (Node *) var;
								return alternate;
							}
						}
					}
					/* Not found, take any */
					targetd->distributionExpr = innerd->distributionExpr;
					return alternate;
				}
			}
			/*
			 * Check clause, if both arguments are distribution keys and
			 * operator is an equality operator
			 */
			else
			{
				OpExpr *op_exp;
				Expr   *arg1,
					   *arg2;

				op_exp = (OpExpr *) ri->clause;
				if (!IsA(op_exp, OpExpr) || list_length(op_exp->args) != 2)
					continue;

				arg1 = (Expr *) linitial(op_exp->args);
				arg2 = (Expr *) lsecond(op_exp->args);

				found_outer = equal(arg1, outerd->distributionExpr) || equal(arg2, outerd->distributionExpr);
				found_inner = equal(arg1, innerd->distributionExpr) || equal(arg2, innerd->distributionExpr);

				if (found_outer && found_inner)
				{
					targetd = makeNode(Distribution);
					targetd->distributionType = innerd->distributionType;
					targetd->nodes = bms_copy(innerd->nodes);
					targetd->restrictNodes = bms_copy(innerd->restrictNodes);
					pathnode->path.distribution = targetd;

					/*
					 * In case of outer join distribution key should not refer
					 * distribution key of nullable part.
					 */
					if (pathnode->jointype == JOIN_FULL)
						/* both parts are nullable */
						targetd->distributionExpr = NULL;
					else if (pathnode->jointype == JOIN_RIGHT)
						targetd->distributionExpr = innerd->distributionExpr;
					else
						targetd->distributionExpr = outerd->distributionExpr;

					return alternate;
				}
			}
		}
	}

	/*
	 * If we could not determine the distribution redistribute the subpathes.
	 */
not_allowed_join:
	/*
	 * If redistribution is required, sometimes the cheapest path would be if
	 * one of the subplan is replicated. If replication of any or all subplans
	 * is possible, return resulting plans as alternates. Try to distribute all
	 * by has as main variant.
	 */

	/* These join types allow replicated inner */
	if (outerd &&
			(pathnode->jointype == JOIN_INNER ||
			 pathnode->jointype == JOIN_LEFT ||
			 pathnode->jointype == JOIN_SEMI ||
			 pathnode->jointype == JOIN_ANTI))
	{
		/*
		 * Since we discard all alternate pathes except one it is OK if all they
		 * reference the same objects
		 */
		JoinPath *altpath = flatCopyJoinPath(pathnode);
		/* Redistribute inner subquery */
		altpath->innerjoinpath = redistribute_path(
				altpath->innerjoinpath,
				LOCATOR_TYPE_REPLICATED,
				bms_copy(outerd->nodes),
				bms_copy(outerd->restrictNodes),
				NULL);
		targetd = makeNode(Distribution);
		targetd->distributionType = outerd->distributionType;
		targetd->nodes = bms_copy(outerd->nodes);
		targetd->restrictNodes = bms_copy(outerd->restrictNodes);
		targetd->distributionExpr = outerd->distributionExpr;
		altpath->path.distribution = targetd;
		alternate = lappend(alternate, altpath);
	}

	/* These join types allow replicated outer */
	if (innerd &&
			(pathnode->jointype == JOIN_INNER ||
			 pathnode->jointype == JOIN_RIGHT))
	{
		/*
		 * Since we discard all alternate pathes except one it is OK if all they
		 * reference the same objects
		 */
		JoinPath *altpath = flatCopyJoinPath(pathnode);
		/* Redistribute inner subquery */
		altpath->outerjoinpath = redistribute_path(
				altpath->outerjoinpath,
				LOCATOR_TYPE_REPLICATED,
				bms_copy(innerd->nodes),
				bms_copy(innerd->restrictNodes),
				NULL);
		targetd = makeNode(Distribution);
		targetd->distributionType = innerd->distributionType;
		targetd->nodes = bms_copy(innerd->nodes);
		targetd->restrictNodes = bms_copy(innerd->restrictNodes);
		targetd->distributionExpr = innerd->distributionExpr;
		altpath->path.distribution = targetd;
		alternate = lappend(alternate, altpath);
	}

	/*
	 * Redistribute subplans to make them compatible.
	 * If any of the subplans is a coordinator subplan skip this stuff and do
	 * coordinator join.
	 */
	if (innerd && outerd)
	{
		RestrictInfo   *preferred = NULL;
		Expr		   *new_inner_key = NULL;
		Expr		   *new_outer_key = NULL;
		char			distType = LOCATOR_TYPE_NONE;
		ListCell 	   *lc;

		/*
		 * Look through the join restrictions to find one that is a hashable
		 * operator on two arguments. Choose best restriction acoording to
		 * following criteria:
		 * 1. one argument is already a partitioning key of one subplan.
		 * 2. restriction is cheaper to calculate
		 */
		foreach(lc, pathnode->joinrestrictinfo)
		{
			RestrictInfo   *ri = (RestrictInfo *) lfirst(lc);

			/* can not handle ORed conditions */
			if (ri->orclause)
				continue;

			if (IsA(ri->clause, OpExpr))
			{
				OpExpr *expr = (OpExpr *) ri->clause;
				if (list_length(expr->args) == 2 &&
						op_hashjoinable(expr->opno, exprType(linitial(expr->args))))
				{
					Expr *left = (Expr *) linitial(expr->args);
					Expr *right = (Expr *) lsecond(expr->args);
					Oid leftType = exprType((Node *) left);
					Oid rightType = exprType((Node *) right);
					Relids inner_rels = pathnode->innerjoinpath->parent->relids;
					Relids outer_rels = pathnode->outerjoinpath->parent->relids;
					QualCost cost;

					/*
					 * Check if both parts are of the same data type and choose
					 * distribution type to redistribute.
					 * XXX We may want more sophisticated algorithm to choose
					 * the best condition to redistribute parts along.
					 * For now use simple but reliable approach.
					 */
					if (leftType != rightType)
						continue;
					/*
					 * Evaluation cost will be needed to choose preferred
					 * distribution
					 */
					cost_qual_eval_node(&cost, (Node *) ri, root);

					if (outerd->distributionExpr)
					{
						/*
						 * If left side is distribution key of outer subquery
						 * and right expression refers only inner subquery
						 */
						if (equal(outerd->distributionExpr, left) &&
								bms_is_subset(ri->right_relids, inner_rels))
						{
							if (!preferred || /* no preferred restriction yet found */
									(new_inner_key && new_outer_key) || /* preferred restriction require redistribution of both parts */
									(cost.per_tuple < preferred->eval_cost.per_tuple)) /* current restriction is cheaper */
							{
								/* set new preferred restriction */
								preferred = ri;
								new_inner_key = right;
								new_outer_key = NULL; /* no need to change */
								distType = outerd->distributionType;
							}
							continue;
						}
						/*
						 * If right side is distribution key of outer subquery
						 * and left expression refers only inner subquery
						 */
						if (equal(outerd->distributionExpr, right) &&
								bms_is_subset(ri->left_relids, inner_rels))
						{
							if (!preferred || /* no preferred restriction yet found */
									(new_inner_key && new_outer_key) || /* preferred restriction require redistribution of both parts */
									(cost.per_tuple < preferred->eval_cost.per_tuple)) /* current restriction is cheaper */
							{
								/* set new preferred restriction */
								preferred = ri;
								new_inner_key = left;
								new_outer_key = NULL; /* no need to change */
								distType = outerd->distributionType;
							}
							continue;
						}
					}
					if (innerd->distributionExpr)
					{
						/*
						 * If left side is distribution key of inner subquery
						 * and right expression refers only outer subquery
						 */
						if (equal(innerd->distributionExpr, left) &&
								bms_is_subset(ri->right_relids, outer_rels))
						{
							if (!preferred || /* no preferred restriction yet found */
									(new_inner_key && new_outer_key) || /* preferred restriction require redistribution of both parts */
									(cost.per_tuple < preferred->eval_cost.per_tuple)) /* current restriction is cheaper */
							{
								/* set new preferred restriction */
								preferred = ri;
								new_inner_key = NULL; /* no need to change */
								new_outer_key = right;
								distType = innerd->distributionType;
							}
							continue;
						}
						/*
						 * If right side is distribution key of inner subquery
						 * and left expression refers only outer subquery
						 */
						if (equal(innerd->distributionExpr, right) &&
								bms_is_subset(ri->left_relids, outer_rels))
						{
							if (!preferred || /* no preferred restriction yet found */
									(new_inner_key && new_outer_key) || /* preferred restriction require redistribution of both parts */
									(cost.per_tuple < preferred->eval_cost.per_tuple)) /* current restriction is cheaper */
							{
								/* set new preferred restriction */
								preferred = ri;
								new_inner_key = NULL; /* no need to change */
								new_outer_key = left;
								distType = innerd->distributionType;
							}
							continue;
						}
					}
					/*
					 * Current restriction recuire redistribution of both parts.
					 * If preferred restriction require redistribution of one,
					 * keep it.
					 */
					if (preferred &&
							(new_inner_key == NULL || new_outer_key == NULL))
						continue;

					/*
					 * Skip this condition if the data type of the expressions
					 * does not allow either HASH or MODULO distribution.
					 * HASH distribution is preferrable.
					 */
					if (IsTypeHashDistributable(leftType))
						distType = LOCATOR_TYPE_HASH;
					else if (IsTypeModuloDistributable(leftType))
						distType = LOCATOR_TYPE_MODULO;
					else
						continue;
					/*
					 * If this restriction the first or easier to calculate
					 * then preferred, try to store it as new preferred
					 * restriction to redistribute along it.
					 */
					if (preferred == NULL ||
							(cost.per_tuple < preferred->eval_cost.per_tuple))
					{
						/*
						 * Left expression depends only on outer subpath and
						 * right expression depends only on inner subpath, so
						 * we can redistribute both and make left expression the
						 * distribution key of outer subplan and right
						 * expression the distribution key of inner subplan
						 */
						if (bms_is_subset(ri->left_relids, outer_rels) &&
								bms_is_subset(ri->right_relids, inner_rels))
						{
							preferred = ri;
							new_outer_key = left;
							new_inner_key = right;
						}
						/*
						 * Left expression depends only on inner subpath and
						 * right expression depends only on outer subpath, so
						 * we can redistribute both and make left expression the
						 * distribution key of inner subplan and right
						 * expression the distribution key of outer subplan
						 */
						if (bms_is_subset(ri->left_relids, inner_rels) &&
								bms_is_subset(ri->right_relids, outer_rels))
						{
							preferred = ri;
							new_inner_key = left;
							new_outer_key = right;
						}
					}
				}
			}
		}
		/* If we have suitable restriction we can repartition accordingly */
		if (preferred)
		{
			Bitmapset *nodes = NULL;
			Bitmapset *restrictNodes = NULL;

			/* If we redistribute both parts do join on all nodes ... */
			if (new_inner_key && new_outer_key)
			{
				int i;
				for (i = 0; i < NumDataNodes; i++)
					nodes = bms_add_member(nodes, i);
			}
			/*
			 * ... if we do only one of them redistribute it on the same nodes
			 * as other.
			 */
			else if (new_inner_key)
			{
				nodes = bms_copy(outerd->nodes);
				restrictNodes = bms_copy(outerd->restrictNodes);
			}
			else /*if (new_outer_key)*/
			{
				nodes = bms_copy(innerd->nodes);
				restrictNodes = bms_copy(innerd->restrictNodes);
			}

			/*
			 * Redistribute join by hash, and, if jointype allows, create
			 * alternate path where inner subplan is distributed by replication
			 */
			if (new_inner_key)
			{
				/* Redistribute inner subquery */
				pathnode->innerjoinpath = redistribute_path(
						pathnode->innerjoinpath,
						distType,
						nodes,
						restrictNodes,
						(Node *) new_inner_key);
			}
			/*
			 * Redistribute join by hash, and, if jointype allows, create
			 * alternate path where outer subplan is distributed by replication
			 */
			if (new_outer_key)
			{
				/* Redistribute outer subquery */
				pathnode->outerjoinpath = redistribute_path(
						pathnode->outerjoinpath,
						distType,
						nodes,
						restrictNodes,
						(Node *) new_outer_key);
			}
			targetd = makeNode(Distribution);
			targetd->distributionType = distType;
			targetd->nodes = nodes;
			targetd->restrictNodes = NULL;
			pathnode->path.distribution = targetd;
			/*
			 * In case of outer join distribution key should not refer
			 * distribution key of nullable part.
			 * NB: we should not refer innerd and outerd here, subpathes are
			 * redistributed already
			 */
			if (pathnode->jointype == JOIN_FULL)
				/* both parts are nullable */
				targetd->distributionExpr = NULL;
			else if (pathnode->jointype == JOIN_RIGHT)
				targetd->distributionExpr =
						pathnode->innerjoinpath->distribution->distributionExpr;
			else
				targetd->distributionExpr =
						pathnode->outerjoinpath->distribution->distributionExpr;

			return alternate;
		}
	}

	/*
	 * Build cartesian product, if no hasheable restrictions is found.
	 * Perform coordinator join in such cases. If this join would be a part of
     * larger join, it will be handled as replicated.
	 * To do that leave join distribution NULL and place a RemoteSubPath node on
	 * top of each subpath to provide access to joined result sets.
	 * Do not redistribute pathes that already have NULL distribution, this is
	 * possible if performing outer join on a coordinator and a datanode
	 * relations.
	 */
	if (innerd)
		pathnode->innerjoinpath = redistribute_path(pathnode->innerjoinpath,
													LOCATOR_TYPE_NONE,
													NULL,
													NULL,
													NULL);
	if (outerd)
		pathnode->outerjoinpath = redistribute_path(pathnode->outerjoinpath,
													LOCATOR_TYPE_NONE,
													NULL,
													NULL,
													NULL);
	return alternate;
}
#endif


/*
 * create_seqscan_path
 *	  Creates a path corresponding to a sequential scan, returning the
 *	  pathnode.
 */
Path *
create_seqscan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_SeqScan;
	pathnode->parent = rel;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->pathkeys = NIL;	/* seqscan has unordered result */

#ifdef XCP
	set_scanpath_distribution(root, rel, pathnode);
	if (rel->baserestrictinfo)
	{
		ListCell *lc;
		foreach (lc, rel->baserestrictinfo)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
			restrict_distribution(root, ri, pathnode);
		}
	}
#endif

	cost_seqscan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_index_path
 *	  Creates a path node for an index scan.
 *
 * 'index' is a usable index.
 * 'indexclauses' is a list of RestrictInfo nodes representing clauses
 *			to be used as index qual conditions in the scan.
 * 'indexclausecols' is an integer list of index column numbers (zero based)
 *			the indexclauses can be used with.
 * 'indexorderbys' is a list of bare expressions (no RestrictInfos)
 *			to be used as index ordering operators in the scan.
 * 'indexorderbycols' is an integer list of index column numbers (zero based)
 *			the ordering operators can be used with.
 * 'pathkeys' describes the ordering of the path.
 * 'indexscandir' is ForwardScanDirection or BackwardScanDirection
 *			for an ordered index, or NoMovementScanDirection for
 *			an unordered index.
 * 'indexonly' is true if an index-only scan is wanted.
 * 'required_outer' is the set of outer relids for a parameterized path.
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *		estimates of caching behavior.
 *
 * Returns the new path node.
 */
IndexPath *
create_index_path(PlannerInfo *root,
				  IndexOptInfo *index,
				  List *indexclauses,
				  List *indexclausecols,
				  List *indexorderbys,
				  List *indexorderbycols,
				  List *pathkeys,
				  ScanDirection indexscandir,
				  bool indexonly,
				  Relids required_outer,
				  double loop_count)
{
	IndexPath  *pathnode = makeNode(IndexPath);
	RelOptInfo *rel = index->rel;
	List	   *indexquals,
			   *indexqualcols;

	pathnode->path.pathtype = indexonly ? T_IndexOnlyScan : T_IndexScan;
	pathnode->path.parent = rel;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.pathkeys = pathkeys;

	/* Convert clauses to indexquals the executor can handle */
	expand_indexqual_conditions(index, indexclauses, indexclausecols,
								&indexquals, &indexqualcols);

	/* Fill in the pathnode */
	pathnode->indexinfo = index;
	pathnode->indexclauses = indexclauses;
	pathnode->indexquals = indexquals;
	pathnode->indexqualcols = indexqualcols;
	pathnode->indexorderbys = indexorderbys;
	pathnode->indexorderbycols = indexorderbycols;
	pathnode->indexscandir = indexscandir;

#ifdef XCP
	set_scanpath_distribution(root, rel, (Path *) pathnode);
	if (indexclauses)
	{
		ListCell *lc;
		foreach (lc, indexclauses)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
			restrict_distribution(root, ri, (Path *) pathnode);
		}
	}
#endif
	cost_index(pathnode, root, loop_count);

	return pathnode;
}

/*
 * create_bitmap_heap_path
 *	  Creates a path node for a bitmap scan.
 *
 * 'bitmapqual' is a tree of IndexPath, BitmapAndPath, and BitmapOrPath nodes.
 * 'required_outer' is the set of outer relids for a parameterized path.
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *		estimates of caching behavior.
 *
 * loop_count should match the value used when creating the component
 * IndexPaths.
 */
BitmapHeapPath *
create_bitmap_heap_path(PlannerInfo *root,
						RelOptInfo *rel,
						Path *bitmapqual,
						Relids required_outer,
						double loop_count)
{
	BitmapHeapPath *pathnode = makeNode(BitmapHeapPath);

	pathnode->path.pathtype = T_BitmapHeapScan;
	pathnode->path.parent = rel;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.pathkeys = NIL;		/* always unordered */

	pathnode->bitmapqual = bitmapqual;

#ifdef XCP
	set_scanpath_distribution(root, rel, (Path *) pathnode);
	if (rel->baserestrictinfo)
	{
		ListCell *lc;
		foreach (lc, rel->baserestrictinfo)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
			restrict_distribution(root, ri, (Path *) pathnode);
		}
	}
#endif

	cost_bitmap_heap_scan(&pathnode->path, root, rel,
						  pathnode->path.param_info,
						  bitmapqual, loop_count);

	return pathnode;
}

/*
 * create_bitmap_and_path
 *	  Creates a path node representing a BitmapAnd.
 */
BitmapAndPath *
create_bitmap_and_path(PlannerInfo *root,
					   RelOptInfo *rel,
					   List *bitmapquals)
{
	BitmapAndPath *pathnode = makeNode(BitmapAndPath);

	pathnode->path.pathtype = T_BitmapAnd;
	pathnode->path.parent = rel;
	pathnode->path.param_info = NULL;	/* not used in bitmap trees */
	pathnode->path.pathkeys = NIL;		/* always unordered */

	pathnode->bitmapquals = bitmapquals;

#ifdef XCP
	set_scanpath_distribution(root, rel, (Path *) pathnode);
#endif

	/* this sets bitmapselectivity as well as the regular cost fields: */
	cost_bitmap_and_node(pathnode, root);

	return pathnode;
}

/*
 * create_bitmap_or_path
 *	  Creates a path node representing a BitmapOr.
 */
BitmapOrPath *
create_bitmap_or_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  List *bitmapquals)
{
	BitmapOrPath *pathnode = makeNode(BitmapOrPath);

	pathnode->path.pathtype = T_BitmapOr;
	pathnode->path.parent = rel;
	pathnode->path.param_info = NULL;	/* not used in bitmap trees */
	pathnode->path.pathkeys = NIL;		/* always unordered */

	pathnode->bitmapquals = bitmapquals;

#ifdef XCP
	set_scanpath_distribution(root, rel, (Path *) pathnode);
#endif

	/* this sets bitmapselectivity as well as the regular cost fields: */
	cost_bitmap_or_node(pathnode, root);

	return pathnode;
}

/*
 * create_tidscan_path
 *	  Creates a path corresponding to a scan by TID, returning the pathnode.
 */
TidPath *
create_tidscan_path(PlannerInfo *root, RelOptInfo *rel, List *tidquals)
{
	TidPath    *pathnode = makeNode(TidPath);

	pathnode->path.pathtype = T_TidScan;
	pathnode->path.parent = rel;
	pathnode->path.param_info = NULL;	/* never parameterized at present */
	pathnode->path.pathkeys = NIL;		/* always unordered */

	pathnode->tidquals = tidquals;

#ifdef XCP
	set_scanpath_distribution(root, rel, (Path *) pathnode);
	/* We may need to pass info about target node to support */
	if (pathnode->path.distribution)
		elog(ERROR, "could not perform TID scan on remote relation");
#endif

	cost_tidscan(&pathnode->path, root, rel, tidquals);

	return pathnode;
}

/*
 * create_append_path
 *	  Creates a path corresponding to an Append plan, returning the
 *	  pathnode.
 *
 * Note that we must handle subpaths = NIL, representing a dummy access path.
 */
AppendPath *
create_append_path(RelOptInfo *rel, List *subpaths, Relids required_outer)
{
	AppendPath *pathnode = makeNode(AppendPath);
	ListCell   *l;
#ifdef XCP
	Distribution *distribution;
	Path	   *subpath;
#endif

	pathnode->path.pathtype = T_Append;
	pathnode->path.parent = rel;
	pathnode->path.param_info = get_appendrel_parampathinfo(rel,
															required_outer);
	pathnode->path.pathkeys = NIL;		/* result is always considered
										 * unsorted */
#ifdef XCP
	/*
	 * Append path is used to implement scans of inherited tables and some
	 * "set" operations, like UNION ALL. While all inherited tables should
	 * have the same distribution, UNION'ed queries may have different.
	 * When paths being appended have the same distribution it is OK to push
	 * Append down to the data nodes. If not, perform "coordinator" Append.
	 */

	/* Special case of the dummy relation, if the subpaths list is empty */
	if (subpaths)
	{
		/* Take distribution of the first node */
		l = list_head(subpaths);
		subpath = (Path *) lfirst(l);
		distribution = copyObject(subpath->distribution);
		/*
		 * Check remaining subpaths, if all distributions equal to the first set
		 * it as a distribution of the Append path; otherwise make up coordinator
		 * Append
		 */
		while ((l = lnext(l)))
		{
			subpath = (Path *) lfirst(l);

			if (equal(distribution, subpath->distribution))
			{
				/*
				 * Both distribution and subpath->distribution may be NULL at
				 * this point, or they both are not null.
				 */
				if (distribution && subpath->distribution->restrictNodes)
					distribution->restrictNodes = bms_union(
							distribution->restrictNodes,
							subpath->distribution->restrictNodes);
			}
			else
			{
				break;
			}
		}
		if (l)
		{
			List *newsubpaths = NIL;
			foreach(l, subpaths)
			{
				subpath = (Path *) lfirst(l);
				if (subpath->distribution)
					subpath = redistribute_path(subpath, LOCATOR_TYPE_NONE,
												NULL, NULL, NULL);
				newsubpaths = lappend(newsubpaths, subpath);
			}
			subpaths = newsubpaths;
			pathnode->path.distribution = NULL;
		}
		else
			pathnode->path.distribution = distribution;
	}
#endif
	pathnode->subpaths = subpaths;

	/*
	 * We don't bother with inventing a cost_append(), but just do it here.
	 *
	 * Compute rows and costs as sums of subplan rows and costs.  We charge
	 * nothing extra for the Append itself, which perhaps is too optimistic,
	 * but since it doesn't do any selection or projection, it is a pretty
	 * cheap node.	If you change this, see also make_append().
	 */
	pathnode->path.rows = 0;
	pathnode->path.startup_cost = 0;
	pathnode->path.total_cost = 0;
	foreach(l, subpaths)
	{
		Path	   *subpath = (Path *) lfirst(l);

		pathnode->path.rows += subpath->rows;

		if (l == list_head(subpaths))	/* first node? */
			pathnode->path.startup_cost = subpath->startup_cost;
		pathnode->path.total_cost += subpath->total_cost;

		/* All child paths must have same parameterization */
		Assert(bms_equal(PATH_REQ_OUTER(subpath), required_outer));
	}

	return pathnode;
}

/*
 * create_merge_append_path
 *	  Creates a path corresponding to a MergeAppend plan, returning the
 *	  pathnode.
 */
MergeAppendPath *
create_merge_append_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 List *subpaths,
						 List *pathkeys,
						 Relids required_outer)
{
	MergeAppendPath *pathnode = makeNode(MergeAppendPath);
	Cost		input_startup_cost;
	Cost		input_total_cost;
	ListCell   *l;
#ifdef XCP
	Distribution *distribution = NULL;
	Path	   *subpath;
#endif

	pathnode->path.pathtype = T_MergeAppend;
	pathnode->path.parent = rel;
#ifdef XCP
	/*
	 * It is safe to push down MergeAppend if all subpath distributions
	 * are the same and these distributions are Replicated or distribution key
	 * is the expression of the first pathkey.
	 */
	/* Take distribution of the first node */
	l = list_head(subpaths);
	subpath = (Path *) lfirst(l);
	distribution = copyObject(subpath->distribution);
	/*
	 * Verify if it is safe to push down MergeAppend with this distribution.
	 * TODO implement check of the second condition (distribution key is the
	 * first pathkey)
	 */
	if (distribution == NULL || IsLocatorReplicated(distribution->distributionType))
	{
		/*
		 * Check remaining subpaths, if all distributions equal to the first set
		 * it as a distribution of the Append path; otherwise make up coordinator
		 * Append
		 */
		while ((l = lnext(l)))
		{
			subpath = (Path *) lfirst(l);

			if (distribution && equal(distribution, subpath->distribution))
			{
				if (subpath->distribution->restrictNodes)
					distribution->restrictNodes = bms_union(
							distribution->restrictNodes,
							subpath->distribution->restrictNodes);
			}
			else
			{
				break;
			}
		}
	}
	if (l)
	{
		List *newsubpaths = NIL;
		foreach(l, subpaths)
		{
			subpath = (Path *) lfirst(l);
			if (subpath->distribution)
				subpath = redistribute_path(subpath, LOCATOR_TYPE_NONE,
											NULL, NULL, NULL);
			newsubpaths = lappend(newsubpaths, subpath);
		}
		subpaths = newsubpaths;
		pathnode->path.distribution = NULL;
	}
	else
		pathnode->path.distribution = distribution;
#endif

	pathnode->path.param_info = get_appendrel_parampathinfo(rel,
															required_outer);
	pathnode->path.pathkeys = pathkeys;
	pathnode->subpaths = subpaths;

	/*
	 * Apply query-wide LIMIT if known and path is for sole base relation.
	 * (Handling this at this low level is a bit klugy.)
	 */
	if (bms_equal(rel->relids, root->all_baserels))
		pathnode->limit_tuples = root->limit_tuples;
	else
		pathnode->limit_tuples = -1.0;

	/*
	 * Add up the sizes and costs of the input paths.
	 */
	pathnode->path.rows = 0;
	input_startup_cost = 0;
	input_total_cost = 0;
	foreach(l, subpaths)
	{
		Path	   *subpath = (Path *) lfirst(l);

		pathnode->path.rows += subpath->rows;

		if (pathkeys_contained_in(pathkeys, subpath->pathkeys))
		{
			/* Subpath is adequately ordered, we won't need to sort it */
			input_startup_cost += subpath->startup_cost;
			input_total_cost += subpath->total_cost;
		}
		else
		{
			/* We'll need to insert a Sort node, so include cost for that */
			Path		sort_path;		/* dummy for result of cost_sort */

			cost_sort(&sort_path,
					  root,
					  pathkeys,
					  subpath->total_cost,
					  subpath->parent->tuples,
					  subpath->parent->width,
					  0.0,
					  work_mem,
					  pathnode->limit_tuples);
			input_startup_cost += sort_path.startup_cost;
			input_total_cost += sort_path.total_cost;
		}

		/* All child paths must have same parameterization */
		Assert(bms_equal(PATH_REQ_OUTER(subpath), required_outer));
	}

	/* Now we can compute total costs of the MergeAppend */
	cost_merge_append(&pathnode->path, root,
					  pathkeys, list_length(subpaths),
					  input_startup_cost, input_total_cost,
					  rel->tuples);

	return pathnode;
}

/*
 * create_result_path
 *	  Creates a path representing a Result-and-nothing-else plan.
 *	  This is only used for the case of a query with an empty jointree.
 */
ResultPath *
create_result_path(List *quals)
{
	ResultPath *pathnode = makeNode(ResultPath);

	pathnode->path.pathtype = T_Result;
	pathnode->path.parent = NULL;
	pathnode->path.param_info = NULL;
	pathnode->path.pathkeys = NIL;
	pathnode->quals = quals;

	/* Hardly worth defining a cost_result() function ... just do it */
	pathnode->path.rows = 1;
	pathnode->path.startup_cost = 0;
	pathnode->path.total_cost = cpu_tuple_cost;

	/*
	 * In theory we should include the qual eval cost as well, but at present
	 * that doesn't accomplish much except duplicate work that will be done
	 * again in make_result; since this is only used for degenerate cases,
	 * nothing interesting will be done with the path cost values...
	 */

	return pathnode;
}

/*
 * create_material_path
 *	  Creates a path corresponding to a Material plan, returning the
 *	  pathnode.
 */
MaterialPath *
create_material_path(RelOptInfo *rel, Path *subpath)
{
	MaterialPath *pathnode = makeNode(MaterialPath);

	Assert(subpath->parent == rel);

	pathnode->path.pathtype = T_Material;
	pathnode->path.parent = rel;
	pathnode->path.param_info = subpath->param_info;
	pathnode->path.pathkeys = subpath->pathkeys;

	pathnode->subpath = subpath;

#ifdef XCP
	pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);
#endif

	cost_material(&pathnode->path,
				  subpath->startup_cost,
				  subpath->total_cost,
				  subpath->rows,
				  rel->width);

	return pathnode;
}

/*
 * create_unique_path
 *	  Creates a path representing elimination of distinct rows from the
 *	  input data.  Distinct-ness is defined according to the needs of the
 *	  semijoin represented by sjinfo.  If it is not possible to identify
 *	  how to make the data unique, NULL is returned.
 *
 * If used at all, this is likely to be called repeatedly on the same rel;
 * and the input subpath should always be the same (the cheapest_total path
 * for the rel).  So we cache the result.
 */
UniquePath *
create_unique_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
				   SpecialJoinInfo *sjinfo)
{
	UniquePath *pathnode;
	Path		sort_path;		/* dummy for result of cost_sort */
	Path		agg_path;		/* dummy for result of cost_agg */
	MemoryContext oldcontext;
	List	   *in_operators;
	List	   *uniq_exprs;
	bool		all_btree;
	bool		all_hash;
	int			numCols;
	ListCell   *lc;

	/* Caller made a mistake if subpath isn't cheapest_total ... */
	Assert(subpath == rel->cheapest_total_path);
	Assert(subpath->parent == rel);
	/* ... or if SpecialJoinInfo is the wrong one */
	Assert(sjinfo->jointype == JOIN_SEMI);
	Assert(bms_equal(rel->relids, sjinfo->syn_righthand));

	/* If result already cached, return it */
	if (rel->cheapest_unique_path)
		return (UniquePath *) rel->cheapest_unique_path;

	/* If we previously failed, return NULL quickly */
	if (sjinfo->join_quals == NIL)
		return NULL;

	/*
	 * We must ensure path struct and subsidiary data are allocated in main
	 * planning context; otherwise GEQO memory management causes trouble.
	 */
	oldcontext = MemoryContextSwitchTo(root->planner_cxt);

	/*----------
	 * Look to see whether the semijoin's join quals consist of AND'ed
	 * equality operators, with (only) RHS variables on only one side of
	 * each one.  If so, we can figure out how to enforce uniqueness for
	 * the RHS.
	 *
	 * Note that the input join_quals list is the list of quals that are
	 * *syntactically* associated with the semijoin, which in practice means
	 * the synthesized comparison list for an IN or the WHERE of an EXISTS.
	 * Particularly in the latter case, it might contain clauses that aren't
	 * *semantically* associated with the join, but refer to just one side or
	 * the other.  We can ignore such clauses here, as they will just drop
	 * down to be processed within one side or the other.  (It is okay to
	 * consider only the syntactically-associated clauses here because for a
	 * semijoin, no higher-level quals could refer to the RHS, and so there
	 * can be no other quals that are semantically associated with this join.
	 * We do things this way because it is useful to be able to run this test
	 * before we have extracted the list of quals that are actually
	 * semantically associated with the particular join.)
	 *
	 * Note that the in_operators list consists of the joinqual operators
	 * themselves (but commuted if needed to put the RHS value on the right).
	 * These could be cross-type operators, in which case the operator
	 * actually needed for uniqueness is a related single-type operator.
	 * We assume here that that operator will be available from the btree
	 * or hash opclass when the time comes ... if not, create_unique_plan()
	 * will fail.
	 *----------
	 */
	in_operators = NIL;
	uniq_exprs = NIL;
	all_btree = true;
	all_hash = enable_hashagg;	/* don't consider hash if not enabled */
	foreach(lc, sjinfo->join_quals)
	{
		OpExpr	   *op = (OpExpr *) lfirst(lc);
		Oid			opno;
		Node	   *left_expr;
		Node	   *right_expr;
		Relids		left_varnos;
		Relids		right_varnos;
		Relids		all_varnos;
		Oid			opinputtype;

		/* Is it a binary opclause? */
		if (!IsA(op, OpExpr) ||
			list_length(op->args) != 2)
		{
			/* No, but does it reference both sides? */
			all_varnos = pull_varnos((Node *) op);
			if (!bms_overlap(all_varnos, sjinfo->syn_righthand) ||
				bms_is_subset(all_varnos, sjinfo->syn_righthand))
			{
				/*
				 * Clause refers to only one rel, so ignore it --- unless it
				 * contains volatile functions, in which case we'd better
				 * punt.
				 */
				if (contain_volatile_functions((Node *) op))
					goto no_unique_path;
				continue;
			}
			/* Non-operator clause referencing both sides, must punt */
			goto no_unique_path;
		}

		/* Extract data from binary opclause */
		opno = op->opno;
		left_expr = linitial(op->args);
		right_expr = lsecond(op->args);
		left_varnos = pull_varnos(left_expr);
		right_varnos = pull_varnos(right_expr);
		all_varnos = bms_union(left_varnos, right_varnos);
		opinputtype = exprType(left_expr);

		/* Does it reference both sides? */
		if (!bms_overlap(all_varnos, sjinfo->syn_righthand) ||
			bms_is_subset(all_varnos, sjinfo->syn_righthand))
		{
			/*
			 * Clause refers to only one rel, so ignore it --- unless it
			 * contains volatile functions, in which case we'd better punt.
			 */
			if (contain_volatile_functions((Node *) op))
				goto no_unique_path;
			continue;
		}

		/* check rel membership of arguments */
		if (!bms_is_empty(right_varnos) &&
			bms_is_subset(right_varnos, sjinfo->syn_righthand) &&
			!bms_overlap(left_varnos, sjinfo->syn_righthand))
		{
			/* typical case, right_expr is RHS variable */
		}
		else if (!bms_is_empty(left_varnos) &&
				 bms_is_subset(left_varnos, sjinfo->syn_righthand) &&
				 !bms_overlap(right_varnos, sjinfo->syn_righthand))
		{
			/* flipped case, left_expr is RHS variable */
			opno = get_commutator(opno);
			if (!OidIsValid(opno))
				goto no_unique_path;
			right_expr = left_expr;
		}
		else
			goto no_unique_path;

		/* all operators must be btree equality or hash equality */
		if (all_btree)
		{
			/* oprcanmerge is considered a hint... */
			if (!op_mergejoinable(opno, opinputtype) ||
				get_mergejoin_opfamilies(opno) == NIL)
				all_btree = false;
		}
		if (all_hash)
		{
			/* ... but oprcanhash had better be correct */
			if (!op_hashjoinable(opno, opinputtype))
				all_hash = false;
		}
		if (!(all_btree || all_hash))
			goto no_unique_path;

		/* so far so good, keep building lists */
		in_operators = lappend_oid(in_operators, opno);
		uniq_exprs = lappend(uniq_exprs, copyObject(right_expr));
	}

	/* Punt if we didn't find at least one column to unique-ify */
	if (uniq_exprs == NIL)
		goto no_unique_path;

	/*
	 * The expressions we'd need to unique-ify mustn't be volatile.
	 */
	if (contain_volatile_functions((Node *) uniq_exprs))
		goto no_unique_path;

#ifdef XCP
	/*
	 * We may only guarantee uniqueness if subplan is either replicated or it is
	 * partitioned and one of the unigue expressions equals to the
	 * distribution expression.
	 */
	if (subpath->distribution &&
		!IsLocatorReplicated(subpath->distribution->distributionType))
	{
		/* Punt if no distribution key */
		if (subpath->distribution->distributionExpr == NULL)
			goto no_unique_path;

		foreach(lc, uniq_exprs)
		{
			void *expr = lfirst(lc);
			if (equal(expr, subpath->distribution->distributionExpr))
				break;
		}

		/* XXX we may try and repartition if no matching expression */
		if (!lc)
			goto no_unique_path;
	}
#endif

	/*
	 * If we get here, we can unique-ify using at least one of sorting and
	 * hashing.  Start building the result Path object.
	 */
	pathnode = makeNode(UniquePath);

	pathnode->path.pathtype = T_Unique;
	pathnode->path.parent = rel;
	pathnode->path.param_info = subpath->param_info;

	/*
	 * Assume the output is unsorted, since we don't necessarily have pathkeys
	 * to represent it.  (This might get overridden below.)
	 */
	pathnode->path.pathkeys = NIL;

	pathnode->subpath = subpath;
	pathnode->in_operators = in_operators;
	pathnode->uniq_exprs = uniq_exprs;

#ifdef XCP
	/* distribution is the same as in the subpath */
	pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);
#endif

	/*
	 * If the input is a relation and it has a unique index that proves the
	 * uniq_exprs are unique, then we don't need to do anything.  Note that
	 * relation_has_unique_index_for automatically considers restriction
	 * clauses for the rel, as well.
	 */
	if (rel->rtekind == RTE_RELATION && all_btree &&
		relation_has_unique_index_for(root, rel, NIL,
									  uniq_exprs, in_operators))
	{
		pathnode->umethod = UNIQUE_PATH_NOOP;
		pathnode->path.rows = rel->rows;
		pathnode->path.startup_cost = subpath->startup_cost;
		pathnode->path.total_cost = subpath->total_cost;
		pathnode->path.pathkeys = subpath->pathkeys;

		rel->cheapest_unique_path = (Path *) pathnode;

		MemoryContextSwitchTo(oldcontext);

		return pathnode;
	}

	/*
	 * If the input is a subquery whose output must be unique already, then we
	 * don't need to do anything.  The test for uniqueness has to consider
	 * exactly which columns we are extracting; for example "SELECT DISTINCT
	 * x,y" doesn't guarantee that x alone is distinct. So we cannot check for
	 * this optimization unless uniq_exprs consists only of simple Vars
	 * referencing subquery outputs.  (Possibly we could do something with
	 * expressions in the subquery outputs, too, but for now keep it simple.)
	 */
	if (rel->rtekind == RTE_SUBQUERY)
	{
		RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
		List	   *sub_tlist_colnos;

		sub_tlist_colnos = translate_sub_tlist(uniq_exprs, rel->relid);

		if (sub_tlist_colnos &&
			query_is_distinct_for(rte->subquery,
								  sub_tlist_colnos, in_operators))
		{
			pathnode->umethod = UNIQUE_PATH_NOOP;
			pathnode->path.rows = rel->rows;
			pathnode->path.startup_cost = subpath->startup_cost;
			pathnode->path.total_cost = subpath->total_cost;
			pathnode->path.pathkeys = subpath->pathkeys;

			rel->cheapest_unique_path = (Path *) pathnode;

			MemoryContextSwitchTo(oldcontext);

			return pathnode;
		}
	}

	/* Estimate number of output rows */
	pathnode->path.rows = estimate_num_groups(root, uniq_exprs, rel->rows);
	numCols = list_length(uniq_exprs);

	if (all_btree)
	{
		/*
		 * Estimate cost for sort+unique implementation
		 */
		cost_sort(&sort_path, root, NIL,
				  subpath->total_cost,
				  rel->rows,
				  rel->width,
				  0.0,
				  work_mem,
				  -1.0);

		/*
		 * Charge one cpu_operator_cost per comparison per input tuple. We
		 * assume all columns get compared at most of the tuples. (XXX
		 * probably this is an overestimate.)  This should agree with
		 * make_unique.
		 */
		sort_path.total_cost += cpu_operator_cost * rel->rows * numCols;
	}

	if (all_hash)
	{
		/*
		 * Estimate the overhead per hashtable entry at 64 bytes (same as in
		 * planner.c).
		 */
		int			hashentrysize = rel->width + 64;

		if (hashentrysize * pathnode->path.rows > work_mem * 1024L)
			all_hash = false;	/* don't try to hash */
		else
			cost_agg(&agg_path, root,
					 AGG_HASHED, NULL,
					 numCols, pathnode->path.rows,
					 subpath->startup_cost,
					 subpath->total_cost,
					 rel->rows);
	}

	if (all_btree && all_hash)
	{
		if (agg_path.total_cost < sort_path.total_cost)
			pathnode->umethod = UNIQUE_PATH_HASH;
		else
			pathnode->umethod = UNIQUE_PATH_SORT;
	}
	else if (all_btree)
		pathnode->umethod = UNIQUE_PATH_SORT;
	else if (all_hash)
		pathnode->umethod = UNIQUE_PATH_HASH;
	else
		goto no_unique_path;

	if (pathnode->umethod == UNIQUE_PATH_HASH)
	{
		pathnode->path.startup_cost = agg_path.startup_cost;
		pathnode->path.total_cost = agg_path.total_cost;
	}
	else
	{
		pathnode->path.startup_cost = sort_path.startup_cost;
		pathnode->path.total_cost = sort_path.total_cost;
	}

	rel->cheapest_unique_path = (Path *) pathnode;

	MemoryContextSwitchTo(oldcontext);

	return pathnode;

no_unique_path:			/* failure exit */

	/* Mark the SpecialJoinInfo as not unique-able */
	sjinfo->join_quals = NIL;

	MemoryContextSwitchTo(oldcontext);

	return NULL;
}

/*
 * translate_sub_tlist - get subquery column numbers represented by tlist
 *
 * The given targetlist usually contains only Vars referencing the given relid.
 * Extract their varattnos (ie, the column numbers of the subquery) and return
 * as an integer List.
 *
 * If any of the tlist items is not a simple Var, we cannot determine whether
 * the subquery's uniqueness condition (if any) matches ours, so punt and
 * return NIL.
 */
static List *
translate_sub_tlist(List *tlist, int relid)
{
	List	   *result = NIL;
	ListCell   *l;

	foreach(l, tlist)
	{
		Var		   *var = (Var *) lfirst(l);

		if (!var || !IsA(var, Var) ||
			var->varno != relid)
			return NIL;			/* punt */

		result = lappend_int(result, var->varattno);
	}
	return result;
}

/*
 * query_is_distinct_for - does query never return duplicates of the
 *		specified columns?
 *
 * colnos is an integer list of output column numbers (resno's).  We are
 * interested in whether rows consisting of just these columns are certain
 * to be distinct.	"Distinctness" is defined according to whether the
 * corresponding upper-level equality operators listed in opids would think
 * the values are distinct.  (Note: the opids entries could be cross-type
 * operators, and thus not exactly the equality operators that the subquery
 * would use itself.  We use equality_ops_are_compatible() to check
 * compatibility.  That looks at btree or hash opfamily membership, and so
 * should give trustworthy answers for all operators that we might need
 * to deal with here.)
 */
static bool
query_is_distinct_for(Query *query, List *colnos, List *opids)
{
	ListCell   *l;
	Oid			opid;

	Assert(list_length(colnos) == list_length(opids));

	/*
	 * DISTINCT (including DISTINCT ON) guarantees uniqueness if all the
	 * columns in the DISTINCT clause appear in colnos and operator semantics
	 * match.
	 */
	if (query->distinctClause)
	{
		foreach(l, query->distinctClause)
		{
			SortGroupClause *sgc = (SortGroupClause *) lfirst(l);
			TargetEntry *tle = get_sortgroupclause_tle(sgc,
													   query->targetList);

			opid = distinct_col_search(tle->resno, colnos, opids);
			if (!OidIsValid(opid) ||
				!equality_ops_are_compatible(opid, sgc->eqop))
				break;			/* exit early if no match */
		}
		if (l == NULL)			/* had matches for all? */
			return true;
	}

	/*
	 * Similarly, GROUP BY guarantees uniqueness if all the grouped columns
	 * appear in colnos and operator semantics match.
	 */
	if (query->groupClause)
	{
		foreach(l, query->groupClause)
		{
			SortGroupClause *sgc = (SortGroupClause *) lfirst(l);
			TargetEntry *tle = get_sortgroupclause_tle(sgc,
													   query->targetList);

			opid = distinct_col_search(tle->resno, colnos, opids);
			if (!OidIsValid(opid) ||
				!equality_ops_are_compatible(opid, sgc->eqop))
				break;			/* exit early if no match */
		}
		if (l == NULL)			/* had matches for all? */
			return true;
	}
	else
	{
		/*
		 * If we have no GROUP BY, but do have aggregates or HAVING, then the
		 * result is at most one row so it's surely unique, for any operators.
		 */
		if (query->hasAggs || query->havingQual)
			return true;
	}

	/*
	 * UNION, INTERSECT, EXCEPT guarantee uniqueness of the whole output row,
	 * except with ALL.
	 */
	if (query->setOperations)
	{
		SetOperationStmt *topop = (SetOperationStmt *) query->setOperations;

		Assert(IsA(topop, SetOperationStmt));
		Assert(topop->op != SETOP_NONE);

		if (!topop->all)
		{
			ListCell   *lg;

			/* We're good if all the nonjunk output columns are in colnos */
			lg = list_head(topop->groupClauses);
			foreach(l, query->targetList)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(l);
				SortGroupClause *sgc;

				if (tle->resjunk)
					continue;	/* ignore resjunk columns */

				/* non-resjunk columns should have grouping clauses */
				Assert(lg != NULL);
				sgc = (SortGroupClause *) lfirst(lg);
				lg = lnext(lg);

				opid = distinct_col_search(tle->resno, colnos, opids);
				if (!OidIsValid(opid) ||
					!equality_ops_are_compatible(opid, sgc->eqop))
					break;		/* exit early if no match */
			}
			if (l == NULL)		/* had matches for all? */
				return true;
		}
	}

	/*
	 * XXX Are there any other cases in which we can easily see the result
	 * must be distinct?
	 */

	return false;
}

/*
 * distinct_col_search - subroutine for query_is_distinct_for
 *
 * If colno is in colnos, return the corresponding element of opids,
 * else return InvalidOid.	(We expect colnos does not contain duplicates,
 * so the result is well-defined.)
 */
static Oid
distinct_col_search(int colno, List *colnos, List *opids)
{
	ListCell   *lc1,
			   *lc2;

	forboth(lc1, colnos, lc2, opids)
	{
		if (colno == lfirst_int(lc1))
			return lfirst_oid(lc2);
	}
	return InvalidOid;
}

/*
 * create_subqueryscan_path
 *	  Creates a path corresponding to a sequential scan of a subquery,
 *	  returning the pathnode.
 */
Path *
#ifdef XCP
create_subqueryscan_path(PlannerInfo *root, RelOptInfo *rel,
						 List *pathkeys, Relids required_outer,
						 Distribution *distribution)
#else
create_subqueryscan_path(PlannerInfo *root, RelOptInfo *rel,
						 List *pathkeys, Relids required_outer)
#endif
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_SubqueryScan;
	pathnode->parent = rel;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->pathkeys = pathkeys;
#ifdef XCP
	pathnode->distribution = distribution;
#endif

	cost_subqueryscan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_functionscan_path
 *	  Creates a path corresponding to a sequential scan of a function,
 *	  returning the pathnode.
 */
Path *
create_functionscan_path(PlannerInfo *root, RelOptInfo *rel)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_FunctionScan;
	pathnode->parent = rel;
	pathnode->param_info = NULL;	/* never parameterized at present */
	pathnode->pathkeys = NIL;	/* for now, assume unordered result */

	cost_functionscan(pathnode, root, rel);

	return pathnode;
}

/*
 * create_valuesscan_path
 *	  Creates a path corresponding to a scan of a VALUES list,
 *	  returning the pathnode.
 */
Path *
create_valuesscan_path(PlannerInfo *root, RelOptInfo *rel)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_ValuesScan;
	pathnode->parent = rel;
	pathnode->param_info = NULL;	/* never parameterized at present */
	pathnode->pathkeys = NIL;	/* result is always unordered */

	cost_valuesscan(pathnode, root, rel);

	return pathnode;
}

/*
 * create_ctescan_path
 *	  Creates a path corresponding to a scan of a non-self-reference CTE,
 *	  returning the pathnode.
 */
Path *
create_ctescan_path(PlannerInfo *root, RelOptInfo *rel)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_CteScan;
	pathnode->parent = rel;
	pathnode->param_info = NULL;	/* never parameterized at present */
	pathnode->pathkeys = NIL;	/* XXX for now, result is always unordered */

	cost_ctescan(pathnode, root, rel);

	return pathnode;
}

/*
 * create_worktablescan_path
 *	  Creates a path corresponding to a scan of a self-reference CTE,
 *	  returning the pathnode.
 */
Path *
create_worktablescan_path(PlannerInfo *root, RelOptInfo *rel)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_WorkTableScan;
	pathnode->parent = rel;
	pathnode->param_info = NULL;	/* never parameterized at present */
	pathnode->pathkeys = NIL;	/* result is always unordered */

	/* Cost is the same as for a regular CTE scan */
	cost_ctescan(pathnode, root, rel);

	return pathnode;
}


#ifdef PGXC
#ifndef XCP
/*
 * create_remotequery_path
 *	  Creates a path corresponding to a scan of a remote query,
 *	  returning the pathnode.
 */
Path *
create_remotequery_path(PlannerInfo *root, RelOptInfo *rel)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_RemoteQuery;
	pathnode->parent = rel;
	pathnode->param_info = NULL;	/* never parameterized at present */
	pathnode->pathkeys = NIL;	/* result is always unordered */

	/* PGXCTODO - set cost properly */
	cost_seqscan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}
#endif /* XCP */
#endif /* PGXC */


/*
 * create_foreignscan_path
 *	  Creates a path corresponding to a scan of a foreign table,
 *	  returning the pathnode.
 *
 * This function is never called from core Postgres; rather, it's expected
 * to be called by the GetForeignPaths function of a foreign data wrapper.
 * We make the FDW supply all fields of the path, since we do not have any
 * way to calculate them in core.
 */
ForeignPath *
create_foreignscan_path(PlannerInfo *root, RelOptInfo *rel,
						double rows, Cost startup_cost, Cost total_cost,
						List *pathkeys,
						Relids required_outer,
						List *fdw_private)
{
	ForeignPath *pathnode = makeNode(ForeignPath);

	pathnode->path.pathtype = T_ForeignScan;
	pathnode->path.parent = rel;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.rows = rows;
	pathnode->path.startup_cost = startup_cost;
	pathnode->path.total_cost = total_cost;
	pathnode->path.pathkeys = pathkeys;

	pathnode->fdw_private = fdw_private;

	return pathnode;
}

/*
 * calc_nestloop_required_outer
 *	  Compute the required_outer set for a nestloop join path
 *
 * Note: result must not share storage with either input
 */
Relids
calc_nestloop_required_outer(Path *outer_path, Path *inner_path)
{
	Relids		outer_paramrels = PATH_REQ_OUTER(outer_path);
	Relids		inner_paramrels = PATH_REQ_OUTER(inner_path);
	Relids		required_outer;

	/* inner_path can require rels from outer path, but not vice versa */
	Assert(!bms_overlap(outer_paramrels, inner_path->parent->relids));
	/* easy case if inner path is not parameterized */
	if (!inner_paramrels)
		return bms_copy(outer_paramrels);
	/* else, form the union ... */
	required_outer = bms_union(outer_paramrels, inner_paramrels);
	/* ... and remove any mention of now-satisfied outer rels */
	required_outer = bms_del_members(required_outer,
									 outer_path->parent->relids);
	/* maintain invariant that required_outer is exactly NULL if empty */
	if (bms_is_empty(required_outer))
	{
		bms_free(required_outer);
		required_outer = NULL;
	}
	return required_outer;
}

/*
 * calc_non_nestloop_required_outer
 *	  Compute the required_outer set for a merge or hash join path
 *
 * Note: result must not share storage with either input
 */
Relids
calc_non_nestloop_required_outer(Path *outer_path, Path *inner_path)
{
	Relids		outer_paramrels = PATH_REQ_OUTER(outer_path);
	Relids		inner_paramrels = PATH_REQ_OUTER(inner_path);
	Relids		required_outer;

	/* neither path can require rels from the other */
	Assert(!bms_overlap(outer_paramrels, inner_path->parent->relids));
	Assert(!bms_overlap(inner_paramrels, outer_path->parent->relids));
	/* form the union ... */
	required_outer = bms_union(outer_paramrels, inner_paramrels);
	/* we do not need an explicit test for empty; bms_union gets it right */
	return required_outer;
}

/*
 * create_nestloop_path
 *	  Creates a pathnode corresponding to a nestloop join between two
 *	  relations.
 *
 * 'joinrel' is the join relation.
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_nestloop
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'semifactors' contains valid data if jointype is SEMI or ANTI
 * 'outer_path' is the outer path
 * 'inner_path' is the inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'pathkeys' are the path keys of the new join path
 * 'required_outer' is the set of required outer rels
 *
 * Returns the resulting path node.
 */
NestPath *
create_nestloop_path(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 JoinType jointype,
					 JoinCostWorkspace *workspace,
					 SpecialJoinInfo *sjinfo,
					 SemiAntiJoinFactors *semifactors,
					 Path *outer_path,
					 Path *inner_path,
					 List *restrict_clauses,
					 List *pathkeys,
					 Relids required_outer)
{
	NestPath   *pathnode = makeNode(NestPath);
#ifdef XCP
	List	   *alternate;
	ListCell   *lc;
#endif
	Relids		inner_req_outer = PATH_REQ_OUTER(inner_path);

	/*
	 * If the inner path is parameterized by the outer, we must drop any
	 * restrict_clauses that are due to be moved into the inner path.  We have
	 * to do this now, rather than postpone the work till createplan time,
	 * because the restrict_clauses list can affect the size and cost
	 * estimates for this path.
	 */
	if (bms_overlap(inner_req_outer, outer_path->parent->relids))
	{
		Relids		inner_and_outer = bms_union(inner_path->parent->relids,
												inner_req_outer);
		List	   *jclauses = NIL;
		ListCell   *lc;

		foreach(lc, restrict_clauses)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			if (!join_clause_is_movable_into(rinfo,
											 inner_path->parent->relids,
											 inner_and_outer))
				jclauses = lappend(jclauses, rinfo);
		}
		restrict_clauses = jclauses;
	}

	pathnode->path.pathtype = T_NestLoop;
	pathnode->path.parent = joinrel;
	pathnode->path.param_info =
		get_joinrel_parampathinfo(root,
								  joinrel,
								  outer_path,
								  inner_path,
								  sjinfo,
								  required_outer,
								  &restrict_clauses);
	pathnode->path.pathkeys = pathkeys;
	pathnode->jointype = jointype;
	pathnode->outerjoinpath = outer_path;
	pathnode->innerjoinpath = inner_path;
	pathnode->joinrestrictinfo = restrict_clauses;

#ifdef XCP
	alternate = set_joinpath_distribution(root, pathnode);
#endif
	final_cost_nestloop(root, pathnode, workspace, sjinfo, semifactors);

#ifdef XCP
	/*
	 * Also calculate costs of all alternates and return cheapest path
	 */
	foreach(lc, alternate)
	{
		NestPath *altpath = (NestPath *) lfirst(lc);
		final_cost_nestloop(root, altpath, workspace, sjinfo, semifactors);
		if (altpath->path.total_cost < pathnode->path.total_cost)
			pathnode = altpath;
	}
#endif

	return pathnode;
}

/*
 * create_mergejoin_path
 *	  Creates a pathnode corresponding to a mergejoin join between
 *	  two relations
 *
 * 'joinrel' is the join relation
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_mergejoin
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'outer_path' is the outer path
 * 'inner_path' is the inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'pathkeys' are the path keys of the new join path
 * 'required_outer' is the set of required outer rels
 * 'mergeclauses' are the RestrictInfo nodes to use as merge clauses
 *		(this should be a subset of the restrict_clauses list)
 * 'outersortkeys' are the sort varkeys for the outer relation
 * 'innersortkeys' are the sort varkeys for the inner relation
 */
MergePath *
create_mergejoin_path(PlannerInfo *root,
					  RelOptInfo *joinrel,
					  JoinType jointype,
					  JoinCostWorkspace *workspace,
					  SpecialJoinInfo *sjinfo,
					  Path *outer_path,
					  Path *inner_path,
					  List *restrict_clauses,
					  List *pathkeys,
					  Relids required_outer,
					  List *mergeclauses,
					  List *outersortkeys,
					  List *innersortkeys)
{
	MergePath  *pathnode = makeNode(MergePath);
#ifdef XCP
	List	   *alternate;
	ListCell   *lc;
#endif

	pathnode->jpath.path.pathtype = T_MergeJoin;
	pathnode->jpath.path.parent = joinrel;
	pathnode->jpath.path.param_info =
		get_joinrel_parampathinfo(root,
								  joinrel,
								  outer_path,
								  inner_path,
								  sjinfo,
								  required_outer,
								  &restrict_clauses);
	pathnode->jpath.path.pathkeys = pathkeys;
	pathnode->jpath.jointype = jointype;
	pathnode->jpath.outerjoinpath = outer_path;
	pathnode->jpath.innerjoinpath = inner_path;
	pathnode->jpath.joinrestrictinfo = restrict_clauses;
	pathnode->path_mergeclauses = mergeclauses;
	pathnode->outersortkeys = outersortkeys;
	pathnode->innersortkeys = innersortkeys;
#ifdef XCP
	alternate = set_joinpath_distribution(root, (JoinPath *) pathnode);
#endif
	/* pathnode->materialize_inner will be set by final_cost_mergejoin */
	final_cost_mergejoin(root, pathnode, workspace, sjinfo);

#ifdef XCP
	/*
	 * Also calculate costs of all alternates and return cheapest path
	 */
	foreach(lc, alternate)
	{
		MergePath *altpath = (MergePath *) lfirst(lc);
		final_cost_mergejoin(root, altpath, workspace, sjinfo);
		if (altpath->jpath.path.total_cost < pathnode->jpath.path.total_cost)
			pathnode = altpath;
	}
#endif

	return pathnode;
}

/*
 * create_hashjoin_path
 *	  Creates a pathnode corresponding to a hash join between two relations.
 *
 * 'joinrel' is the join relation
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_hashjoin
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'semifactors' contains valid data if jointype is SEMI or ANTI
 * 'outer_path' is the cheapest outer path
 * 'inner_path' is the cheapest inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'required_outer' is the set of required outer rels
 * 'hashclauses' are the RestrictInfo nodes to use as hash clauses
 *		(this should be a subset of the restrict_clauses list)
 */
HashPath *
create_hashjoin_path(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 JoinType jointype,
					 JoinCostWorkspace *workspace,
					 SpecialJoinInfo *sjinfo,
					 SemiAntiJoinFactors *semifactors,
					 Path *outer_path,
					 Path *inner_path,
					 List *restrict_clauses,
					 Relids required_outer,
					 List *hashclauses)
{
	HashPath   *pathnode = makeNode(HashPath);
#ifdef XCP
	List	   *alternate;
	ListCell   *lc;
#endif

	pathnode->jpath.path.pathtype = T_HashJoin;
	pathnode->jpath.path.parent = joinrel;
	pathnode->jpath.path.param_info =
		get_joinrel_parampathinfo(root,
								  joinrel,
								  outer_path,
								  inner_path,
								  sjinfo,
								  required_outer,
								  &restrict_clauses);

	/*
	 * A hashjoin never has pathkeys, since its output ordering is
	 * unpredictable due to possible batching.	XXX If the inner relation is
	 * small enough, we could instruct the executor that it must not batch,
	 * and then we could assume that the output inherits the outer relation's
	 * ordering, which might save a sort step.	However there is considerable
	 * downside if our estimate of the inner relation size is badly off. For
	 * the moment we don't risk it.  (Note also that if we wanted to take this
	 * seriously, joinpath.c would have to consider many more paths for the
	 * outer rel than it does now.)
	 */
	pathnode->jpath.path.pathkeys = NIL;
	pathnode->jpath.jointype = jointype;
	pathnode->jpath.outerjoinpath = outer_path;
	pathnode->jpath.innerjoinpath = inner_path;
	pathnode->jpath.joinrestrictinfo = restrict_clauses;
	pathnode->path_hashclauses = hashclauses;
#ifdef XCP
	alternate = set_joinpath_distribution(root, (JoinPath *) pathnode);
#endif
	/* final_cost_hashjoin will fill in pathnode->num_batches */
	final_cost_hashjoin(root, pathnode, workspace, sjinfo, semifactors);

#ifdef XCP
	/*
	 * Calculate costs of all alternates and return cheapest path
	 */
	foreach(lc, alternate)
	{
		HashPath *altpath = (HashPath *) lfirst(lc);
		final_cost_hashjoin(root, altpath, workspace, sjinfo, semifactors);
		if (altpath->jpath.path.total_cost < pathnode->jpath.path.total_cost)
			pathnode = altpath;
	}
#endif

	return pathnode;
}

/*
 * reparameterize_path
 *		Attempt to modify a Path to have greater parameterization
 *
 * We use this to attempt to bring all child paths of an appendrel to the
 * same parameterization level, ensuring that they all enforce the same set
 * of join quals (and thus that that parameterization can be attributed to
 * an append path built from such paths).  Currently, only a few path types
 * are supported here, though more could be added at need.	We return NULL
 * if we can't reparameterize the given path.
 *
 * Note: we intentionally do not pass created paths to add_path(); it would
 * possibly try to delete them on the grounds of being cost-inferior to the
 * paths they were made from, and we don't want that.  Paths made here are
 * not necessarily of general-purpose usefulness, but they can be useful
 * as members of an append path.
 */
Path *
reparameterize_path(PlannerInfo *root, Path *path,
					Relids required_outer,
					double loop_count)
{
	RelOptInfo *rel = path->parent;

	/* Can only increase, not decrease, path's parameterization */
	if (!bms_is_subset(PATH_REQ_OUTER(path), required_outer))
		return NULL;
	switch (path->pathtype)
	{
		case T_SeqScan:
			return create_seqscan_path(root, rel, required_outer);
		case T_IndexScan:
		case T_IndexOnlyScan:
			{
				IndexPath  *ipath = (IndexPath *) path;
				IndexPath  *newpath = makeNode(IndexPath);

				/*
				 * We can't use create_index_path directly, and would not want
				 * to because it would re-compute the indexqual conditions
				 * which is wasted effort.	Instead we hack things a bit:
				 * flat-copy the path node, revise its param_info, and redo
				 * the cost estimate.
				 */
				memcpy(newpath, ipath, sizeof(IndexPath));
				newpath->path.param_info =
					get_baserel_parampathinfo(root, rel, required_outer);
				cost_index(newpath, root, loop_count);
				return (Path *) newpath;
			}
		case T_BitmapHeapScan:
			{
				BitmapHeapPath *bpath = (BitmapHeapPath *) path;

				return (Path *) create_bitmap_heap_path(root,
														rel,
														bpath->bitmapqual,
														required_outer,
														loop_count);
			}
		case T_SubqueryScan:
#ifdef XCP
			return create_subqueryscan_path(root, rel, path->pathkeys,
											required_outer, path->distribution);
#else
			return create_subqueryscan_path(root, rel, path->pathkeys,
											required_outer);
#endif
		default:
			break;
	}
	return NULL;
}
