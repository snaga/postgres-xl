/*-------------------------------------------------------------------------
 *
 * parse_agg.h
 *	  handle aggregates and window functions in parser
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_agg.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_AGG_H
#define PARSE_AGG_H

#include "parser/parse_node.h"

extern void transformAggregateCall(ParseState *pstate, Aggref *agg,
					   List *args, List *aggorder,
					   bool agg_distinct);
extern void transformWindowFuncCall(ParseState *pstate, WindowFunc *wfunc,
						WindowDef *windef);

extern void parseCheckAggregates(ParseState *pstate, Query *qry);
extern void parseCheckWindowFuncs(ParseState *pstate, Query *qry);

extern void build_aggregate_fnexprs(Oid *agg_input_types,
						int agg_num_inputs,
						Oid agg_state_type,
#ifdef XCP
						Oid agg_collect_type,
#endif
						Oid agg_result_type,
						Oid agg_input_collation,
						Oid transfn_oid,
#ifdef XCP
						Oid collectfn_oid,
#endif
						Oid finalfn_oid,
						Expr **transfnexpr,
#ifdef XCP
						Expr **collectfnexpr,
#endif
						Expr **finalfnexpr);

#endif   /* PARSE_AGG_H */
