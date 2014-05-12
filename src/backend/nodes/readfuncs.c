/*-------------------------------------------------------------------------
 *
 * readfuncs.c
 *	  Reader functions for Postgres tree nodes.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/readfuncs.c
 *
 * NOTES
 *	  Path and Plan nodes do not have any readfuncs support, because we
 *	  never have occasion to read them in.	(There was once code here that
 *	  claimed to read them, but it was broken as well as unused.)  We
 *	  never read executor state trees, either.
 *
 *	  Parse location fields are written out by outfuncs.c, but only for
 *	  possible debugging use.  When reading a location field, we discard
 *	  the stored value and set the location field to -1 (ie, "unknown").
 *	  This is because nodes coming from a stored rule should not be thought
 *	  to have a known location in the current query's text.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "nodes/parsenodes.h"
#include "nodes/readfuncs.h"
#ifdef PGXC
#include "access/htup.h"
#endif
#ifdef XCP
#include "fmgr.h"
#include "catalog/namespace.h"
#include "nodes/plannodes.h"
#include "pgxc/execRemote.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/*
 * When we sending query plans between nodes we need to send OIDs of various
 * objects - relations, data types, functions, etc.
 * On different nodes OIDs of these objects may differ, so we need to send an
 * identifier, depending on object type, allowing to lookup OID on target node.
 * On the other hand we want to save space when storing rules, or in other cases
 * when we need to encode and decode nodes on the same node.
 * For now default format is not portable, as it is in original Postgres code.
 * Later we may want to add extra parameter in stringToNode() function
 */
static bool portable_input = false;
void
set_portable_input(bool value)
{
	portable_input = value;
}
#endif /* XCP */

/*
 * Macros to simplify reading of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.	Note that these
 * hard-wire conventions about the names of the local variables in a Read
 * routine.
 */

/* Macros for declaring appropriate local variables */

/* A few guys need only local_node */
#define READ_LOCALS_NO_FIELDS(nodeTypeName) \
	nodeTypeName *local_node = makeNode(nodeTypeName)

/* And a few guys need only the pg_strtok support fields */
#define READ_TEMP_LOCALS()	\
	char	   *token;		\
	int			length;		\
	(void) token				/* possibly unused */

/* ... but most need both */
#define READ_LOCALS(nodeTypeName)			\
	READ_LOCALS_NO_FIELDS(nodeTypeName);	\
	READ_TEMP_LOCALS()

/* Read an integer field (anything written as ":fldname %d") */
#define READ_INT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoi(token)

/* Read an unsigned integer field (anything written as ":fldname %u") */
#define READ_UINT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoui(token)

#ifdef XCP
/* Read a long integer field (anything written as ":fldname %ld") */
#define READ_LONG_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atol(token)
#endif

/* Read an OID field (don't hard-wire assumption that OID is same as uint) */
#ifdef XCP
#define READ_OID_FIELD(fldname) \
	(AssertMacro(!portable_input), 	/* only allow to read OIDs within a node */ \
	 token = pg_strtok(&length),	/* skip :fldname */ \
	 token = pg_strtok(&length),	/* get field value */ \
	 local_node->fldname = atooid(token))
#else
#define READ_OID_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atooid(token)
#endif

/* Read a char field (ie, one ascii character) */
#define READ_CHAR_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = token[0]

/* Read an enumerated-type field that was written as an integer code */
#define READ_ENUM_FIELD(fldname, enumtype) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = (enumtype) atoi(token)

/* Read a float field */
#define READ_FLOAT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atof(token)

/* Read a boolean field */
#define READ_BOOL_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = strtobool(token)

/* Read a character-string field */
#define READ_STRING_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = nullable_string(token, length)

/* Read a parse location field (and throw away the value, per notes above) */
#define READ_LOCATION_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = -1	/* set field to "unknown" */

/* Read a Node field */
#define READ_NODE_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = nodeRead(NULL, 0)

/* Read a bitmapset field */
#define READ_BITMAPSET_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = _readBitmapset()

#ifdef XCP
/* Read fields of a Plan node */
#define READ_PLAN_FIELDS(nodeTypeName) \
	Plan *plan_node; \
	READ_LOCALS(nodeTypeName); \
	plan_node = (Plan *) local_node; \
	token = pg_strtok(&length);		/* skip :startup_cost */ \
	token = pg_strtok(&length);		/* get field value */ \
	plan_node->startup_cost = atof(token); \
	token = pg_strtok(&length);		/* skip :total_cost */ \
	token = pg_strtok(&length);		/* get field value */ \
	plan_node->total_cost = atof(token); \
	token = pg_strtok(&length);		/* skip :plan_rows */ \
	token = pg_strtok(&length);		/* get field value */ \
	plan_node->plan_rows = atof(token); \
	token = pg_strtok(&length);		/* skip :plan_width */ \
	token = pg_strtok(&length);		/* get field value */ \
	plan_node->plan_width = atoi(token); \
	token = pg_strtok(&length);		/* skip :targetlist */ \
	plan_node->targetlist = nodeRead(NULL, 0); \
	token = pg_strtok(&length);		/* skip :qual */ \
	plan_node->qual = nodeRead(NULL, 0); \
	token = pg_strtok(&length);		/* skip :lefttree */ \
	plan_node->lefttree = nodeRead(NULL, 0); \
	token = pg_strtok(&length);		/* skip :righttree */ \
	plan_node->righttree = nodeRead(NULL, 0); \
	token = pg_strtok(&length);		/* skip :initPlan */ \
	plan_node->initPlan = nodeRead(NULL, 0); \
	token = pg_strtok(&length);		/* skip :extParam */ \
	plan_node->extParam = _readBitmapset(); \
	token = pg_strtok(&length);		/* skip :allParam */ \
	plan_node->allParam = _readBitmapset()

/* Read fields of a Scan node */
#define READ_SCAN_FIELDS(nodeTypeName) \
	Scan *scan_node; \
	READ_PLAN_FIELDS(nodeTypeName); \
	scan_node = (Scan *) local_node; \
	token = pg_strtok(&length);		/* skip :scanrelid */ \
	token = pg_strtok(&length);		/* get field value */ \
	scan_node->scanrelid = atoi(token)

/* Read fields of a Join node */
#define READ_JOIN_FIELDS(nodeTypeName) \
	Join *join_node; \
	READ_PLAN_FIELDS(nodeTypeName); \
	join_node = (Join *) local_node; \
	token = pg_strtok(&length);		/* skip :jointype */ \
	token = pg_strtok(&length);		/* get field value */ \
	join_node->jointype = (JoinType) atoi(token); \
	token = pg_strtok(&length);		/* skip :joinqual */ \
	join_node->joinqual = nodeRead(NULL, 0)

/*
 * Macros to read an identifier and lookup the OID
 * The identifier depends on object type.
 */
#define NSP_OID(nspname) LookupNamespaceNoError(nspname)

/* Read relation identifier and lookup the OID */
#define READ_RELID_FIELD(fldname) \
	do { \
		char	   *nspname; /* namespace name */ \
		char	   *relname; /* relation name */ \
		token = pg_strtok(&length);		/* skip :fldname */ \
		token = pg_strtok(&length); /* get nspname */ \
		nspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get relname */ \
		relname = nullable_string(token, length); \
		if (relname) \
			local_node->fldname = get_relname_relid(relname, \
													NSP_OID(nspname)); \
		else \
			local_node->fldname = InvalidOid; \
	} while (0)

/* Read data type identifier and lookup the OID */
#define READ_TYPID_FIELD(fldname) \
	do { \
		char	   *nspname; /* namespace name */ \
		char	   *typname; /* data type name */ \
		token = pg_strtok(&length);		/* skip :fldname */ \
		token = pg_strtok(&length); /* get nspname */ \
		nspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get typname */ \
		typname = nullable_string(token, length); \
		if (typname) \
			local_node->fldname = get_typname_typid(typname, \
													NSP_OID(nspname)); \
		else \
			local_node->fldname = InvalidOid; \
	} while (0)

/* Read function identifier and lookup the OID */
#define READ_FUNCID_FIELD(fldname) \
	do { \
		char       *nspname; /* namespace name */ \
		char       *funcname; /* function name */ \
		int 		nargs; /* number of arguments */ \
		Oid		   *argtypes; /* argument types */ \
		token = pg_strtok(&length);		/* skip :fldname */ \
		token = pg_strtok(&length); /* get nspname */ \
		nspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get funcname */ \
		funcname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get nargs */ \
		nargs = atoi(token); \
		if (funcname) \
		{ \
			int	i; \
			argtypes = palloc(nargs * sizeof(Oid)); \
			for (i = 0; i < nargs; i++) \
			{ \
				char *typnspname; /* argument type namespace */ \
				char *typname; /* argument type name */ \
				token = pg_strtok(&length); /* get type nspname */ \
				typnspname = nullable_string(token, length); \
				token = pg_strtok(&length); /* get type name */ \
				typname = nullable_string(token, length); \
				argtypes[i] = get_typname_typid(typname, \
												NSP_OID(typnspname)); \
			} \
			local_node->fldname = get_funcid(funcname, \
											 buildoidvector(argtypes, nargs), \
											 NSP_OID(nspname)); \
		} \
		else \
			local_node->fldname = InvalidOid; \
	} while (0)

/* Read operator identifier and lookup the OID */
#define READ_OPERID_FIELD(fldname) \
	do { \
		char       *nspname; /* namespace name */ \
		char       *oprname; /* operator name */ \
		char	   *leftnspname; /* left type namespace */ \
		char	   *leftname; /* left type name */ \
		Oid			oprleft; /* left type */ \
		char	   *rightnspname; /* right type namespace */ \
		char	   *rightname; /* right type name */ \
		Oid			oprright; /* right type */ \
		token = pg_strtok(&length);		/* skip :fldname */ \
		token = pg_strtok(&length); /* get nspname */ \
		nspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get operator name */ \
		oprname = nullable_string(token, length); \
		token = pg_strtok(&length); /* left type namespace */ \
		leftnspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* left type name */ \
		leftname = nullable_string(token, length); \
		token = pg_strtok(&length); /* right type namespace */ \
		rightnspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* right type name */ \
		rightname = nullable_string(token, length); \
		if (oprname) \
		{ \
			if (leftname) \
				oprleft = get_typname_typid(leftname, \
											NSP_OID(leftnspname)); \
			else \
				oprleft = InvalidOid; \
			if (rightname) \
				oprright = get_typname_typid(rightname, \
											 NSP_OID(rightnspname)); \
			else \
				oprright = InvalidOid; \
			local_node->fldname = get_operid(oprname, \
											 oprleft, \
											 oprright, \
											 NSP_OID(nspname)); \
		} \
		else \
			local_node->fldname = InvalidOid; \
	} while (0)

/* Read collation identifier and lookup the OID */
#define READ_COLLID_FIELD(fldname) \
	do { \
		char       *nspname; /* namespace name */ \
		char       *collname; /* collation name */ \
		int 		collencoding; /* collation encoding */ \
		token = pg_strtok(&length);		/* skip :fldname */ \
		token = pg_strtok(&length); /* get nspname */ \
		nspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get collname */ \
		collname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get collencoding */ \
		collencoding = atoi(token); \
		if (collname) \
			local_node->fldname = get_collid(collname, \
											 collencoding, \
											 NSP_OID(nspname)); \
		else \
			local_node->fldname = InvalidOid; \
	} while (0)
#endif

/* Routine exit */
#define READ_DONE() \
	return local_node


/*
 * NOTE: use atoi() to read values written with %d, or atoui() to read
 * values written with %u in outfuncs.c.  An exception is OID values,
 * for which use atooid().	(As of 7.1, outfuncs.c writes OIDs as %u,
 * but this will probably change in the future.)
 */
#define atoui(x)  ((unsigned int) strtoul((x), NULL, 10))

#define atooid(x)  ((Oid) strtoul((x), NULL, 10))

#define strtobool(x)  ((*(x) == 't') ? true : false)

#define nullable_string(token,length)  \
	((length) == 0 ? NULL : debackslash(token, length))


static Datum readDatum(bool typbyval);
#ifdef XCP
static Datum scanDatum(Oid typid, int typmod);
#endif

/*
 * _readBitmapset
 */
static Bitmapset *
_readBitmapset(void)
{
	Bitmapset  *result = NULL;

	READ_TEMP_LOCALS();

	token = pg_strtok(&length);
	if (token == NULL)
		elog(ERROR, "incomplete Bitmapset structure");
	if (length != 1 || token[0] != '(')
		elog(ERROR, "unrecognized token: \"%.*s\"", length, token);

	token = pg_strtok(&length);
	if (token == NULL)
		elog(ERROR, "incomplete Bitmapset structure");
	if (length != 1 || token[0] != 'b')
		elog(ERROR, "unrecognized token: \"%.*s\"", length, token);

	for (;;)
	{
		int			val;
		char	   *endptr;

		token = pg_strtok(&length);
		if (token == NULL)
			elog(ERROR, "unterminated Bitmapset structure");
		if (length == 1 && token[0] == ')')
			break;
		val = (int) strtol(token, &endptr, 10);
		if (endptr != token + length)
			elog(ERROR, "unrecognized integer: \"%.*s\"", length, token);
		result = bms_add_member(result, val);
	}

	return result;
}


/*
 * _readQuery
 */
static Query *
_readQuery(void)
{
	READ_LOCALS(Query);

	READ_ENUM_FIELD(commandType, CmdType);
	READ_ENUM_FIELD(querySource, QuerySource);
	local_node->queryId = 0;	/* not saved in output format */
	READ_BOOL_FIELD(canSetTag);
	READ_NODE_FIELD(utilityStmt);
	READ_INT_FIELD(resultRelation);
	READ_BOOL_FIELD(hasAggs);
	READ_BOOL_FIELD(hasWindowFuncs);
	READ_BOOL_FIELD(hasSubLinks);
	READ_BOOL_FIELD(hasDistinctOn);
	READ_BOOL_FIELD(hasRecursive);
	READ_BOOL_FIELD(hasModifyingCTE);
	READ_BOOL_FIELD(hasForUpdate);
	READ_NODE_FIELD(cteList);
	READ_NODE_FIELD(rtable);
	READ_NODE_FIELD(jointree);
	READ_NODE_FIELD(targetList);
	READ_NODE_FIELD(returningList);
	READ_NODE_FIELD(groupClause);
	READ_NODE_FIELD(havingQual);
	READ_NODE_FIELD(windowClause);
	READ_NODE_FIELD(distinctClause);
	READ_NODE_FIELD(sortClause);
	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);
	READ_NODE_FIELD(rowMarks);
	READ_NODE_FIELD(setOperations);
	READ_NODE_FIELD(constraintDeps);

	READ_DONE();
}

/*
 * _readNotifyStmt
 */
static NotifyStmt *
_readNotifyStmt(void)
{
	READ_LOCALS(NotifyStmt);

	READ_STRING_FIELD(conditionname);
	READ_STRING_FIELD(payload);

	READ_DONE();
}

/*
 * _readDeclareCursorStmt
 */
static DeclareCursorStmt *
_readDeclareCursorStmt(void)
{
	READ_LOCALS(DeclareCursorStmt);

	READ_STRING_FIELD(portalname);
	READ_INT_FIELD(options);
	READ_NODE_FIELD(query);

	READ_DONE();
}

/*
 * _readSortGroupClause
 */
static SortGroupClause *
_readSortGroupClause(void)
{
	READ_LOCALS(SortGroupClause);

	READ_UINT_FIELD(tleSortGroupRef);
#ifdef XCP
	if (portable_input)
		READ_OPERID_FIELD(eqop);
	else
#endif
	READ_OID_FIELD(eqop);
#ifdef XCP
	if (portable_input)
		READ_OPERID_FIELD(sortop);
	else
#endif
	READ_OID_FIELD(sortop);
	READ_BOOL_FIELD(nulls_first);
	READ_BOOL_FIELD(hashable);

	READ_DONE();
}

/*
 * _readWindowClause
 */
static WindowClause *
_readWindowClause(void)
{
	READ_LOCALS(WindowClause);

	READ_STRING_FIELD(name);
	READ_STRING_FIELD(refname);
	READ_NODE_FIELD(partitionClause);
	READ_NODE_FIELD(orderClause);
	READ_INT_FIELD(frameOptions);
	READ_NODE_FIELD(startOffset);
	READ_NODE_FIELD(endOffset);
	READ_UINT_FIELD(winref);
	READ_BOOL_FIELD(copiedOrder);

	READ_DONE();
}

/*
 * _readRowMarkClause
 */
static RowMarkClause *
_readRowMarkClause(void)
{
	READ_LOCALS(RowMarkClause);

	READ_UINT_FIELD(rti);
	READ_BOOL_FIELD(forUpdate);
	READ_BOOL_FIELD(noWait);
	READ_BOOL_FIELD(pushedDown);

	READ_DONE();
}

/*
 * _readCommonTableExpr
 */
static CommonTableExpr *
_readCommonTableExpr(void)
{
	READ_LOCALS(CommonTableExpr);

	READ_STRING_FIELD(ctename);
	READ_NODE_FIELD(aliascolnames);
	READ_NODE_FIELD(ctequery);
	READ_LOCATION_FIELD(location);
	READ_BOOL_FIELD(cterecursive);
	READ_INT_FIELD(cterefcount);
	READ_NODE_FIELD(ctecolnames);
	READ_NODE_FIELD(ctecoltypes);
	READ_NODE_FIELD(ctecoltypmods);
	READ_NODE_FIELD(ctecolcollations);

	READ_DONE();
}

/*
 * _readSetOperationStmt
 */
static SetOperationStmt *
_readSetOperationStmt(void)
{
	READ_LOCALS(SetOperationStmt);

	READ_ENUM_FIELD(op, SetOperation);
	READ_BOOL_FIELD(all);
	READ_NODE_FIELD(larg);
	READ_NODE_FIELD(rarg);
	READ_NODE_FIELD(colTypes);
	READ_NODE_FIELD(colTypmods);
	READ_NODE_FIELD(colCollations);
	READ_NODE_FIELD(groupClauses);

	READ_DONE();
}


/*
 *	Stuff from primnodes.h.
 */

static Alias *
_readAlias(void)
{
	READ_LOCALS(Alias);

	READ_STRING_FIELD(aliasname);
	READ_NODE_FIELD(colnames);

	READ_DONE();
}

static RangeVar *
_readRangeVar(void)
{
	READ_LOCALS(RangeVar);

	local_node->catalogname = NULL;		/* not currently saved in output
										 * format */

	READ_STRING_FIELD(schemaname);
	READ_STRING_FIELD(relname);
	READ_ENUM_FIELD(inhOpt, InhOption);
	READ_CHAR_FIELD(relpersistence);
	READ_NODE_FIELD(alias);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static IntoClause *
_readIntoClause(void)
{
	READ_LOCALS(IntoClause);

	READ_NODE_FIELD(rel);
	READ_NODE_FIELD(colNames);
	READ_NODE_FIELD(options);
	READ_ENUM_FIELD(onCommit, OnCommitAction);
	READ_STRING_FIELD(tableSpaceName);
	READ_BOOL_FIELD(skipData);

	READ_DONE();
}

/*
 * _readVar
 */
static Var *
_readVar(void)
{
	READ_LOCALS(Var);

	READ_UINT_FIELD(varno);
	READ_INT_FIELD(varattno);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(vartype);
	else
#endif
	READ_OID_FIELD(vartype);
	READ_INT_FIELD(vartypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(varcollid);
	else
#endif
	READ_OID_FIELD(varcollid);
	READ_UINT_FIELD(varlevelsup);
	READ_UINT_FIELD(varnoold);
	READ_INT_FIELD(varoattno);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readConst
 */
static Const *
_readConst(void)
{
	READ_LOCALS(Const);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(consttype);
	else
#endif
	READ_OID_FIELD(consttype);
	READ_INT_FIELD(consttypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(constcollid);
	else
#endif
	READ_OID_FIELD(constcollid);
	READ_INT_FIELD(constlen);
	READ_BOOL_FIELD(constbyval);
	READ_BOOL_FIELD(constisnull);
	READ_LOCATION_FIELD(location);

	token = pg_strtok(&length); /* skip :constvalue */
	if (local_node->constisnull)
		token = pg_strtok(&length);		/* skip "<>" */
	else
#ifdef XCP
		if (portable_input)
			local_node->constvalue = scanDatum(local_node->consttype,
											   local_node->consttypmod);
		else
#endif
		local_node->constvalue = readDatum(local_node->constbyval);

	READ_DONE();
}

/*
 * _readParam
 */
static Param *
_readParam(void)
{
	READ_LOCALS(Param);

	READ_ENUM_FIELD(paramkind, ParamKind);
	READ_INT_FIELD(paramid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(paramtype);
	else
#endif
	READ_OID_FIELD(paramtype);
	READ_INT_FIELD(paramtypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(paramcollid);
	else
#endif
	READ_OID_FIELD(paramcollid);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readAggref
 */
static Aggref *
_readAggref(void)
{
	READ_LOCALS(Aggref);

#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(aggfnoid);
	else
#endif
	READ_OID_FIELD(aggfnoid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(aggtype);
	else
#endif
	READ_OID_FIELD(aggtype);
#ifdef PGXC
#ifndef XCP
	READ_OID_FIELD(aggtrantype);
	READ_BOOL_FIELD(agghas_collectfn);
#endif /* XCP */
#endif /* PGXC */
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(aggcollid);
	else
#endif
	READ_OID_FIELD(aggcollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(aggorder);
	READ_NODE_FIELD(aggdistinct);
	READ_BOOL_FIELD(aggstar);
	READ_UINT_FIELD(agglevelsup);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readWindowFunc
 */
static WindowFunc *
_readWindowFunc(void)
{
	READ_LOCALS(WindowFunc);

#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(winfnoid);
	else
#endif
	READ_OID_FIELD(winfnoid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(wintype);
	else
#endif
	READ_OID_FIELD(wintype);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(wincollid);
	else
#endif
	READ_OID_FIELD(wincollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_UINT_FIELD(winref);
	READ_BOOL_FIELD(winstar);
	READ_BOOL_FIELD(winagg);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readArrayRef
 */
static ArrayRef *
_readArrayRef(void)
{
	READ_LOCALS(ArrayRef);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(refarraytype);
	else
#endif
	READ_OID_FIELD(refarraytype);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(refelemtype);
	else
#endif
	READ_OID_FIELD(refelemtype);
	READ_INT_FIELD(reftypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(refcollid);
	else
#endif
	READ_OID_FIELD(refcollid);
	READ_NODE_FIELD(refupperindexpr);
	READ_NODE_FIELD(reflowerindexpr);
	READ_NODE_FIELD(refexpr);
	READ_NODE_FIELD(refassgnexpr);

	READ_DONE();
}

/*
 * _readFuncExpr
 */
static FuncExpr *
_readFuncExpr(void)
{
	READ_LOCALS(FuncExpr);

#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(funcid);
	else
#endif
	READ_OID_FIELD(funcid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(funcresulttype);
	else
#endif
	READ_OID_FIELD(funcresulttype);
	READ_BOOL_FIELD(funcretset);
	READ_ENUM_FIELD(funcformat, CoercionForm);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(funccollid);
	else
#endif
	READ_OID_FIELD(funccollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNamedArgExpr
 */
static NamedArgExpr *
_readNamedArgExpr(void)
{
	READ_LOCALS(NamedArgExpr);

	READ_NODE_FIELD(arg);
	READ_STRING_FIELD(name);
	READ_INT_FIELD(argnumber);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readOpExpr
 */
static OpExpr *
_readOpExpr(void)
{
	READ_LOCALS(OpExpr);

#ifdef XCP
	if (portable_input)
		READ_OPERID_FIELD(opno);
	else
#endif
	READ_OID_FIELD(opno);
#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(opfuncid);
	else
#endif
	READ_OID_FIELD(opfuncid);

#ifndef XCP
	/*
	 * The opfuncid is stored in the textual format primarily for debugging
	 * and documentation reasons.  We want to always read it as zero to force
	 * it to be re-looked-up in the pg_operator entry.	This ensures that
	 * stored rules don't have hidden dependencies on operators' functions.
	 * (We don't currently support an ALTER OPERATOR command, but might
	 * someday.)
	 */
	local_node->opfuncid = InvalidOid;
#endif

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(opresulttype);
	else
#endif
	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(opcollid);
	else
#endif
	READ_OID_FIELD(opcollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readDistinctExpr
 */
static DistinctExpr *
_readDistinctExpr(void)
{
	READ_LOCALS(DistinctExpr);

#ifdef XCP
	if (portable_input)
		READ_OPERID_FIELD(opno);
	else
#endif
	READ_OID_FIELD(opno);
#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(opfuncid);
	else
#endif
	READ_OID_FIELD(opfuncid);

#ifndef XCP
	/*
	 * The opfuncid is stored in the textual format primarily for debugging
	 * and documentation reasons.  We want to always read it as zero to force
	 * it to be re-looked-up in the pg_operator entry.	This ensures that
	 * stored rules don't have hidden dependencies on operators' functions.
	 * (We don't currently support an ALTER OPERATOR command, but might
	 * someday.)
	 */
	local_node->opfuncid = InvalidOid;
#endif

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(opresulttype);
	else
#endif
	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(opcollid);
	else
#endif
	READ_OID_FIELD(opcollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNullIfExpr
 */
static NullIfExpr *
_readNullIfExpr(void)
{
	READ_LOCALS(NullIfExpr);

#ifdef XCP
	if (portable_input)
		READ_OPERID_FIELD(opno);
	else
#endif
	READ_OID_FIELD(opno);
#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(opfuncid);
	else
#endif
	READ_OID_FIELD(opfuncid);

	/*
	 * The opfuncid is stored in the textual format primarily for debugging
	 * and documentation reasons.  We want to always read it as zero to force
	 * it to be re-looked-up in the pg_operator entry.	This ensures that
	 * stored rules don't have hidden dependencies on operators' functions.
	 * (We don't currently support an ALTER OPERATOR command, but might
	 * someday.)
	 */
#ifdef XCP
	/* Do not invalidate if we have just looked up the value */
	if (!portable_input)
#endif
	local_node->opfuncid = InvalidOid;

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(opresulttype);
	else
#endif
	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(opcollid);
	else
#endif
	READ_OID_FIELD(opcollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readScalarArrayOpExpr
 */
static ScalarArrayOpExpr *
_readScalarArrayOpExpr(void)
{
	READ_LOCALS(ScalarArrayOpExpr);

#ifdef XCP
	if (portable_input)
		READ_OPERID_FIELD(opno);
	else
#endif
	READ_OID_FIELD(opno);
#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(opfuncid);
	else
#endif
	READ_OID_FIELD(opfuncid);
#ifndef XCP
	/*
	 * The opfuncid is stored in the textual format primarily for debugging
	 * and documentation reasons.  We want to always read it as zero to force
	 * it to be re-looked-up in the pg_operator entry.	This ensures that
	 * stored rules don't have hidden dependencies on operators' functions.
	 * (We don't currently support an ALTER OPERATOR command, but might
	 * someday.)
	 */
	local_node->opfuncid = InvalidOid;
#endif

	READ_BOOL_FIELD(useOr);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readBoolExpr
 */
static BoolExpr *
_readBoolExpr(void)
{
	READ_LOCALS(BoolExpr);

	/* do-it-yourself enum representation */
	token = pg_strtok(&length); /* skip :boolop */
	token = pg_strtok(&length); /* get field value */
	if (strncmp(token, "and", 3) == 0)
		local_node->boolop = AND_EXPR;
	else if (strncmp(token, "or", 2) == 0)
		local_node->boolop = OR_EXPR;
	else if (strncmp(token, "not", 3) == 0)
		local_node->boolop = NOT_EXPR;
	else
		elog(ERROR, "unrecognized boolop \"%.*s\"", length, token);

	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readSubLink
 */
static SubLink *
_readSubLink(void)
{
	READ_LOCALS(SubLink);

	READ_ENUM_FIELD(subLinkType, SubLinkType);
	READ_NODE_FIELD(testexpr);
	READ_NODE_FIELD(operName);
	READ_NODE_FIELD(subselect);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

#ifdef XCP
/*
 * _readSubPlan is not needed since it doesn't appear in stored rules.
 */
static SubPlan *
_readSubPlan(void)
{
	READ_LOCALS(SubPlan);

	READ_ENUM_FIELD(subLinkType, SubLinkType);
	READ_NODE_FIELD(testexpr);
	READ_NODE_FIELD(paramIds);
	READ_INT_FIELD(plan_id);
	READ_STRING_FIELD(plan_name);
	if (portable_input)
		READ_TYPID_FIELD(firstColType);
	else
		READ_OID_FIELD(firstColType);
	READ_INT_FIELD(firstColTypmod);
	if (portable_input)
		READ_COLLID_FIELD(firstColCollation);
	else
		READ_OID_FIELD(firstColCollation);
	READ_BOOL_FIELD(useHashTable);
	READ_BOOL_FIELD(unknownEqFalse);
	READ_NODE_FIELD(setParam);
	READ_NODE_FIELD(parParam);
	READ_NODE_FIELD(args);
	READ_FLOAT_FIELD(startup_cost);
	READ_FLOAT_FIELD(per_call_cost);

	READ_DONE();
}
#endif

/*
 * _readFieldSelect
 */
static FieldSelect *
_readFieldSelect(void)
{
	READ_LOCALS(FieldSelect);

	READ_NODE_FIELD(arg);
	READ_INT_FIELD(fieldnum);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(resultcollid);
	else
#endif
	READ_OID_FIELD(resultcollid);

	READ_DONE();
}

/*
 * _readFieldStore
 */
static FieldStore *
_readFieldStore(void)
{
	READ_LOCALS(FieldStore);

	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(newvals);
	READ_NODE_FIELD(fieldnums);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);

	READ_DONE();
}

/*
 * _readRelabelType
 */
static RelabelType *
_readRelabelType(void)
{
	READ_LOCALS(RelabelType);

	READ_NODE_FIELD(arg);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(resultcollid);
	else
#endif
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(relabelformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCoerceViaIO
 */
static CoerceViaIO *
_readCoerceViaIO(void)
{
	READ_LOCALS(CoerceViaIO);

	READ_NODE_FIELD(arg);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(resultcollid);
	else
#endif
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(coerceformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readArrayCoerceExpr
 */
static ArrayCoerceExpr *
_readArrayCoerceExpr(void)
{
	READ_LOCALS(ArrayCoerceExpr);

	READ_NODE_FIELD(arg);
#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(elemfuncid);
	else
#endif
	READ_OID_FIELD(elemfuncid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(resultcollid);
	else
#endif
	READ_OID_FIELD(resultcollid);
	READ_BOOL_FIELD(isExplicit);
	READ_ENUM_FIELD(coerceformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readConvertRowtypeExpr
 */
static ConvertRowtypeExpr *
_readConvertRowtypeExpr(void)
{
	READ_LOCALS(ConvertRowtypeExpr);

	READ_NODE_FIELD(arg);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);
	READ_ENUM_FIELD(convertformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCollateExpr
 */
static CollateExpr *
_readCollateExpr(void)
{
	READ_LOCALS(CollateExpr);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(collOid);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCaseExpr
 */
static CaseExpr *
_readCaseExpr(void)
{
	READ_LOCALS(CaseExpr);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(casetype);
	else
#endif
	READ_OID_FIELD(casetype);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(casecollid);
	else
#endif
	READ_OID_FIELD(casecollid);
	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(defresult);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCaseWhen
 */
static CaseWhen *
_readCaseWhen(void)
{
	READ_LOCALS(CaseWhen);

	READ_NODE_FIELD(expr);
	READ_NODE_FIELD(result);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCaseTestExpr
 */
static CaseTestExpr *
_readCaseTestExpr(void)
{
	READ_LOCALS(CaseTestExpr);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(typeId);
	else
#endif
	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(collation);
	else
#endif
	READ_OID_FIELD(collation);

	READ_DONE();
}

/*
 * _readArrayExpr
 */
static ArrayExpr *
_readArrayExpr(void)
{
	READ_LOCALS(ArrayExpr);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(array_typeid);
	else
#endif
	READ_OID_FIELD(array_typeid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(array_collid);
	else
#endif
	READ_OID_FIELD(array_collid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(element_typeid);
	else
#endif
	READ_OID_FIELD(element_typeid);
	READ_NODE_FIELD(elements);
	READ_BOOL_FIELD(multidims);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readRowExpr
 */
static RowExpr *
_readRowExpr(void)
{
	READ_LOCALS(RowExpr);

	READ_NODE_FIELD(args);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(row_typeid);
	else
#endif
	READ_OID_FIELD(row_typeid);
	READ_ENUM_FIELD(row_format, CoercionForm);
	READ_NODE_FIELD(colnames);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readRowCompareExpr
 */
static RowCompareExpr *
_readRowCompareExpr(void)
{
	READ_LOCALS(RowCompareExpr);

	READ_ENUM_FIELD(rctype, RowCompareType);
	READ_NODE_FIELD(opnos);
	READ_NODE_FIELD(opfamilies);
	READ_NODE_FIELD(inputcollids);
	READ_NODE_FIELD(largs);
	READ_NODE_FIELD(rargs);

	READ_DONE();
}

/*
 * _readCoalesceExpr
 */
static CoalesceExpr *
_readCoalesceExpr(void)
{
	READ_LOCALS(CoalesceExpr);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(coalescetype);
	else
#endif
	READ_OID_FIELD(coalescetype);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(coalescecollid);
	else
#endif
	READ_OID_FIELD(coalescecollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readMinMaxExpr
 */
static MinMaxExpr *
_readMinMaxExpr(void)
{
	READ_LOCALS(MinMaxExpr);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(minmaxtype);
	else
#endif
	READ_OID_FIELD(minmaxtype);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(minmaxcollid);
	else
#endif
	READ_OID_FIELD(minmaxcollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_ENUM_FIELD(op, MinMaxOp);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readXmlExpr
 */
static XmlExpr *
_readXmlExpr(void)
{
	READ_LOCALS(XmlExpr);

	READ_ENUM_FIELD(op, XmlExprOp);
	READ_STRING_FIELD(name);
	READ_NODE_FIELD(named_args);
	READ_NODE_FIELD(arg_names);
	READ_NODE_FIELD(args);
	READ_ENUM_FIELD(xmloption, XmlOptionType);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(type);
	else
#endif
	READ_OID_FIELD(type);
	READ_INT_FIELD(typmod);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNullTest
 */
static NullTest *
_readNullTest(void)
{
	READ_LOCALS(NullTest);

	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(nulltesttype, NullTestType);
	READ_BOOL_FIELD(argisrow);

	READ_DONE();
}

/*
 * _readBooleanTest
 */
static BooleanTest *
_readBooleanTest(void)
{
	READ_LOCALS(BooleanTest);

	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(booltesttype, BoolTestType);

	READ_DONE();
}

/*
 * _readCoerceToDomain
 */
static CoerceToDomain *
_readCoerceToDomain(void)
{
	READ_LOCALS(CoerceToDomain);

	READ_NODE_FIELD(arg);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(resultcollid);
	else
#endif
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(coercionformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCoerceToDomainValue
 */
static CoerceToDomainValue *
_readCoerceToDomainValue(void)
{
	READ_LOCALS(CoerceToDomainValue);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(typeId);
	else
#endif
	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(collation);
	else
#endif
	READ_OID_FIELD(collation);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readSetToDefault
 */
static SetToDefault *
_readSetToDefault(void)
{
	READ_LOCALS(SetToDefault);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(typeId);
	else
#endif
	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(collation);
	else
#endif
	READ_OID_FIELD(collation);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCurrentOfExpr
 */
static CurrentOfExpr *
_readCurrentOfExpr(void)
{
	READ_LOCALS(CurrentOfExpr);

	READ_UINT_FIELD(cvarno);
	READ_STRING_FIELD(cursor_name);
	READ_INT_FIELD(cursor_param);

	READ_DONE();
}

/*
 * _readTargetEntry
 */
static TargetEntry *
_readTargetEntry(void)
{
	READ_LOCALS(TargetEntry);

	READ_NODE_FIELD(expr);
	READ_INT_FIELD(resno);
	READ_STRING_FIELD(resname);
	READ_UINT_FIELD(ressortgroupref);
#ifdef XCP
	if (portable_input)
		READ_RELID_FIELD(resorigtbl);
	else
#endif
	READ_OID_FIELD(resorigtbl);
	READ_INT_FIELD(resorigcol);
	READ_BOOL_FIELD(resjunk);

	READ_DONE();
}

/*
 * _readRangeTblRef
 */
static RangeTblRef *
_readRangeTblRef(void)
{
	READ_LOCALS(RangeTblRef);

	READ_INT_FIELD(rtindex);

	READ_DONE();
}

/*
 * _readJoinExpr
 */
static JoinExpr *
_readJoinExpr(void)
{
	READ_LOCALS(JoinExpr);

	READ_ENUM_FIELD(jointype, JoinType);
	READ_BOOL_FIELD(isNatural);
	READ_NODE_FIELD(larg);
	READ_NODE_FIELD(rarg);
	READ_NODE_FIELD(usingClause);
	READ_NODE_FIELD(quals);
	READ_NODE_FIELD(alias);
	READ_INT_FIELD(rtindex);

	READ_DONE();
}

/*
 * _readFromExpr
 */
static FromExpr *
_readFromExpr(void)
{
	READ_LOCALS(FromExpr);

	READ_NODE_FIELD(fromlist);
	READ_NODE_FIELD(quals);

	READ_DONE();
}


/*
 *	Stuff from parsenodes.h.
 */

/*
 * _readRangeTblEntry
 */
static RangeTblEntry *
_readRangeTblEntry(void)
{
	READ_LOCALS(RangeTblEntry);

	/* put alias + eref first to make dump more legible */
	READ_NODE_FIELD(alias);
	READ_NODE_FIELD(eref);
	READ_ENUM_FIELD(rtekind, RTEKind);
#ifdef PGXC
#ifndef XCP
	READ_STRING_FIELD(relname);
#endif
#endif

	switch (local_node->rtekind)
	{
		case RTE_RELATION:
#ifdef XCP
			if (portable_input)
				READ_RELID_FIELD(relid);
			else
#endif
			READ_OID_FIELD(relid);
			READ_CHAR_FIELD(relkind);
			break;
		case RTE_SUBQUERY:
			READ_NODE_FIELD(subquery);
			READ_BOOL_FIELD(security_barrier);
			break;
		case RTE_JOIN:
			READ_ENUM_FIELD(jointype, JoinType);
			READ_NODE_FIELD(joinaliasvars);
			break;
		case RTE_FUNCTION:
			READ_NODE_FIELD(funcexpr);
			READ_NODE_FIELD(funccoltypes);
			READ_NODE_FIELD(funccoltypmods);
			READ_NODE_FIELD(funccolcollations);
			break;
		case RTE_VALUES:
			READ_NODE_FIELD(values_lists);
			READ_NODE_FIELD(values_collations);
			break;
		case RTE_CTE:
			READ_STRING_FIELD(ctename);
			READ_UINT_FIELD(ctelevelsup);
			READ_BOOL_FIELD(self_reference);
			READ_NODE_FIELD(ctecoltypes);
			READ_NODE_FIELD(ctecoltypmods);
			READ_NODE_FIELD(ctecolcollations);
			break;
#ifdef PGXC
		case RTE_REMOTE_DUMMY:
			/* Nothing to do */
			break;
#endif /* PGXC */
		default:
			elog(ERROR, "unrecognized RTE kind: %d",
				 (int) local_node->rtekind);
			break;
	}

	READ_BOOL_FIELD(inh);
	READ_BOOL_FIELD(inFromCl);
	READ_UINT_FIELD(requiredPerms);
#ifdef XCP
	if (portable_input)
	{
		local_node->requiredPerms = 0; /* no permission checks on data node */
		token = pg_strtok(&length);	/* skip :fldname */ \
		token = pg_strtok(&length);	/* skip field value */ \
		local_node->checkAsUser = InvalidOid;
	}
	else
#endif
	READ_OID_FIELD(checkAsUser);
	READ_BITMAPSET_FIELD(selectedCols);
	READ_BITMAPSET_FIELD(modifiedCols);

	READ_DONE();
}


#ifdef XCP
/*
 * _readPlan
 */
static Plan *
_readPlan(void)
{
	READ_PLAN_FIELDS(Plan);

	READ_DONE();
}



/*
 * _readResult
 */
static Result *
_readResult(void)
{
	READ_PLAN_FIELDS(Result);

	READ_NODE_FIELD(resconstantqual);

	READ_DONE();
}


/*
 * _readModifyTable
 */
static ModifyTable *
_readModifyTable(void)
{
	READ_PLAN_FIELDS(ModifyTable);

	READ_ENUM_FIELD(operation, CmdType);
	READ_BOOL_FIELD(canSetTag);
	READ_NODE_FIELD(resultRelations);
	READ_INT_FIELD(resultRelIndex);
	READ_NODE_FIELD(plans);
	READ_NODE_FIELD(returningLists);
	READ_NODE_FIELD(rowMarks);
	READ_INT_FIELD(epqParam);

	READ_DONE();
}


/*
 * _readAppend
 */
static Append *
_readAppend(void)
{
	READ_PLAN_FIELDS(Append);

	READ_NODE_FIELD(appendplans);

	READ_DONE();
}


/*
 * _readMergeAppend
 */
static MergeAppend *
_readMergeAppend(void)
{
	int i;
	READ_PLAN_FIELDS(MergeAppend);

	READ_NODE_FIELD(mergeplans);
	READ_INT_FIELD(numCols);

	token = pg_strtok(&length);		/* skip :sortColIdx */
	local_node->sortColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->sortColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :sortOperators */
	local_node->sortOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->sortOperators[i] = get_operid(oprname,
 													  oprleft,
													  oprright,
													  NSP_OID(nspname));
		}
		else
		local_node->sortOperators[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :collations */
	local_node->collations = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *collname; /* collation name */
			int 		collencoding; /* collation encoding */
			/* the token is already read */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get collname */
			collname = nullable_string(token, length);
			token = pg_strtok(&length); /* get nargs */
			collencoding = atoi(token);
			if (collname)
				local_node->collations[i] = get_collid(collname,
													   collencoding,
													   NSP_OID(nspname));
			else
				local_node->collations[i] = InvalidOid;
		}
		else
		local_node->collations[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :nullsFirst */
	local_node->nullsFirst = (bool *) palloc(local_node->numCols * sizeof(bool));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->nullsFirst[i] = strtobool(token);
	}

	READ_DONE();
}


/*
 * _readRecursiveUnion
 */
static RecursiveUnion *
_readRecursiveUnion(void)
{
	int i;
	READ_PLAN_FIELDS(RecursiveUnion);

	READ_INT_FIELD(wtParam);
	READ_INT_FIELD(numCols);

	token = pg_strtok(&length);		/* skip :dupColIdx */
	local_node->dupColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->dupColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :dupOperators */
	local_node->dupOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->dupOperators[i] = atooid(token);
	}

	READ_LONG_FIELD(numGroups);

	READ_DONE();
}


/*
 * _readBitmapAnd
 */
static BitmapAnd *
_readBitmapAnd(void)
{
	READ_PLAN_FIELDS(BitmapAnd);

	READ_NODE_FIELD(bitmapplans);

	READ_DONE();
}


/*
 * _readBitmapOr
 */
static BitmapOr *
_readBitmapOr(void)
{
	READ_PLAN_FIELDS(BitmapOr);

	READ_NODE_FIELD(bitmapplans);

	READ_DONE();
}


/*
 * _readScan
 */
static Scan *
_readScan(void)
{
	READ_SCAN_FIELDS(Scan);

	READ_DONE();
}


/*
 * _readSeqScan
 */
static SeqScan *
_readSeqScan(void)
{
	READ_SCAN_FIELDS(SeqScan);

	READ_DONE();
}


/*
 * _readIndexScan
 */
static IndexScan *
_readIndexScan(void)
{
	READ_SCAN_FIELDS(IndexScan);

	if (portable_input)
		READ_RELID_FIELD(indexid);
	else
		READ_OID_FIELD(indexid);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(indexqualorig);
	READ_NODE_FIELD(indexorderby);
	READ_NODE_FIELD(indexorderbyorig);
	READ_ENUM_FIELD(indexorderdir, ScanDirection);

	READ_DONE();
}


/*
 * _readIndexOnlyScan
 */
static IndexOnlyScan *
_readIndexOnlyScan(void)
{
	READ_SCAN_FIELDS(IndexOnlyScan);

	if (portable_input)
		READ_RELID_FIELD(indexid);
	else
		READ_OID_FIELD(indexid);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(indexorderby);
	READ_NODE_FIELD(indextlist);
	READ_ENUM_FIELD(indexorderdir, ScanDirection);

	READ_DONE();
}


/*
 * _readBitmapIndexScan
 */
static BitmapIndexScan *
_readBitmapIndexScan(void)
{
	READ_SCAN_FIELDS(BitmapIndexScan);

	if (portable_input)
		READ_RELID_FIELD(indexid);
	else
		READ_OID_FIELD(indexid);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(indexqualorig);

	READ_DONE();
}


/*
 * _readBitmapHeapScan
 */
static BitmapHeapScan *
_readBitmapHeapScan(void)
{
	READ_SCAN_FIELDS(BitmapHeapScan);

	READ_NODE_FIELD(bitmapqualorig);

	READ_DONE();
}


/*
 * _readTidScan
 */
static TidScan *
_readTidScan(void)
{
	READ_SCAN_FIELDS(TidScan);

	READ_NODE_FIELD(tidquals);

	READ_DONE();
}


/*
 * _readSubqueryScan
 */
static SubqueryScan *
_readSubqueryScan(void)
{
	READ_SCAN_FIELDS(SubqueryScan);

	READ_NODE_FIELD(subplan);

	READ_DONE();
}


/*
 * _readFunctionScan
 */
static FunctionScan *
_readFunctionScan(void)
{
	READ_SCAN_FIELDS(FunctionScan);

	READ_NODE_FIELD(funcexpr);
	READ_NODE_FIELD(funccolnames);
	READ_NODE_FIELD(funccoltypes);
	READ_NODE_FIELD(funccoltypmods);
	READ_NODE_FIELD(funccolcollations);

	READ_DONE();
}


/*
 * _readValuesScan
 */
static ValuesScan *
_readValuesScan(void)
{
	READ_SCAN_FIELDS(ValuesScan);

	READ_NODE_FIELD(values_lists);

	READ_DONE();
}


/*
 * _readCteScan
 */
static CteScan *
_readCteScan(void)
{
	READ_SCAN_FIELDS(CteScan);

	READ_INT_FIELD(ctePlanId);
	READ_INT_FIELD(cteParam);

	READ_DONE();
}


/*
 * _readWorkTableScan
 */
static WorkTableScan *
_readWorkTableScan(void)
{
	READ_SCAN_FIELDS(WorkTableScan);

	READ_INT_FIELD(wtParam);

	READ_DONE();
}


/*
 * _readJoin
 */
static Join *
_readJoin(void)
{
	READ_JOIN_FIELDS(Join);

	READ_DONE();
}


/*
 * _readNestLoop
 */
static NestLoop *
_readNestLoop(void)
{
	READ_JOIN_FIELDS(NestLoop);

	READ_NODE_FIELD(nestParams);

	READ_DONE();
}


/*
 * _readMergeJoin
 */
static MergeJoin *
_readMergeJoin(void)
{
	int			numCols;
	int			i;
	READ_JOIN_FIELDS(MergeJoin);

	READ_NODE_FIELD(mergeclauses);
	numCols = list_length(local_node->mergeclauses);


	token = pg_strtok(&length);		/* skip :mergeFamilies */
	local_node->mergeFamilies = (Oid *) palloc(numCols * sizeof(Oid));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->mergeFamilies[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :mergeCollations */
	local_node->mergeCollations = (Oid *) palloc(numCols * sizeof(Oid));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *collname; /* collation name */
			int 		collencoding; /* collation encoding */
			/* the token is already read */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get collname */
			collname = nullable_string(token, length);
			token = pg_strtok(&length); /* get nargs */
			collencoding = atoi(token);
			if (collname)
				local_node->mergeCollations[i] = get_collid(collname,
															collencoding,
															NSP_OID(nspname));
			else
				local_node->mergeCollations[i] = InvalidOid;
		}
		else
		local_node->mergeCollations[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :mergeStrategies */
	local_node->mergeStrategies = (int *) palloc(numCols * sizeof(int));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->mergeStrategies[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :mergeNullsFirst */
	local_node->mergeNullsFirst = (bool *) palloc(numCols * sizeof(bool));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->mergeNullsFirst[i] = strtobool(token);
	}

	READ_DONE();
}


/*
 * _readHashJoin
 */
static HashJoin *
_readHashJoin(void)
{
	READ_JOIN_FIELDS(HashJoin);

	READ_NODE_FIELD(hashclauses);

	READ_DONE();
}


/*
 * _readMaterial
 */
static Material *
_readMaterial(void)
{
	READ_PLAN_FIELDS(Material);

	READ_DONE();
}


/*
 * _readSort
 */
static Sort *
_readSort(void)
{
	int i;
	READ_PLAN_FIELDS(Sort);

	READ_INT_FIELD(numCols);

	token = pg_strtok(&length);		/* skip :sortColIdx */
	local_node->sortColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->sortColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :sortOperators */
	local_node->sortOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->sortOperators[i] = get_operid(oprname,
 													  oprleft,
													  oprright,
													  NSP_OID(nspname));
		}
		else
		local_node->sortOperators[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :collations */
	local_node->collations = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *collname; /* collation name */
			int 		collencoding; /* collation encoding */
			/* the token is already read */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get collname */
			collname = nullable_string(token, length);
			token = pg_strtok(&length); /* get nargs */
			collencoding = atoi(token);
			if (collname)
				local_node->collations[i] = get_collid(collname,
													   collencoding,
													   NSP_OID(nspname));
			else
				local_node->collations[i] = InvalidOid;
		}
		else
		local_node->collations[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :nullsFirst */
	local_node->nullsFirst = (bool *) palloc(local_node->numCols * sizeof(bool));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->nullsFirst[i] = strtobool(token);
	}

	READ_DONE();
}


/*
 * _readGroup
 */
static Group *
_readGroup(void)
{
	int i;
	READ_PLAN_FIELDS(Group);

	READ_INT_FIELD(numCols);

	token = pg_strtok(&length);		/* skip :grpColIdx */
	local_node->grpColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->grpColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :grpOperators */
	local_node->grpOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->grpOperators[i] = get_operid(oprname,
													 oprleft,
													 oprright,
													 NSP_OID(nspname));
		}
		else
			local_node->grpOperators[i] = atooid(token);
	}

	READ_DONE();
}


/*
 * _readAgg
 */
static Agg *
_readAgg(void)
{
	int i;
	READ_PLAN_FIELDS(Agg);

	READ_ENUM_FIELD(aggstrategy, AggStrategy);
	READ_ENUM_FIELD(aggdistribution, AggDistribution);
	READ_INT_FIELD(numCols);

	token = pg_strtok(&length);		/* skip :grpColIdx */
	local_node->grpColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->grpColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :grpOperators */
	local_node->grpOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->grpOperators[i] = get_operid(oprname,
													 oprleft,
													 oprright,
													 NSP_OID(nspname));
		}
		else
			local_node->grpOperators[i] = atooid(token);
	}

	READ_LONG_FIELD(numGroups);

	READ_DONE();
}


/*
 * _readWindowAgg
 */
static WindowAgg *
_readWindowAgg(void)
{
	int i;
	READ_PLAN_FIELDS(WindowAgg);

	READ_INT_FIELD(winref);
	READ_INT_FIELD(partNumCols);

	token = pg_strtok(&length);		/* skip :partColIdx */
	local_node->partColIdx = (AttrNumber *) palloc(local_node->partNumCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->partNumCols; i++)
	{
		token = pg_strtok(&length);
		local_node->partColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :partOperators */
	local_node->partOperators = (Oid *) palloc(local_node->partNumCols * sizeof(Oid));
	for (i = 0; i < local_node->partNumCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->partOperators[i] = get_operid(oprname,
													  oprleft,
													  oprright,
													  NSP_OID(nspname));
		}
		else
			local_node->partOperators[i] = atooid(token);
	}

	READ_INT_FIELD(ordNumCols);

	token = pg_strtok(&length);		/* skip :ordColIdx */
	local_node->ordColIdx = (AttrNumber *) palloc(local_node->ordNumCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->ordNumCols; i++)
	{
		token = pg_strtok(&length);
		local_node->ordColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :ordOperators */
	local_node->ordOperators = (Oid *) palloc(local_node->ordNumCols * sizeof(Oid));
	for (i = 0; i < local_node->ordNumCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->ordOperators[i] = get_operid(oprname,
													 oprleft,
													 oprright,
													 NSP_OID(nspname));
		}
		else
			local_node->ordOperators[i] = atooid(token);
	}

	READ_INT_FIELD(frameOptions);
	READ_NODE_FIELD(startOffset);
	READ_NODE_FIELD(endOffset);

	READ_DONE();
}


/*
 * _readUnique
 */
static Unique *
_readUnique(void)
{
	int i;
	READ_PLAN_FIELDS(Unique);

	READ_INT_FIELD(numCols);

	token = pg_strtok(&length);		/* skip :uniqColIdx */
	local_node->uniqColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->uniqColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :uniqOperators */
	local_node->uniqOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->uniqOperators[i] = get_operid(oprname,
													  oprleft,
													  oprright,
													  NSP_OID(nspname));
		}
		else
			local_node->uniqOperators[i] = atooid(token);
	}

	READ_DONE();
}


/*
 * _readHash
 */
static Hash *
_readHash(void)
{
	READ_PLAN_FIELDS(Hash);

	if (portable_input)
		READ_RELID_FIELD(skewTable);
	else
		READ_OID_FIELD(skewTable);
	READ_INT_FIELD(skewColumn);
	READ_BOOL_FIELD(skewInherit);
	if (portable_input)
		READ_TYPID_FIELD(skewColType);
	else
		READ_OID_FIELD(skewColType);
	READ_INT_FIELD(skewColTypmod);

	READ_DONE();
}


/*
 * _readSetOp
 */
static SetOp *
_readSetOp(void)
{
	int i;
	READ_PLAN_FIELDS(SetOp);

	READ_ENUM_FIELD(cmd, SetOpCmd);
	READ_ENUM_FIELD(strategy, SetOpStrategy);
	READ_INT_FIELD(numCols);

	token = pg_strtok(&length);		/* skip :dupColIdx */
	local_node->dupColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->dupColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :dupOperators */
	local_node->dupOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->dupOperators[i] = atooid(token);
	}

	READ_INT_FIELD(flagColIdx);
	READ_INT_FIELD(firstFlag);
	READ_LONG_FIELD(numGroups);

	READ_DONE();
}


/*
 * _readLimit
 */
static Limit *
_readLimit(void)
{
	READ_PLAN_FIELDS(Limit);

	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);

	READ_DONE();
}


/*
 * _readRemoteSubplan
 */
static RemoteSubplan *
_readRemoteSubplan(void)
{
	READ_SCAN_FIELDS(RemoteSubplan);

	READ_CHAR_FIELD(distributionType);
	READ_INT_FIELD(distributionKey);
	READ_NODE_FIELD(distributionNodes);
	READ_NODE_FIELD(distributionRestrict);
	READ_NODE_FIELD(nodeList);
	READ_BOOL_FIELD(execOnAll);
	READ_NODE_FIELD(sort);
	READ_STRING_FIELD(cursor);
	READ_INT_FIELD(unique);

	READ_DONE();
}


/*
 * _readRemoteStmt
 */
static RemoteStmt *
_readRemoteStmt(void)
{
	int i;
	READ_LOCALS(RemoteStmt);

	READ_ENUM_FIELD(commandType, CmdType);
	READ_BOOL_FIELD(hasReturning);
	READ_NODE_FIELD(planTree);
	READ_NODE_FIELD(rtable);
	READ_NODE_FIELD(resultRelations);
	READ_NODE_FIELD(subplans);
	READ_INT_FIELD(nParamExec);
	READ_INT_FIELD(nParamRemote);
	if (local_node->nParamRemote > 0)
	{
		local_node->remoteparams = (RemoteParam *) palloc(
				local_node->nParamRemote * sizeof(RemoteParam));
		for (i = 0; i < local_node->nParamRemote; i++)
		{
			RemoteParam *rparam = &(local_node->remoteparams[i]);
			token = pg_strtok(&length); /* skip  :paramkind */
			token = pg_strtok(&length);
			rparam->paramkind = (ParamKind) atoi(token);

			token = pg_strtok(&length); /* skip  :paramid */
			token = pg_strtok(&length);
			rparam->paramid = atoi(token);

			token = pg_strtok(&length); /* skip  :paramtype */
			if (portable_input)
			{
				char	   *nspname; /* namespace name */
				char	   *typname; /* data type name */
				token = pg_strtok(&length); /* get nspname */
				nspname = nullable_string(token, length);
				token = pg_strtok(&length); /* get typname */
				typname = nullable_string(token, length);
				if (typname)
					rparam->paramtype = get_typname_typid(typname,
														  NSP_OID(nspname));
				else
					rparam->paramtype = InvalidOid;
			}
			else
			{
				token = pg_strtok(&length);
				rparam->paramtype = atooid(token);
			}
		}
	}
	else
		local_node->remoteparams = NULL;

	READ_NODE_FIELD(rowMarks);
	READ_CHAR_FIELD(distributionType);
	READ_INT_FIELD(distributionKey);
	READ_NODE_FIELD(distributionNodes);
	READ_NODE_FIELD(distributionRestrict);

	READ_DONE();
}


/*
 * _readSimpleSort
 */
static SimpleSort *
_readSimpleSort(void)
{
	int i;
	READ_LOCALS(SimpleSort);

	READ_INT_FIELD(numCols);

	token = pg_strtok(&length);		/* skip :sortColIdx */
	local_node->sortColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->sortColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :sortOperators */
	local_node->sortOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->sortOperators[i] = get_operid(oprname,
 													  oprleft,
													  oprright,
													  NSP_OID(nspname));
		}
		else
			local_node->sortOperators[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :sortCollations */
	local_node->sortCollations = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *collname; /* collation name */
			int 		collencoding; /* collation encoding */
			/* the token is already read */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get collname */
			collname = nullable_string(token, length);
			token = pg_strtok(&length); /* get nargs */
			collencoding = atoi(token);
			if (collname)
				local_node->sortCollations[i] = get_collid(collname,
													   collencoding,
													   NSP_OID(nspname));
			else
				local_node->sortCollations[i] = InvalidOid;
		}
		else
			local_node->sortCollations[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :nullsFirst */
	local_node->nullsFirst = (bool *) palloc(local_node->numCols * sizeof(bool));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->nullsFirst[i] = strtobool(token);
	}

	READ_DONE();
}


/*
 * _readNestLoopParam
 */
static NestLoopParam *
_readNestLoopParam(void)
{
	READ_LOCALS(NestLoopParam);

	READ_INT_FIELD(paramno);
	READ_NODE_FIELD(paramval);

	READ_DONE();
}


/*
 * _readPlanRowMark
 */
static PlanRowMark *
_readPlanRowMark(void)
{
	READ_LOCALS(PlanRowMark);

	READ_UINT_FIELD(rti);
	READ_UINT_FIELD(prti);
	READ_UINT_FIELD(rowmarkId);
	READ_ENUM_FIELD(markType, RowMarkType);
	READ_BOOL_FIELD(noWait);
	READ_BOOL_FIELD(isParent);

	READ_DONE();
}

/*
 * _readLockRows
 */
static LockRows *
_readLockRows(void)
{
	READ_PLAN_FIELDS(LockRows);

	READ_NODE_FIELD(rowMarks);
	READ_INT_FIELD(epqParam);

	READ_DONE();
}

#endif /* XCP */


/*
 * parseNodeString
 *
 * Given a character string representing a node tree, parseNodeString creates
 * the internal node structure.
 *
 * The string to be read must already have been loaded into pg_strtok().
 */
Node *
parseNodeString(void)
{
	void	   *return_value;

	READ_TEMP_LOCALS();

	token = pg_strtok(&length);

#define MATCH(tokname, namelen) \
	(length == namelen && memcmp(token, tokname, namelen) == 0)

	if (MATCH("QUERY", 5))
		return_value = _readQuery();
	else if (MATCH("SORTGROUPCLAUSE", 15))
		return_value = _readSortGroupClause();
	else if (MATCH("WINDOWCLAUSE", 12))
		return_value = _readWindowClause();
	else if (MATCH("ROWMARKCLAUSE", 13))
		return_value = _readRowMarkClause();
	else if (MATCH("COMMONTABLEEXPR", 15))
		return_value = _readCommonTableExpr();
	else if (MATCH("SETOPERATIONSTMT", 16))
		return_value = _readSetOperationStmt();
	else if (MATCH("ALIAS", 5))
		return_value = _readAlias();
	else if (MATCH("RANGEVAR", 8))
		return_value = _readRangeVar();
	else if (MATCH("INTOCLAUSE", 10))
		return_value = _readIntoClause();
	else if (MATCH("VAR", 3))
		return_value = _readVar();
	else if (MATCH("CONST", 5))
		return_value = _readConst();
	else if (MATCH("PARAM", 5))
		return_value = _readParam();
	else if (MATCH("AGGREF", 6))
		return_value = _readAggref();
	else if (MATCH("WINDOWFUNC", 10))
		return_value = _readWindowFunc();
	else if (MATCH("ARRAYREF", 8))
		return_value = _readArrayRef();
	else if (MATCH("FUNCEXPR", 8))
		return_value = _readFuncExpr();
	else if (MATCH("NAMEDARGEXPR", 12))
		return_value = _readNamedArgExpr();
	else if (MATCH("OPEXPR", 6))
		return_value = _readOpExpr();
	else if (MATCH("DISTINCTEXPR", 12))
		return_value = _readDistinctExpr();
	else if (MATCH("NULLIFEXPR", 10))
		return_value = _readNullIfExpr();
	else if (MATCH("SCALARARRAYOPEXPR", 17))
		return_value = _readScalarArrayOpExpr();
	else if (MATCH("BOOLEXPR", 8))
		return_value = _readBoolExpr();
	else if (MATCH("SUBLINK", 7))
		return_value = _readSubLink();
#ifdef XCP
	else if (MATCH("SUBPLAN", 7))
		return_value = _readSubPlan();
#endif
	else if (MATCH("FIELDSELECT", 11))
		return_value = _readFieldSelect();
	else if (MATCH("FIELDSTORE", 10))
		return_value = _readFieldStore();
	else if (MATCH("RELABELTYPE", 11))
		return_value = _readRelabelType();
	else if (MATCH("COERCEVIAIO", 11))
		return_value = _readCoerceViaIO();
	else if (MATCH("ARRAYCOERCEEXPR", 15))
		return_value = _readArrayCoerceExpr();
	else if (MATCH("CONVERTROWTYPEEXPR", 18))
		return_value = _readConvertRowtypeExpr();
	else if (MATCH("COLLATE", 7))
		return_value = _readCollateExpr();
	else if (MATCH("CASE", 4))
		return_value = _readCaseExpr();
	else if (MATCH("WHEN", 4))
		return_value = _readCaseWhen();
	else if (MATCH("CASETESTEXPR", 12))
		return_value = _readCaseTestExpr();
	else if (MATCH("ARRAY", 5))
		return_value = _readArrayExpr();
	else if (MATCH("ROW", 3))
		return_value = _readRowExpr();
	else if (MATCH("ROWCOMPARE", 10))
		return_value = _readRowCompareExpr();
	else if (MATCH("COALESCE", 8))
		return_value = _readCoalesceExpr();
	else if (MATCH("MINMAX", 6))
		return_value = _readMinMaxExpr();
	else if (MATCH("XMLEXPR", 7))
		return_value = _readXmlExpr();
	else if (MATCH("NULLTEST", 8))
		return_value = _readNullTest();
	else if (MATCH("BOOLEANTEST", 11))
		return_value = _readBooleanTest();
	else if (MATCH("COERCETODOMAIN", 14))
		return_value = _readCoerceToDomain();
	else if (MATCH("COERCETODOMAINVALUE", 19))
		return_value = _readCoerceToDomainValue();
	else if (MATCH("SETTODEFAULT", 12))
		return_value = _readSetToDefault();
	else if (MATCH("CURRENTOFEXPR", 13))
		return_value = _readCurrentOfExpr();
	else if (MATCH("TARGETENTRY", 11))
		return_value = _readTargetEntry();
	else if (MATCH("RANGETBLREF", 11))
		return_value = _readRangeTblRef();
	else if (MATCH("JOINEXPR", 8))
		return_value = _readJoinExpr();
	else if (MATCH("FROMEXPR", 8))
		return_value = _readFromExpr();
	else if (MATCH("RTE", 3))
		return_value = _readRangeTblEntry();
	else if (MATCH("NOTIFY", 6))
		return_value = _readNotifyStmt();
	else if (MATCH("DECLARECURSOR", 13))
		return_value = _readDeclareCursorStmt();
#ifdef XCP
	else if (MATCH("PLAN", 4))
		return_value = _readPlan();
	else if (MATCH("RESULT", 6))
		return_value = _readResult();
	else if (MATCH("MODIFYTABLE", 11))
		return_value = _readModifyTable();
	else if (MATCH("APPEND", 6))
		return_value = _readAppend();
	else if (MATCH("MERGEAPPEND", 11))
		return_value = _readMergeAppend();
	else if (MATCH("RECURSIVEUNION", 14))
		return_value = _readRecursiveUnion();
	else if (MATCH("BITMAPAND", 9))
		return_value = _readBitmapAnd();
	else if (MATCH("BITMAPOR", 8))
		return_value = _readBitmapOr();
	else if (MATCH("SCAN", 4))
		return_value = _readScan();
	else if (MATCH("SEQSCAN", 7))
		return_value = _readSeqScan();
	else if (MATCH("INDEXSCAN", 9))
		return_value = _readIndexScan();
	else if (MATCH("INDEXONLYSCAN", 13))
		return_value = _readIndexOnlyScan();
	else if (MATCH("BITMAPINDEXSCAN", 15))
		return_value = _readBitmapIndexScan();
	else if (MATCH("BITMAPHEAPSCAN", 14))
		return_value = _readBitmapHeapScan();
	else if (MATCH("TIDSCAN", 7))
		return_value = _readTidScan();
	else if (MATCH("SUBQUERYSCAN", 12))
		return_value = _readSubqueryScan();
	else if (MATCH("FUNCTIONSCAN", 12))
		return_value = _readFunctionScan();
	else if (MATCH("VALUESSCAN", 10))
		return_value = _readValuesScan();
	else if (MATCH("CTESCAN", 7))
		return_value = _readCteScan();
	else if (MATCH("WORKTABLESCAN", 13))
		return_value = _readWorkTableScan();
	else if (MATCH("JOIN", 4))
		return_value = _readJoin();
	else if (MATCH("NESTLOOP", 8))
		return_value = _readNestLoop();
	else if (MATCH("MERGEJOIN", 9))
		return_value = _readMergeJoin();
	else if (MATCH("HASHJOIN", 8))
		return_value = _readHashJoin();
	else if (MATCH("MATERIAL", 8))
		return_value = _readMaterial();
	else if (MATCH("SORT", 4))
		return_value = _readSort();
	else if (MATCH("GROUP", 5))
		return_value = _readGroup();
	else if (MATCH("AGG", 3))
		return_value = _readAgg();
	else if (MATCH("WINDOWAGG", 9))
		return_value = _readWindowAgg();
	else if (MATCH("UNIQUE", 6))
		return_value = _readUnique();
	else if (MATCH("HASH", 4))
		return_value = _readHash();
	else if (MATCH("SETOP", 5))
		return_value = _readSetOp();
	else if (MATCH("LIMIT", 5))
		return_value = _readLimit();
	else if (MATCH("REMOTESUBPLAN", 13))
		return_value = _readRemoteSubplan();
	else if (MATCH("REMOTESTMT", 10))
		return_value = _readRemoteStmt();
	else if (MATCH("SIMPLESORT", 10))
		return_value = _readSimpleSort();
	else if (MATCH("NESTLOOPPARAM", 13))
		return_value = _readNestLoopParam();
	else if (MATCH("PLANROWMARK", 11))
		return_value = _readPlanRowMark();
	else if (MATCH("LOCKROWS", 8))
		return_value = _readLockRows();
#endif
	else
	{
		elog(ERROR, "badly formatted node string \"%.32s\"...", token);
		return_value = NULL;	/* keep compiler quiet */
	}

	return (Node *) return_value;
}


/*
 * readDatum
 *
 * Given a string representation of a constant, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
static Datum
readDatum(bool typbyval)
{
	Size		length,
				i;
	int			tokenLength;
	char	   *token;
	Datum		res;
	char	   *s;

	/*
	 * read the actual length of the value
	 */
	token = pg_strtok(&tokenLength);
	length = atoui(token);

	token = pg_strtok(&tokenLength);	/* read the '[' */
	if (token == NULL || token[0] != '[')
		elog(ERROR, "expected \"[\" to start datum, but got \"%s\"; length = %lu",
			 token ? (const char *) token : "[NULL]",
			 (unsigned long) length);

	if (typbyval)
	{
		if (length > (Size) sizeof(Datum))
			elog(ERROR, "byval datum but length = %lu",
				 (unsigned long) length);
		res = (Datum) 0;
		s = (char *) (&res);
		for (i = 0; i < (Size) sizeof(Datum); i++)
		{
			token = pg_strtok(&tokenLength);
			s[i] = (char) atoi(token);
		}
	}
	else if (length <= 0)
		res = (Datum) NULL;
	else
	{
		s = (char *) palloc(length);
		for (i = 0; i < length; i++)
		{
			token = pg_strtok(&tokenLength);
			s[i] = (char) atoi(token);
		}
		res = PointerGetDatum(s);
	}

	token = pg_strtok(&tokenLength);	/* read the ']' */
	if (token == NULL || token[0] != ']')
		elog(ERROR, "expected \"]\" to end datum, but got \"%s\"; length = %lu",
			 token ? (const char *) token : "[NULL]",
			 (unsigned long) length);

	return res;
}

#ifdef XCP
/*
 * scanDatum
 *
 * Recreate Datum from the text format understandable by the input function
 * of the specified data type.
 */
static Datum
scanDatum(Oid typid, int typmod)
{
	Oid			typInput;
	Oid			typioparam;
	FmgrInfo	finfo;
	FunctionCallInfoData fcinfo;
	char	   *value;
	Datum		res;
	READ_TEMP_LOCALS();

	/* Get input function for the type */
	getTypeInputInfo(typid, &typInput, &typioparam);
	fmgr_info(typInput, &finfo);

	/* Read the value */
	token = pg_strtok(&length);
	value = nullable_string(token, length);

	/* The value can not be NULL, so we actually received empty string */
	if (value == NULL)
		value = "";

	/* Invoke input function */
	InitFunctionCallInfoData(fcinfo, &finfo, 3, InvalidOid, NULL, NULL);

	fcinfo.arg[0] = CStringGetDatum(value);
	fcinfo.arg[1] = ObjectIdGetDatum(typioparam);
	fcinfo.arg[2] = Int32GetDatum(typmod);
	fcinfo.argnull[0] = false;
	fcinfo.argnull[1] = false;
	fcinfo.argnull[2] = false;

	res = FunctionCallInvoke(&fcinfo);

	return res;
}
#endif
