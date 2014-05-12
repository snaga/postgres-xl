/*-------------------------------------------------------------------------
 *
 * pghba.c
 *	  display and manipulate the contents of pg_hba.conf
 *
 *	  contrib/pghba/pghba.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/hba.h"
#include "utils/builtins.h"
#include "miscadmin.h"

#define NCOLUMNS	28
/* This is used to separate values in multi-valued column strings */
#define MULTI_VALUE_SEP "\001"

PG_MODULE_MAGIC;

Datum		pghba_list(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pghba_list);

Datum
pghba_list(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc       tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	ListCell   *line;
	HbaLine    *hba;
	List       *lines;
	StringInfoData 	buf;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	tupdesc = CreateTemplateTupleDesc(NCOLUMNS, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "linenumber",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "conntype",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "databases",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "roles",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "addr",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "mask",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 7, "ip_cmp_method",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 8, "hostname",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 9, "auth_method",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 10, "usermap",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 11, "pamservice",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 12, "ldaptls",
					   BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 13, "ldapserver",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 14, "ldapport",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 15, "ldapbinddn",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 16, "ldapbindpasswd",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 17, "ldapsearchattribute",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 18, "ldapbasedn",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 19, "ldapprefix",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 20, "ldapsuffix",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 21, "clientcert",
					   BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 22, "krb_server_hostname",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 23, "krb_realm",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 24, "include_realm",
					   BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 25, "radiusserver",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 26, "radiussecret",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 27, "radiusidentifier",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 28, "radiusport",
					   INT4OID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	initStringInfo(&buf);
	lines = get_parsed_hba();
	foreach(line, lines)
	{
		Datum           values[NCOLUMNS];
		bool            nulls[NCOLUMNS];
		int             i = 0, j;
		char			*databases;
		char			*roles;
		char			*member;
		char 			addr[INET6_ADDRSTRLEN];
		char            mask[INET6_ADDRSTRLEN];
		socklen_t 		addr_len;
		socklen_t 		mask_len;

		hba = (HbaLine *) lfirst(line);

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[i++] = ObjectIdGetDatum(hba->linenumber);
		values[i++] = ObjectIdGetDatum(hba->conntype);

		/* Get the list of databases as text */
		resetStringInfo(&buf);
		databases = pstrdup(hba->database);
		j = 0;
		for (member = strtok(databases, MULTI_VALUE_SEP);
			 member != NULL;
			 member = strtok(NULL, MULTI_VALUE_SEP))
		{
			if (j++ == 0)
				appendStringInfo(&buf, "%s", member);
			else
				appendStringInfo(&buf, ",%s", member);
		}
		values[i++] = CStringGetTextDatum(pstrdup(buf.data));

		/* Get the list of roles as text */
		resetStringInfo(&buf);
		roles = pstrdup(hba->role);
		j = 0;
		for (member = strtok(roles, MULTI_VALUE_SEP);
			 member != NULL;
			 member = strtok(NULL, MULTI_VALUE_SEP))
		{
			if (j++ == 0)
				appendStringInfo(&buf, "%s", member);
			else
				appendStringInfo(&buf, ",%s", member);
		}
		values[i++] = CStringGetTextDatum(pstrdup(buf.data));

		if (hba->conntype == ctLocal)
		{
			/* Address is null for local */
			nulls[i++] = true;

			/* Mask is null for local */
			nulls[i++] = true;
		} else {
			addr_len=sizeof(hba->addr);
			getnameinfo((struct sockaddr *) & hba->addr,addr_len,addr,sizeof(addr),
						0,0,NI_NUMERICHOST);

			values[i++] = CStringGetTextDatum(addr);

			mask_len=sizeof(hba->mask);
			getnameinfo((struct sockaddr *) & hba->mask,mask_len,mask,sizeof(mask),
						0,0,NI_NUMERICHOST);

			values[i++] = CStringGetTextDatum(mask);
		}

		values[i++] = ObjectIdGetDatum(hba->ip_cmp_method);

		if (hba->hostname == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->hostname));

		values[i++] = ObjectIdGetDatum(hba->auth_method);

		if (hba->usermap == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->usermap));

		if (hba->pamservice == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->pamservice));

		values[i++] = BoolGetDatum(hba->ldaptls);

		if (hba->ldapserver == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->ldapserver));

		values[i++] = ObjectIdGetDatum(hba->ldapport);

		if (hba->ldapbinddn == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->ldapbinddn));

		if (hba->ldapbindpasswd == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->ldapbindpasswd));

		if (hba->ldapsearchattribute == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->ldapsearchattribute));

		if (hba->ldapbasedn == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->ldapbasedn));

		if (hba->ldapprefix == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->ldapprefix));

		if (hba->ldapsuffix == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->ldapsuffix));

		values[i++] = BoolGetDatum(hba->clientcert);

		if (hba->krb_server_hostname == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->krb_server_hostname));

		if (hba->krb_realm == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->krb_realm));

		values[i++] = BoolGetDatum(hba->include_realm);

		if (hba->radiusserver == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->radiusserver));

		if (hba->radiussecret == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->radiussecret));

		if (hba->radiusidentifier == NULL)
			nulls[i++] = true;
		else
			values[i++] = CStringGetTextDatum(pstrdup(hba->radiusidentifier));

		values[i++] = ObjectIdGetDatum(hba->radiusport);

		Assert(i == NCOLUMNS);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
