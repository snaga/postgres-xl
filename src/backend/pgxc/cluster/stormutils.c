/*-------------------------------------------------------------------------
 *
 * stormutils.c
 *
 * Miscellaneous util functions
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#ifdef XCP
#include "postgres.h"
#include "miscadmin.h"

#include "utils/builtins.h"
#include "../interfaces/libpq/libpq-fe.h"
#include "commands/dbcommands.h"

/*
 * stormdb_promote_standby:
 *
 * Promote a standby into a regular backend by touching the trigger file. We
 * cannot do it from outside via a normal shell script because this function
 * needs to be called in context of the operation that is moving the node.
 * Providing a function call provides some sense of transactional atomicity
 */
Datum
stormdb_promote_standby(PG_FUNCTION_ARGS)
{
	char		trigger_file[MAXPGPATH];
	FILE		*fp;

	snprintf(trigger_file, MAXPGPATH, "%s/stormdb.failover", DataDir);

	if ((fp = fopen(trigger_file, "w")) == NULL)
		ereport(ERROR,
				(errmsg("could not create trigger file"),
				 errdetail("The trigger file path was: %s",
						   trigger_file)));
	fclose(fp);

	PG_RETURN_VOID();
}
#endif
