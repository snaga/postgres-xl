/*-------------------------------------------------------------------------
 *
 * poolmgr.c
 *
 *	  Connection pool manager handles connections to Datanodes
 *
 * The pooler runs as a separate process and is forked off from a
 * Coordinator postmaster. If the Coordinator needs a connection from a
 * Datanode, it asks for one from the pooler, which maintains separate
 * pools for each Datanode. A group of connections can be requested in
 * a single request, and the pooler returns a list of file descriptors
 * to use for the connections.
 *
 * Note the current implementation does not yet shrink the pool over time
 * as connections are idle.  Also, it does not queue requests; if a
 * connection is unavailable, it will simply fail. This should be implemented
 * one day, although there is a chance for deadlocks. For now, limiting
 * connections should be done between the application and Coordinator.
 * Still, this is useful to avoid having to re-establish connections to the
 * Datanodes all the time for multiple Coordinator backend sessions.
 *
 * The term "agent" here refers to a session manager, one for each backend
 * Coordinator connection to the pooler. It will contain a list of connections
 * allocated to a session, at most one per Datanode.
 *
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <signal.h>
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolutils.h"
#include "../interfaces/libpq/libpq-fe.h"
#include "../interfaces/libpq/libpq-int.h"
#include "postmaster/postmaster.h"		/* For UnixSocketDir */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#ifdef XCP
#include "pgxc/pause.h"
#include "storage/procarray.h"
#endif

/* Configuration options */
#ifdef XCP
int			PoolConnKeepAlive = 600;
int			PoolMaintenanceTimeout = 30;
#else
int			MinPoolSize = 1;
#endif
int			MaxPoolSize = 100;
int			PoolerPort = 6667;

bool			PersistentConnections = false;

/* Flag to tell if we are Postgres-XC pooler process */
static bool am_pgxc_pooler = false;

/* Connection information cached */
typedef struct
{
	Oid	nodeoid;
	char	*host;
	int	port;
} PGXCNodeConnectionInfo;

#ifdef XCP
/* Handle to the pool manager (Session's side) */
typedef struct
{
	/* communication channel */
	PoolPort	port;
} PoolHandle;
#endif

/* The root memory context */
static MemoryContext PoolerMemoryContext = NULL;
/*
 * Allocations of core objects: Datanode connections, upper level structures,
 * connection strings, etc.
 */
static MemoryContext PoolerCoreContext = NULL;
/*
 * Memory to store Agents
 */
static MemoryContext PoolerAgentContext = NULL;

/* Pool to all the databases (linked list) */
static DatabasePool *databasePools = NULL;

/* PoolAgents */
static int	agentCount = 0;
static PoolAgent **poolAgents;

static PoolHandle *poolHandle = NULL;

static int	is_pool_locked = false;
static int	server_fd = -1;

static int	node_info_check(PoolAgent *agent);
#ifdef XCP
static void agent_init(PoolAgent *agent, const char *database,
						const char *user_name);
#else
static void agent_init(PoolAgent *agent, const char *database, const char *user_name,
	                   const char *pgoptions);
#endif
static void agent_destroy(PoolAgent *agent);
static void agent_create(void);
static void agent_handle_input(PoolAgent *agent, StringInfo s);
#ifndef XCP
static int agent_session_command(PoolAgent *agent,
								 const char *set_command,
								 PoolCommandType command_type);
static int agent_set_command(PoolAgent *agent,
							 const char *set_command,
							 PoolCommandType command_type);
static int agent_temp_command(PoolAgent *agent);
#endif
#ifdef XCP
static DatabasePool *create_database_pool(const char *database,
										   const char *user_name);
#else
static DatabasePool *create_database_pool(const char *database, const char *user_name, const char *pgoptions);
#endif
static void insert_database_pool(DatabasePool *pool);
static int	destroy_database_pool(const char *database, const char *user_name);
static void reload_database_pools(PoolAgent *agent);
#ifdef XCP
static DatabasePool *find_database_pool(const char *database,
										 const char *user_name);
#else
static DatabasePool *find_database_pool(const char *database, const char *user_name, const char *pgoptions);
#endif
static DatabasePool *remove_database_pool(const char *database, const char *user_name);
static int *agent_acquire_connections(PoolAgent *agent, List *datanodelist, List *coordlist);
#ifndef XCP
static int send_local_commands(PoolAgent *agent, List *datanodelist, List *coordlist);
#endif
static int cancel_query_on_connections(PoolAgent *agent, List *datanodelist, List *coordlist);
static PGXCNodePoolSlot *acquire_connection(DatabasePool *dbPool, Oid node);
static void agent_release_connections(PoolAgent *agent, bool force_destroy);
#ifndef XCP
static void agent_reset_session(PoolAgent *agent);
#endif
static void release_connection(DatabasePool *dbPool, PGXCNodePoolSlot *slot,
							   Oid node, bool force_destroy);
static void destroy_slot(PGXCNodePoolSlot *slot);
static PGXCNodePool *grow_pool(DatabasePool *dbPool, Oid node);
static void destroy_node_pool(PGXCNodePool *node_pool);
static void PoolerLoop(void);
static int clean_connection(List *node_discard,
							const char *database,
							const char *user_name);
static int *abort_pids(int *count,
					   int pid,
					   const char *database,
					   const char *user_name);
static char *build_node_conn_str(Oid node, DatabasePool *dbPool);
/* Signal handlers */
static void pooler_die(SIGNAL_ARGS);
static void pooler_quickdie(SIGNAL_ARGS);
#ifdef XCP
static void PoolManagerConnect(const char *database, const char *user_name);
static void pooler_sighup(SIGNAL_ARGS);
static bool shrink_pool(DatabasePool *pool);
static void pools_maintenance(void);
#endif
/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
#ifdef XCP
static volatile sig_atomic_t got_SIGHUP = false;
#endif
static volatile sig_atomic_t shutdown_requested = false;

void
PGXCPoolerProcessIam(void)
{
	am_pgxc_pooler = true;
}

bool
IsPGXCPoolerProcess(void)
{
    return am_pgxc_pooler;
}

/*
 * Initialize internal structures
 */
int
PoolManagerInit()
{
	elog(DEBUG1, "Pooler process is started: %d", getpid());

	/*
	 * Set up memory contexts for the pooler objects
	 */
	PoolerMemoryContext = AllocSetContextCreate(TopMemoryContext,
												"PoolerMemoryContext",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);
	PoolerCoreContext = AllocSetContextCreate(PoolerMemoryContext,
											  "PoolerCoreContext",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);
	PoolerAgentContext = AllocSetContextCreate(PoolerMemoryContext,
											   "PoolerAgentContext",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.	(pool manager probably never has any
	 * child processes, but for consistency we make all postmaster child
	 * processes do this.)
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif
	/*
	 * Properly accept or ignore signals the postmaster might send us
	 */
	pqsignal(SIGINT, pooler_die);
	pqsignal(SIGTERM, pooler_die);
	pqsignal(SIGQUIT, pooler_quickdie);
#ifdef XCP
	pqsignal(SIGHUP, pooler_sighup);
#else
	pqsignal(SIGHUP, SIG_IGN);
#endif
	/* TODO other signal handlers */

	/* We allow SIGQUIT (quickdie) at all times */
#ifdef HAVE_SIGPROCMASK
	sigdelset(&BlockSig, SIGQUIT);
#else
	BlockSig &= ~(sigmask(SIGQUIT));
#endif

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/* Allocate pooler structures in the Pooler context */
	MemoryContextSwitchTo(PoolerMemoryContext);

	poolAgents = (PoolAgent **) palloc(MaxConnections * sizeof(PoolAgent *));
	if (poolAgents == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	PoolerLoop();
	return 0;
}


/*
 * Check connection info consistency with system catalogs
 */
static int
node_info_check(PoolAgent *agent)
{
	DatabasePool   *dbPool = databasePools;
	List 		   *checked = NIL;
	int 			res = POOL_CHECK_SUCCESS;
	Oid			   *coOids;
	Oid			   *dnOids;
	int				numCo;
	int				numDn;

	/*
	 * First check if agent's node information matches to current content of the
	 * shared memory table.
	 */
	PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

	if (agent->num_coord_connections != numCo ||
			agent->num_dn_connections != numDn ||
			memcmp(agent->coord_conn_oids, coOids, numCo * sizeof(Oid)) ||
			memcmp(agent->dn_conn_oids, dnOids, numDn * sizeof(Oid)))
		res = POOL_CHECK_FAILED;

	/* Release palloc'ed memory */
	pfree(coOids);
	pfree(dnOids);

	/*
	 * Iterate over all dbnode pools and check if connection strings
	 * are matching node definitions.
	 */
	while (res == POOL_CHECK_SUCCESS && dbPool)
	{
		HASH_SEQ_STATUS hseq_status;
		PGXCNodePool   *nodePool;

		hash_seq_init(&hseq_status, dbPool->nodePools);
		while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
		{
			char 		   *connstr_chk;

			/* No need to check same Datanode twice */
			if (list_member_oid(checked, nodePool->nodeoid))
				continue;
			checked = lappend_oid(checked, nodePool->nodeoid);

			connstr_chk = build_node_conn_str(nodePool->nodeoid, dbPool);
			if (connstr_chk == NULL)
			{
				/* Problem of constructing connection string */
				hash_seq_term(&hseq_status);
				res = POOL_CHECK_FAILED;
				break;
			}
			/* return error if there is difference */
			if (strcmp(connstr_chk, nodePool->connstr))
			{
				pfree(connstr_chk);
				hash_seq_term(&hseq_status);
				res = POOL_CHECK_FAILED;
				break;
			}

			pfree(connstr_chk);
		}
		dbPool = dbPool->next;
	}
	list_free(checked);
	return res;
}

/*
 * Destroy internal structures
 */
int
PoolManagerDestroy(void)
{
	int			status = 0;

	if (PoolerMemoryContext)
	{
		MemoryContextDelete(PoolerMemoryContext);
		PoolerMemoryContext = NULL;
	}

	return status;
}


#ifdef XCP
/*
 * Connect to the pooler process
 */
static void
#else
/*
 * Get handle to pool manager
 * Invoked from Postmaster's main loop just before forking off new session
 * Returned PoolHandle structure will be inherited by session process
 */
PoolHandle *
#endif
GetPoolManagerHandle(void)
{
	PoolHandle *handle;
	int			fdsock;

#ifdef XCP
	if (poolHandle)
		/* already connected */
		return;
#endif

	/* Connect to the pooler */
	fdsock = pool_connect(PoolerPort, UnixSocketDir);
	if (fdsock < 0)
	{
		int			saved_errno = errno;

		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to connect to pool manager: %m")));
		errno = saved_errno;
#ifndef XCP
		return NULL;
#endif
	}

	/* Allocate handle */
	/*
	 * XXX we may change malloc here to palloc but first ensure
	 * the CurrentMemoryContext is properly set.
	 * The handle allocated just before new session is forked off and
	 * inherited by the session process. It should remain valid for all
	 * the session lifetime.
	 */
	handle = (PoolHandle *) malloc(sizeof(PoolHandle));
	if (!handle)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
#ifndef XCP
		return NULL;
#endif
	}

	handle->port.fdsock = fdsock;
	handle->port.RecvLength = 0;
	handle->port.RecvPointer = 0;
	handle->port.SendPointer = 0;

#ifdef XCP
	poolHandle = handle;
#else
	return handle;
#endif
}


#ifndef XCP
/*
 * XXX May create on_proc_exit callback instead
 */
void
PoolManagerCloseHandle(PoolHandle *handle)
{
	close(Socket(handle->port));
	free(handle);
	handle = NULL;
}
#endif

/*
 * Create agent
 */
static void
agent_create(void)
{
	MemoryContext oldcontext;
	int			new_fd;
	PoolAgent  *agent;

	new_fd = accept(server_fd, NULL, NULL);
	if (new_fd < 0)
	{
		int			saved_errno = errno;

		ereport(LOG,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("pool manager failed to accept connection: %m")));
		errno = saved_errno;
		return;
	}

	oldcontext = MemoryContextSwitchTo(PoolerAgentContext);

	/* Allocate agent */
	agent = (PoolAgent *) palloc(sizeof(PoolAgent));
	if (!agent)
	{
		close(new_fd);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		return;
	}

	agent->port.fdsock = new_fd;
	agent->port.RecvLength = 0;
	agent->port.RecvPointer = 0;
	agent->port.SendPointer = 0;
	agent->pool = NULL;
	agent->mcxt = AllocSetContextCreate(CurrentMemoryContext,
										"Agent",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);
	agent->num_dn_connections = 0;
	agent->num_coord_connections = 0;
	agent->dn_conn_oids = NULL;
	agent->coord_conn_oids = NULL;
	agent->dn_connections = NULL;
	agent->coord_connections = NULL;
#ifndef XCP
	agent->session_params = NULL;
	agent->local_params = NULL;
	agent->is_temp = false;
#endif
	agent->pid = 0;

	/* Append new agent to the list */
	poolAgents[agentCount++] = agent;

	MemoryContextSwitchTo(oldcontext);
}


#ifndef XCP
/*
 * session_options
 * Returns the pgoptions string generated using a particular
 * list of parameters that are required to be propagated to Datanodes.
 * These parameters then become default values for the pooler sessions.
 * For e.g., a psql user sets PGDATESTYLE. This value should be set
 * as the default connection parameter in the pooler session that is
 * connected to the Datanodes. There are various parameters which need to
 * be analysed individually to determine whether these should be set on
 * Datanodes.
 *
 * Note: These parameters values are the default values of the particular
 * Coordinator backend session, and not the new values set by SET command.
 *
 */

char *session_options(void)
{
	int				 i;
	char			*pgoptions[] = {"DateStyle", "timezone", "geqo", "intervalstyle", "lc_monetary"};
	StringInfoData	 options;
	List			*value_list;
	ListCell		*l;

	initStringInfo(&options);

	for (i = 0; i < sizeof(pgoptions)/sizeof(char*); i++)
	{
		const char		*value;

		appendStringInfo(&options, " -c %s=", pgoptions[i]);

		value = GetConfigOptionResetString(pgoptions[i]);

		/* lc_monetary does not accept lower case values */
		if (strcmp(pgoptions[i], "lc_monetary") == 0)
		{
			appendStringInfoString(&options, value);
			continue;
		}

		SplitIdentifierString(strdup(value), ',', &value_list);
		foreach(l, value_list)
		{
			char *value = (char *) lfirst(l);
			appendStringInfoString(&options, value);
			if (lnext(l))
				appendStringInfoChar(&options, ',');
		}
	}

	return options.data;
}
#endif


/*
 * Associate session with specified database and respective connection pool
 * Invoked from Session process
 */
#ifdef XCP
static void
PoolManagerConnect(const char *database, const char *user_name)
{
	int 	n32;
	char 	msgtype = 'c';
	int 	unamelen = strlen(user_name);
	int 	dbnamelen = strlen(database);
	char	atchar = ' ';

	/* Connect to the pooler process if not yet connected */
	GetPoolManagerHandle();
	if (poolHandle == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to connect to the pooler process")));

	/*
	 * Special handling for db_user_namespace=on
	 * We need to handle per-db users and global users. The per-db users will
	 * arrive with @dbname and global users just as username. Handle both of
	 * them appropriately
	 */
	if (strcmp(GetConfigOption("db_user_namespace", false, false), "on") == 0)
	{
		if (strchr(user_name, '@') != NULL)
		{
			Assert(unamelen > dbnamelen + 1);
			unamelen -= (dbnamelen + 1);
		}
		else
		{
			atchar = '@';
			unamelen++;
		}
	}

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	n32 = htonl(dbnamelen + unamelen + 18);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* PID number */
	n32 = htonl(MyProcPid);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Length of Database string */
	n32 = htonl(dbnamelen + 1);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send database name followed by \0 terminator */
	pool_putbytes(&poolHandle->port, database, dbnamelen);
	pool_putbytes(&poolHandle->port, "\0", 1);

	/* Length of user name string */
	n32 = htonl(unamelen + 1);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send user name followed by \0 terminator */
	/* Send the '@' char if needed. Already accounted for in len */
	if (atchar == '@')
	{
		pool_putbytes(&poolHandle->port, user_name, unamelen - 1);
		pool_putbytes(&poolHandle->port, "@", 1);
	}
	else
		pool_putbytes(&poolHandle->port, user_name, unamelen);
	pool_putbytes(&poolHandle->port, "\0", 1);
	pool_flush(&poolHandle->port);
}
#else
void
PoolManagerConnect(PoolHandle *handle,
	               const char *database, const char *user_name,
	               char *pgoptions)
{
	int n32;
	char msgtype = 'c';

	Assert(handle);
	Assert(database);
	Assert(user_name);

	/* Save the handle */
	poolHandle = handle;

	/* Message type */
	pool_putbytes(&handle->port, &msgtype, 1);

	/* Message length */
	n32 = htonl(strlen(database) + strlen(user_name) + strlen(pgoptions) + 23);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* PID number */
	n32 = htonl(MyProcPid);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* Length of Database string */
	n32 = htonl(strlen(database) + 1);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* Send database name followed by \0 terminator */
	pool_putbytes(&handle->port, database, strlen(database) + 1);
	pool_flush(&handle->port);

	/* Length of user name string */
	n32 = htonl(strlen(user_name) + 1);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* Send user name followed by \0 terminator */
	pool_putbytes(&handle->port, user_name, strlen(user_name) + 1);
	pool_flush(&handle->port);

	/* Length of pgoptions string */
	n32 = htonl(strlen(pgoptions) + 1);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* Send pgoptions followed by \0 terminator */
	pool_putbytes(&handle->port, pgoptions, strlen(pgoptions) + 1);
	pool_flush(&handle->port);

}
#endif

/*
 * Reconnect to pool manager
 * It simply does a disconnection and a reconnection.
 */
void
PoolManagerReconnect(void)
{
#ifdef XCP
	/* Connected, disconnect */
	if (poolHandle)
		PoolManagerDisconnect();

	PoolManagerConnect(get_database_name(MyDatabaseId), GetClusterUserName());
#else
	PoolHandle *handle;

	Assert(poolHandle);

	PoolManagerDisconnect();
	handle = GetPoolManagerHandle();
	PoolManagerConnect(handle,
					   get_database_name(MyDatabaseId),
					   GetUserNameFromId(GetUserId()),
					   session_options());
#endif
}


#ifndef XCP
int
PoolManagerSetCommand(PoolCommandType command_type, const char *set_command)
{
	int n32, res;
	char msgtype = 's';

	Assert(poolHandle);

	/*
	 * If SET LOCAL is in use, flag current transaction as using
	 * transaction-block related parameters with pooler agent.
	 */
	if (command_type == POOL_CMD_LOCAL_SET)
		SetCurrentLocalParamStatus(true);

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	if (set_command)
		n32 = htonl(strlen(set_command) + 13);
	else
		n32 = htonl(12);

	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* LOCAL or SESSION parameter ? */
	n32 = htonl(command_type);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	if (set_command)
	{
		/* Length of SET command string */
		n32 = htonl(strlen(set_command) + 1);
		pool_putbytes(&poolHandle->port, (char *) &n32, 4);

		/* Send command string followed by \0 terminator */
		pool_putbytes(&poolHandle->port, set_command, strlen(set_command) + 1);
	}
	else
	{
		/* Send empty command */
		n32 = htonl(0);
		pool_putbytes(&poolHandle->port, (char *) &n32, 4);
	}

	pool_flush(&poolHandle->port);

	/* Get result */
	res = pool_recvres(&poolHandle->port);

	return res;
}

/*
 * Send commands to alter the behavior of current transaction and update begin sent status
 */

int
PoolManagerSendLocalCommand(int dn_count, int* dn_list, int co_count, int* co_list)
{
	uint32		n32;
	/*
	 * Buffer contains the list of both Coordinator and Datanodes, as well
	 * as the number of connections
	 */
	uint32 		buf[2 + dn_count + co_count];
	int 		i;

	if (poolHandle == NULL)
		return EOF;

	if (dn_count == 0 && co_count == 0)
		return EOF;

	if (dn_count != 0 && dn_list == NULL)
		return EOF;

	if (co_count != 0 && co_list == NULL)
		return EOF;

	/* Insert the list of Datanodes in buffer */
	n32 = htonl((uint32) dn_count);
	buf[0] = n32;

	for (i = 0; i < dn_count;)
	{
		n32 = htonl((uint32) dn_list[i++]);
		buf[i] = n32;
	}

	/* Insert the list of Coordinators in buffer */
	n32 = htonl((uint32) co_count);
	buf[dn_count + 1] = n32;

	/* Not necessary to send to pooler a request if there is no Coordinator */
	if (co_count != 0)
	{
		for (i = dn_count + 1; i < (dn_count + co_count + 1);)
		{
			n32 = htonl((uint32) co_list[i - (dn_count + 1)]);
			buf[++i] = n32;
		}
	}
	pool_putmessage(&poolHandle->port, 'b', (char *) buf, (2 + dn_count + co_count) * sizeof(uint32));
	pool_flush(&poolHandle->port);

	/* Get result */
	return pool_recvres(&poolHandle->port);
}
#endif

/*
 * Lock/unlock pool manager
 * During locking, the only operations not permitted are abort, connection and
 * connection obtention.
 */
void
PoolManagerLock(bool is_lock)
{
	char msgtype = 'o';
	int n32;
	int msglen = 8;
#ifdef XCP
	if (poolHandle == NULL)
		PoolManagerConnect(get_database_name(MyDatabaseId),
						   GetClusterUserName());
#else
	Assert(poolHandle);
#endif

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	n32 = htonl(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Lock information */
	n32 = htonl((int) is_lock);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);
	pool_flush(&poolHandle->port);
}

/*
 * Init PoolAgent
 */
#ifdef XCP
static void
agent_init(PoolAgent *agent, const char *database,
						const char *user_name)
#else
static void
agent_init(PoolAgent *agent, const char *database, const char *user_name,
           const char *pgoptions)
#endif
{
	MemoryContext oldcontext;

	Assert(agent);
	Assert(database);
	Assert(user_name);

	/* disconnect if we are still connected */
	if (agent->pool)
		agent_release_connections(agent, false);

	oldcontext = MemoryContextSwitchTo(agent->mcxt);

	/* Get needed info and allocate memory */
	PgxcNodeGetOids(&agent->coord_conn_oids, &agent->dn_conn_oids,
					&agent->num_coord_connections, &agent->num_dn_connections, false);

	agent->coord_connections = (PGXCNodePoolSlot **)
			palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot *));
	agent->dn_connections = (PGXCNodePoolSlot **)
			palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot *));
#ifdef XCP
	/* find database */
	agent->pool = find_database_pool(database, user_name);

	/* create if not found */
	if (agent->pool == NULL)
		agent->pool = create_database_pool(database, user_name);
#else
	/* find database */
	agent->pool = find_database_pool(database, user_name, pgoptions);

	/* create if not found */
	if (agent->pool == NULL)
		agent->pool = create_database_pool(database, user_name, pgoptions);
#endif

	MemoryContextSwitchTo(oldcontext);

	return;
}

/*
 * Destroy PoolAgent
 */
static void
agent_destroy(PoolAgent *agent)
{
	int	i;

	Assert(agent);

	close(Socket(agent->port));

	/* Discard connections if any remaining */
	if (agent->pool)
	{
#ifdef XCP
		/*
		 * If session is disconnecting while there are active connections
		 * we can not know if they clean or not, so force destroy them
		 */
		agent_release_connections(agent, true);
#else
		/*
		 * Agent is being destroyed, so reset session parameters
		 * before putting back connections to pool.
		 */
		agent_reset_session(agent);

		/*
		 * Release them all.
		 * Force disconnection if there are temporary objects on agent.
		 */
		agent_release_connections(agent, agent->is_temp);
#endif
	}

	/* find agent in the list */
	for (i = 0; i < agentCount; i++)
	{
		if (poolAgents[i] == agent)
		{
			/* Free memory. All connection slots are NULL at this point */
			MemoryContextDelete(agent->mcxt);

			pfree(agent);
			/* shrink the list and move last agent into the freed slot */
			if (i < --agentCount)
				poolAgents[i] = poolAgents[agentCount];
			/* only one match is expected so exit */
			break;
		}
	}
}


/*
 * Release handle to pool manager
 */
void
PoolManagerDisconnect(void)
{
#ifdef XCP
	if (!poolHandle)
		return; /* not even connected */
#else
	Assert(poolHandle);
#endif

	pool_putmessage(&poolHandle->port, 'd', NULL, 0);
	pool_flush(&poolHandle->port);

	close(Socket(poolHandle->port));
#ifdef XCP
	free(poolHandle);
#endif
	poolHandle = NULL;
}


/*
 * Get pooled connections
 */
int *
PoolManagerGetConnections(List *datanodelist, List *coordlist)
{
	int			i;
	ListCell   *nodelist_item;
	int		   *fds;
	int			totlen = list_length(datanodelist) + list_length(coordlist);
	int			nodes[totlen + 2];

#ifdef XCP
	if (poolHandle == NULL)
		PoolManagerConnect(get_database_name(MyDatabaseId),
						   GetClusterUserName());
#else
	Assert(poolHandle);
#endif

	/*
	 * Prepare end send message to pool manager.
	 * First with Datanode list.
	 * This list can be NULL for a query that does not need
	 * Datanode Connections (Sequence DDLs)
	 */
	nodes[0] = htonl(list_length(datanodelist));
	i = 1;
	if (list_length(datanodelist) != 0)
	{
		foreach(nodelist_item, datanodelist)
		{
			nodes[i++] = htonl(lfirst_int(nodelist_item));
		}
	}
	/* Then with Coordinator list (can be nul) */
	nodes[i++] = htonl(list_length(coordlist));
	if (list_length(coordlist) != 0)
	{
		foreach(nodelist_item, coordlist)
		{
			nodes[i++] = htonl(lfirst_int(nodelist_item));
		}
	}

	pool_putmessage(&poolHandle->port, 'g', (char *) nodes, sizeof(int) * (totlen + 2));
	pool_flush(&poolHandle->port);

	/* Receive response */
	fds = (int *) palloc(sizeof(int) * totlen);
	if (fds == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}
	if (pool_recvfds(&poolHandle->port, fds, totlen))
	{
		pfree(fds);
		return NULL;
	}

	return fds;
}

/*
 * Abort active transactions using pooler.
 * Take a lock forbidding access to Pooler for new transactions.
 */
int
PoolManagerAbortTransactions(char *dbname, char *username, int **proc_pids)
{
	int		num_proc_ids = 0;
	int		n32, msglen;
	char		msgtype = 'a';
	int		dblen = dbname ? strlen(dbname) + 1 : 0;
	int		userlen = username ? strlen(username) + 1 : 0;

#ifdef XCP
	/*
	 * New connection may be established to clean connections to
	 * specified nodes and databases.
	 */
	if (poolHandle == NULL)
		PoolManagerConnect(get_database_name(MyDatabaseId),
						   GetClusterUserName());
#else
	Assert(poolHandle);
#endif

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	msglen = dblen + userlen + 12;
	n32 = htonl(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Length of Database string */
	n32 = htonl(dblen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send database name, followed by \0 terminator if necessary */
	if (dbname)
		pool_putbytes(&poolHandle->port, dbname, dblen);

	/* Length of Username string */
	n32 = htonl(userlen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send user name, followed by \0 terminator if necessary */
	if (username)
		pool_putbytes(&poolHandle->port, username, userlen);

	pool_flush(&poolHandle->port);

	/* Then Get back Pids from Pooler */
	num_proc_ids = pool_recvpids(&poolHandle->port, proc_pids);

	return num_proc_ids;
}


/*
 * Clean up Pooled connections
 */
void
PoolManagerCleanConnection(List *datanodelist, List *coordlist, char *dbname, char *username)
{
	int			totlen = list_length(datanodelist) + list_length(coordlist);
	int			nodes[totlen + 2];
	ListCell		*nodelist_item;
	int			i, n32, msglen;
	char			msgtype = 'f';
	int			userlen = username ? strlen(username) + 1 : 0;
	int			dblen = dbname ? strlen(dbname) + 1 : 0;

#ifdef XCP
	/*
	 * New connection may be established to clean connections to
	 * specified nodes and databases.
	 */
	if (poolHandle == NULL)
		PoolManagerConnect(get_database_name(MyDatabaseId),
						   GetClusterUserName());
#endif

	nodes[0] = htonl(list_length(datanodelist));
	i = 1;
	if (list_length(datanodelist) != 0)
	{
		foreach(nodelist_item, datanodelist)
		{
			nodes[i++] = htonl(lfirst_int(nodelist_item));
		}
	}
	/* Then with Coordinator list (can be nul) */
	nodes[i++] = htonl(list_length(coordlist));
	if (list_length(coordlist) != 0)
	{
		foreach(nodelist_item, coordlist)
		{
			nodes[i++] = htonl(lfirst_int(nodelist_item));
		}
	}

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	msglen = sizeof(int) * (totlen + 2) + dblen + userlen + 12;
	n32 = htonl(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send list of nodes */
	pool_putbytes(&poolHandle->port, (char *) nodes, sizeof(int) * (totlen + 2));

	/* Length of Database string */
	n32 = htonl(dblen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send database name, followed by \0 terminator if necessary */
	if (dbname)
		pool_putbytes(&poolHandle->port, dbname, dblen);

	/* Length of Username string */
	n32 = htonl(userlen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send user name, followed by \0 terminator if necessary */
	if (username)
		pool_putbytes(&poolHandle->port, username, userlen);

	pool_flush(&poolHandle->port);

	/* Receive result message */
	if (pool_recvres(&poolHandle->port) != CLEAN_CONNECTION_COMPLETED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Clean connections not completed")));
}


/*
 * Check connection information consistency cached in pooler with catalog information
 */
bool
PoolManagerCheckConnectionInfo(void)
{
	int res;

#ifdef XCP
	/*
	 * New connection may be established to clean connections to
	 * specified nodes and databases.
	 */
	if (poolHandle == NULL)
		PoolManagerConnect(get_database_name(MyDatabaseId),
						   GetClusterUserName());
#else
	Assert(poolHandle);
#endif
	PgxcNodeListAndCount();
	pool_putmessage(&poolHandle->port, 'q', NULL, 0);
	pool_flush(&poolHandle->port);

	res = pool_recvres(&poolHandle->port);

	if (res == POOL_CHECK_SUCCESS)
		return true;

	return false;
}


/*
 * Reload connection data in pooler and drop all the existing connections of pooler
 */
void
PoolManagerReloadConnectionInfo(void)
{
	Assert(poolHandle);
	PgxcNodeListAndCount();
	pool_putmessage(&poolHandle->port, 'p', NULL, 0);
	pool_flush(&poolHandle->port);
}


/*
 * Handle messages to agent
 */
static void
agent_handle_input(PoolAgent * agent, StringInfo s)
{
	int			qtype;

	qtype = pool_getbyte(&agent->port);
	/*
	 * We can have multiple messages, so handle them all
	 */
	for (;;)
	{
		const char *database = NULL;
		const char *user_name = NULL;
#ifndef XCP
		const char *pgoptions = NULL;
		PoolCommandType	command_type;
#endif
		int			datanodecount;
		int			coordcount;
		List	   *nodelist = NIL;
		List	   *datanodelist = NIL;
		List	   *coordlist = NIL;
		int		   *fds;
		int		   *pids;
		int			i, len, res;

		/*
		 * During a pool cleaning, Abort, Connect and Get Connections messages
		 * are not allowed on pooler side.
		 * It avoids to have new backends taking connections
		 * while remaining transactions are aborted during FORCE and then
		 * Pools are being shrinked.
		 */
		if (is_pool_locked && (qtype == 'a' || qtype == 'c' || qtype == 'g'))
			elog(WARNING,"Pool operation cannot run during pool lock");

		elog(DEBUG1, "Pooler is handling command %c from %d", (char) qtype, agent->pid);

		switch (qtype)
		{
			case 'a':			/* ABORT */
				pool_getmessage(&agent->port, s, 0);
				len = pq_getmsgint(s, 4);
				if (len > 0)
					database = pq_getmsgbytes(s, len);

				len = pq_getmsgint(s, 4);
				if (len > 0)
					user_name = pq_getmsgbytes(s, len);

				pq_getmsgend(s);

				pids = abort_pids(&len, agent->pid, database, user_name);

				pool_sendpids(&agent->port, pids, len);
				if (pids)
					pfree(pids);
				break;
#ifndef XCP
			case 'b':		/* Fire transaction-block commands on given nodes */
				/*
				 * Length of message is caused by:
				 * - Message header = 4bytes
				 * - Number of Datanodes sent = 4bytes
				 * - List of Datanodes = NumPoolDataNodes * 4bytes (max)
				 * - Number of Coordinators sent = 4bytes
				 * - List of Coordinators = NumPoolCoords * 4bytes (max)
				 */
				pool_getmessage(&agent->port, s, 4 * agent->num_dn_connections + 4 * agent->num_coord_connections + 12);
				datanodecount = pq_getmsgint(s, 4);
				for (i = 0; i < datanodecount; i++)
					datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));
				coordcount = pq_getmsgint(s, 4);
				/* It is possible that no Coordinators are involved in the transaction */
				for (i = 0; i < coordcount; i++)
					coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));
				pq_getmsgend(s);
				/* Send local commands if any to the nodes involved in the transaction */
				res = send_local_commands(agent, datanodelist, coordlist);
				/* Send result */
				pool_sendres(&agent->port, res);

				list_free(datanodelist);
				list_free(coordlist);
				break;
#endif
			case 'c':			/* CONNECT */
				pool_getmessage(&agent->port, s, 0);
				agent->pid = pq_getmsgint(s, 4);
				len = pq_getmsgint(s, 4);
				database = pq_getmsgbytes(s, len);
				len = pq_getmsgint(s, 4);
				user_name = pq_getmsgbytes(s, len);
#ifndef XCP
				len = pq_getmsgint(s, 4);
				pgoptions = pq_getmsgbytes(s, len);
#endif
				/*
				 * Coordinator pool is not initialized.
				 * With that it would be impossible to create a Database by default.
				 */
#ifdef XCP
				agent_init(agent, database, user_name);
#else
				agent_init(agent, database, user_name, pgoptions);
#endif
				pq_getmsgend(s);
				break;
			case 'd':			/* DISCONNECT */
				pool_getmessage(&agent->port, s, 4);
				agent_destroy(agent);
				pq_getmsgend(s);
				break;
			case 'f':			/* CLEAN CONNECTION */
				pool_getmessage(&agent->port, s, 0);
				datanodecount = pq_getmsgint(s, 4);
				/* It is possible to clean up only Coordinators connections */
				for (i = 0; i < datanodecount; i++)
				{
					/* Translate index to Oid */
					int index = pq_getmsgint(s, 4);
					Oid node = agent->dn_conn_oids[index];
					nodelist = lappend_oid(nodelist, node);
				}
				coordcount = pq_getmsgint(s, 4);
				/* It is possible to clean up only Datanode connections */
				for (i = 0; i < coordcount; i++)
				{
					/* Translate index to Oid */
					int index = pq_getmsgint(s, 4);
					Oid node = agent->coord_conn_oids[index];
					nodelist = lappend_oid(nodelist, node);
				}
				len = pq_getmsgint(s, 4);
				if (len > 0)
					database = pq_getmsgbytes(s, len);
				len = pq_getmsgint(s, 4);
				if (len > 0)
					user_name = pq_getmsgbytes(s, len);

				pq_getmsgend(s);

				/* Clean up connections here */
				res = clean_connection(nodelist, database, user_name);

				list_free(nodelist);

				/* Send success result */
				pool_sendres(&agent->port, res);
				break;
			case 'g':			/* GET CONNECTIONS */
				/*
				 * Length of message is caused by:
				 * - Message header = 4bytes
				 * - List of Datanodes = NumPoolDataNodes * 4bytes (max)
				 * - List of Coordinators = NumPoolCoords * 4bytes (max)
				 * - Number of Datanodes sent = 4bytes
				 * - Number of Coordinators sent = 4bytes
				 * It is better to send in a same message the list of Co and Dn at the same
				 * time, this permits to reduce interactions between postmaster and pooler
				 */
				pool_getmessage(&agent->port, s, 4 * agent->num_dn_connections + 4 * agent->num_coord_connections + 12);
				datanodecount = pq_getmsgint(s, 4);
				for (i = 0; i < datanodecount; i++)
					datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));
				coordcount = pq_getmsgint(s, 4);
				/* It is possible that no Coordinators are involved in the transaction */
				for (i = 0; i < coordcount; i++)
					coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));
				pq_getmsgend(s);

				/*
				 * In case of error agent_acquire_connections will log
				 * the error and return NULL
				 */
				fds = agent_acquire_connections(agent, datanodelist, coordlist);
				list_free(datanodelist);
				list_free(coordlist);

				pool_sendfds(&agent->port, fds, fds ? datanodecount + coordcount : 0);
				if (fds)
					pfree(fds);
				break;

			case 'h':			/* Cancel SQL Command in progress on specified connections */
				/*
				 * Length of message is caused by:
				 * - Message header = 4bytes
				 * - List of Datanodes = NumPoolDataNodes * 4bytes (max)
				 * - List of Coordinators = NumPoolCoords * 4bytes (max)
				 * - Number of Datanodes sent = 4bytes
				 * - Number of Coordinators sent = 4bytes
				 */
				pool_getmessage(&agent->port, s, 4 * agent->num_dn_connections + 4 * agent->num_coord_connections + 12);
				datanodecount = pq_getmsgint(s, 4);
				for (i = 0; i < datanodecount; i++)
					datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));
				coordcount = pq_getmsgint(s, 4);
				/* It is possible that no Coordinators are involved in the transaction */
				for (i = 0; i < coordcount; i++)
					coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));
				pq_getmsgend(s);

				cancel_query_on_connections(agent, datanodelist, coordlist);
				list_free(datanodelist);
				list_free(coordlist);
				break;
			case 'o':			/* Lock/unlock pooler */
				pool_getmessage(&agent->port, s, 8);
				is_pool_locked = pq_getmsgint(s, 4);
				pq_getmsgend(s);
				break;
			case 'p':			/* Reload connection info */
				/*
				 * Connection information reloaded concerns all the database pools.
				 * A database pool is reloaded as follows for each remote node:
				 * - node pool is deleted if the node has been deleted from catalog.
				 *   Subsequently all its connections are dropped.
				 * - node pool is deleted if its port or host information is changed.
				 *   Subsequently all its connections are dropped.
				 * - node pool is kept unchanged with existing connection information
				 *   is not changed. However its index position in node pool is changed
				 *   according to the alphabetical order of the node name in new
				 *   cluster configuration.
				 * Backend sessions are responsible to reconnect to the pooler to update
				 * their agent with newest connection information.
				 * The session invocating connection information reload is reconnected
				 * and uploaded automatically after database pool reload.
				 * Other server sessions are signaled to reconnect to pooler and update
				 * their connection information separately.
				 * During reload process done internally on pooler, pooler is locked
				 * to forbid new connection requests.
				 */
				pool_getmessage(&agent->port, s, 4);
				pq_getmsgend(s);

				/* First update all the pools */
				reload_database_pools(agent);
				break;
			case 'q':			/* Check connection info consistency */
				pool_getmessage(&agent->port, s, 4);
				pq_getmsgend(s);

				/* Check cached info consistency */
				res = node_info_check(agent);

				/* Send result */
				pool_sendres(&agent->port, res);
				break;
			case 'r':			/* RELEASE CONNECTIONS */
#ifdef XCP
				{
					bool destroy;

					pool_getmessage(&agent->port, s, 8);
					destroy = (bool) pq_getmsgint(s, 4);
					pq_getmsgend(s);
					agent_release_connections(agent, destroy);
				}
#else
				pool_getmessage(&agent->port, s, 4);
				pq_getmsgend(s);
				agent_release_connections(agent, false);
#endif
				break;
#ifndef XCP
			case 's':			/* Session-related COMMAND */
				pool_getmessage(&agent->port, s, 0);
				/* Determine if command is local or session */
				command_type = (PoolCommandType) pq_getmsgint(s, 4);
				/* Get the SET command if necessary */
				len = pq_getmsgint(s, 4);
				if (len != 0)
					set_command = pq_getmsgbytes(s, len);

				pq_getmsgend(s);

				/* Manage command depending on its type */
				res = agent_session_command(agent, set_command, command_type);

				/* Send success result */
				pool_sendres(&agent->port, res);
				break;
#endif
			default:			/* EOF or protocol violation */
				agent_destroy(agent);
				return;
		}
		/* avoid reading from connection */
		if ((qtype = pool_pollbyte(&agent->port)) == EOF)
			break;
	}
}

#ifndef XCP
/*
 * Manage a session command for pooler
 */
static int
agent_session_command(PoolAgent *agent, const char *set_command, PoolCommandType command_type)
{
	int res;

	switch (command_type)
	{
		case POOL_CMD_LOCAL_SET:
		case POOL_CMD_GLOBAL_SET:
			res = agent_set_command(agent, set_command, command_type);
			break;
		case POOL_CMD_TEMP:
			res = agent_temp_command(agent);
			break;
		default:
			res = -1;
			break;
	}

	return res;
}

/*
 * Set agent flag that a temporary object is in use.
 */
static int
agent_temp_command(PoolAgent *agent)
{
	agent->is_temp = true;
	return 0;
}

/*
 * Save a SET command and distribute it to the agent connections
 * already in use.
 */
static int
agent_set_command(PoolAgent *agent, const char *set_command, PoolCommandType command_type)
{
	char   *params_string;
	int		i;
	int		res = 0;

	Assert(agent);
	Assert(set_command);
	Assert(command_type == POOL_CMD_LOCAL_SET || command_type == POOL_CMD_GLOBAL_SET);

	if (command_type == POOL_CMD_LOCAL_SET)
		params_string = agent->local_params;
	else if (command_type == POOL_CMD_GLOBAL_SET)
		params_string = agent->session_params;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Set command process failed")));

	/* First command recorded */
	if (!params_string)
	{
		params_string = pstrdup(set_command);
		if (!params_string)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
	}
	else
	{
		/*
		 * Second command or more recorded.
		 * Commands are saved with format 'SET param1 TO value1;...;SET paramN TO valueN'
		 */
		params_string = (char *) repalloc(params_string,
										  strlen(params_string) + strlen(set_command) + 2);
		if (!params_string)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));

		sprintf(params_string, "%s;%s", params_string, set_command);
	}

	/*
	 * Launch the new command to all the connections already hold by the agent
	 * It does not matter if command is local or global as this has explicitely been sent
	 * by client. PostgreSQL backend also cannot send to its pooler agent SET LOCAL if current
	 * transaction is not in a transaction block. This has also no effect on local Coordinator
	 * session.
	 */
	for (i = 0; i < agent->num_dn_connections; i++)
	{
		if (agent->dn_connections[i])
			res |= PGXCNodeSendSetQuery(agent->dn_connections[i]->conn, set_command);
	}

	for (i = 0; i < agent->num_coord_connections; i++)
	{
		if (agent->coord_connections[i])
			res |= PGXCNodeSendSetQuery(agent->coord_connections[i]->conn, set_command);
	}

	/* Save the latest string */
	if (command_type == POOL_CMD_LOCAL_SET)
		agent->local_params = params_string;
	else if (command_type == POOL_CMD_GLOBAL_SET)
		agent->session_params = params_string;

	return res;
}
#endif

/*
 * acquire connection
 */
static int *
agent_acquire_connections(PoolAgent *agent, List *datanodelist, List *coordlist)
{
	int			i;
	int		   *result;
	ListCell   *nodelist_item;
	MemoryContext oldcontext;

	Assert(agent);

	/* Check if pooler can accept those requests */
	if (list_length(datanodelist) > agent->num_dn_connections ||
			list_length(coordlist) > agent->num_coord_connections)
		return NULL;

	/*
	 * Allocate memory
	 * File descriptors of Datanodes and Coordinators are saved in the same array,
	 * This array will be sent back to the postmaster.
	 * It has a length equal to the length of the Datanode list
	 * plus the length of the Coordinator list.
	 * Datanode fds are saved first, then Coordinator fds are saved.
	 */
	result = (int *) palloc((list_length(datanodelist) + list_length(coordlist)) * sizeof(int));
	if (result == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/*
	 * There are possible memory allocations in the core pooler, we want
	 * these allocations in the contect of the database pool
	 */
	oldcontext = MemoryContextSwitchTo(agent->pool->mcxt);


	/* Initialize result */
	i = 0;
	/* Save in array fds of Datanodes first */
	foreach(nodelist_item, datanodelist)
	{
		int			node = lfirst_int(nodelist_item);

		/* Acquire from the pool if none */
		if (agent->dn_connections[node] == NULL)
		{
			PGXCNodePoolSlot *slot = acquire_connection(agent->pool,
														agent->dn_conn_oids[node]);

			/* Handle failure */
			if (slot == NULL)
			{
				pfree(result);
				MemoryContextSwitchTo(oldcontext);
				return NULL;
			}

			/* Store in the descriptor */
			agent->dn_connections[node] = slot;

			/*
			 * Update newly-acquired slot with session parameters.
			 * Local parameters are fired only once BEGIN has been launched on
			 * remote nodes.
			 */
#ifndef XCP
			if (agent->session_params)
				PGXCNodeSendSetQuery(slot->conn, agent->session_params);
#endif
		}

		result[i++] = PQsocket((PGconn *) agent->dn_connections[node]->conn);
	}

	/* Save then in the array fds for Coordinators */
	foreach(nodelist_item, coordlist)
	{
		int			node = lfirst_int(nodelist_item);

		/* Acquire from the pool if none */
		if (agent->coord_connections[node] == NULL)
		{
			PGXCNodePoolSlot *slot = acquire_connection(agent->pool, agent->coord_conn_oids[node]);

			/* Handle failure */
			if (slot == NULL)
			{
				pfree(result);
				MemoryContextSwitchTo(oldcontext);
				return NULL;
			}

			/* Store in the descriptor */
			agent->coord_connections[node] = slot;

			/*
			 * Update newly-acquired slot with session parameters.
			 * Local parameters are fired only once BEGIN has been launched on
			 * remote nodes.
			 */
#ifndef XCP
			if (agent->session_params)
				PGXCNodeSendSetQuery(slot->conn, agent->session_params);
#endif
		}

		result[i++] = PQsocket((PGconn *) agent->coord_connections[node]->conn);
	}

	MemoryContextSwitchTo(oldcontext);

	return result;
}

#ifndef XCP
/*
 * send transaction local commands if any, set the begin sent status in any case
 */
static int
send_local_commands(PoolAgent *agent, List *datanodelist, List *coordlist)
{
	int			tmp;
	int			res;
	ListCell		*nodelist_item;
	PGXCNodePoolSlot	*slot;

	Assert(agent);

	res = 0;

	if (datanodelist != NULL)
	{
		res = list_length(datanodelist);
		if (res > 0 && agent->dn_connections == NULL)
			return 0;

		foreach(nodelist_item, datanodelist)
		{
			int	node = lfirst_int(nodelist_item);

			if(node < 0 || node >= agent->num_dn_connections)
				continue;

			slot = agent->dn_connections[node];

			if (slot == NULL)
				continue;

			if (agent->local_params != NULL)
			{
				tmp = PGXCNodeSendSetQuery(slot->conn, agent->local_params);
				res = res + tmp;
			}
		}
	}

	if (coordlist != NULL)
	{
		res = list_length(coordlist);
		if (res > 0 && agent->coord_connections == NULL)
			return 0;

		foreach(nodelist_item, coordlist)
		{
			int	node = lfirst_int(nodelist_item);

			if(node < 0 || node >= agent->num_coord_connections)
				continue;

			slot = agent->coord_connections[node];

			if (slot == NULL)
				continue;

			if (agent->local_params != NULL)
			{
				tmp = PGXCNodeSendSetQuery(slot->conn, agent->local_params);
				res = res + tmp;
			}
		}
	}

	if (res < 0)
		return -res;
	return 0;
}
#endif

/*
 * Cancel query
 */
static int
cancel_query_on_connections(PoolAgent *agent, List *datanodelist, List *coordlist)
{
	ListCell	*nodelist_item;
	char		errbuf[256];
	int		nCount;
	bool		bRet;

	nCount = 0;

	if (agent == NULL)
		return nCount;

	/* Send cancel on Datanodes first */
	foreach(nodelist_item, datanodelist)
	{
		int	node = lfirst_int(nodelist_item);

		if(node < 0 || node >= agent->num_dn_connections)
			continue;

		if (agent->dn_connections == NULL)
			break;

		bRet = PQcancel((PGcancel *) agent->dn_connections[node]->xc_cancelConn, errbuf, sizeof(errbuf));
		if (bRet != false)
		{
			nCount++;
		}
	}

	/* Send cancel to Coordinators too, e.g. if DDL was in progress */
	foreach(nodelist_item, coordlist)
	{
		int	node = lfirst_int(nodelist_item);

		if(node < 0 || node >= agent->num_coord_connections)
			continue;

		if (agent->coord_connections == NULL)
			break;

		bRet = PQcancel((PGcancel *) agent->coord_connections[node]->xc_cancelConn, errbuf, sizeof(errbuf));
		if (bRet != false)
		{
			nCount++;
		}
	}

	return nCount;
}

/*
 * Return connections back to the pool
 */
#ifdef XCP
void
PoolManagerReleaseConnections(bool force)
{
	char msgtype = 'r';
	int n32;
	int msglen = 8;

	/* If disconnected from pooler all the connections already released */
	if (!poolHandle)
		return;

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	n32 = htonl(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Lock information */
	n32 = htonl((int) force);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);
	pool_flush(&poolHandle->port);
}
#else
void
PoolManagerReleaseConnections(void)
{
	Assert(poolHandle);
	pool_putmessage(&poolHandle->port, 'r', NULL, 0);
	pool_flush(&poolHandle->port);
}
#endif


/*
 * Cancel Query
 */
void
PoolManagerCancelQuery(int dn_count, int* dn_list, int co_count, int* co_list)
{
	uint32		n32;
	/*
	 * Buffer contains the list of both Coordinator and Datanodes, as well
	 * as the number of connections
	 */
	uint32 		buf[2 + dn_count + co_count];
	int 		i;

	if (poolHandle == NULL)
		return;

	if (dn_count == 0 && co_count == 0)
		return;

	if (dn_count != 0 && dn_list == NULL)
		return;

	if (co_count != 0 && co_list == NULL)
		return;

	/* Insert the list of Datanodes in buffer */
	n32 = htonl((uint32) dn_count);
	buf[0] = n32;

	for (i = 0; i < dn_count;)
	{
		n32 = htonl((uint32) dn_list[i++]);
		buf[i] = n32;
	}

	/* Insert the list of Coordinators in buffer */
	n32 = htonl((uint32) co_count);
	buf[dn_count + 1] = n32;

	/* Not necessary to send to pooler a request if there is no Coordinator */
	if (co_count != 0)
	{
		for (i = dn_count + 1; i < (dn_count + co_count + 1);)
		{
			n32 = htonl((uint32) co_list[i - (dn_count + 1)]);
			buf[++i] = n32;
		}
	}
	pool_putmessage(&poolHandle->port, 'h', (char *) buf, (2 + dn_count + co_count) * sizeof(uint32));
	pool_flush(&poolHandle->port);
}

/*
 * Release connections for Datanodes and Coordinators
 */
static void
agent_release_connections(PoolAgent *agent, bool force_destroy)
{
	MemoryContext oldcontext;
	int			i;

	if (!agent->dn_connections && !agent->coord_connections)
		return;
#ifdef XCP
	if (!force_destroy && cluster_ex_lock_held)
	{
		elog(LOG, "Not releasing connection with cluster lock");
		return;
	}
#endif

#ifndef XCP
	/*
	 * If there are some session parameters or temporary objects,
	 * do not put back connections to pool.
	 * Disconnection will be made when session is cut for this user.
	 * Local parameters are reset when transaction block is finished,
	 * so don't do anything for them, but just reset their list.
	 */
	if (agent->local_params)
	{
		pfree(agent->local_params);
		agent->local_params = NULL;
	}
	if ((agent->session_params || agent->is_temp) && !force_destroy)
		return;
#endif

	/*
	 * There are possible memory allocations in the core pooler, we want
	 * these allocations in the contect of the database pool
	 */
	oldcontext = MemoryContextSwitchTo(agent->pool->mcxt);

	/*
	 * Remaining connections are assumed to be clean.
	 * First clean up for Datanodes
	 */
	for (i = 0; i < agent->num_dn_connections; i++)
	{
		PGXCNodePoolSlot *slot = agent->dn_connections[i];

		/*
		 * Release connection.
		 * If connection has temporary objects on it, destroy connection slot.
		 */
		if (slot)
			release_connection(agent->pool, slot, agent->dn_conn_oids[i], force_destroy);
		agent->dn_connections[i] = NULL;
	}
	/* Then clean up for Coordinator connections */
	for (i = 0; i < agent->num_coord_connections; i++)
	{
		PGXCNodePoolSlot *slot = agent->coord_connections[i];

		/*
		 * Release connection.
		 * If connection has temporary objects on it, destroy connection slot.
		 */
		if (slot)
			release_connection(agent->pool, slot, agent->coord_conn_oids[i], force_destroy);
		agent->coord_connections[i] = NULL;
	}

#ifdef XCP
	/*
	 * Released connections are now in the pool and we may want to close
	 * them eventually. Update the oldest_idle value to reflect the latest
	 * last access time if not already updated..
	 */
	if (!force_destroy && agent->pool->oldest_idle == (time_t) 0)
		agent->pool->oldest_idle = time(NULL);
#endif

	MemoryContextSwitchTo(oldcontext);
}


#ifndef XCP
/*
 * Reset session parameters for given connections in the agent.
 * This is done before putting back to pool connections that have been
 * modified by session parameters.
 */
static void
agent_reset_session(PoolAgent *agent)
{
	int			i;

	if (!agent->session_params && !agent->local_params)
		return;

	/* Reset connection params */
	/* Check agent slot for each Datanode */
	if (agent->dn_connections)
	{
		for (i = 0; i < agent->num_dn_connections; i++)
		{
			PGXCNodePoolSlot *slot = agent->dn_connections[i];

			/* Reset given slot with parameters */
			if (slot)
				PGXCNodeSendSetQuery(slot->conn, "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;SET GLOBAL_SESSION TO NONE;");
		}
	}

	if (agent->coord_connections)
	{
		/* Check agent slot for each Coordinator */
		for (i = 0; i < agent->num_coord_connections; i++)
		{
			PGXCNodePoolSlot *slot = agent->coord_connections[i];

			/* Reset given slot with parameters */
			if (slot)
				PGXCNodeSendSetQuery(slot->conn, "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;SET GLOBAL_SESSION TO NONE;");
		}
	}

	/* Parameters are reset, so free commands */
	if (agent->session_params)
	{
		pfree(agent->session_params);
		agent->session_params = NULL;
	}
	if (agent->local_params)
	{
		pfree(agent->local_params);
		agent->local_params = NULL;
	}
}
#endif


/*
 * Create new empty pool for a database.
 * By default Database Pools have a size null so as to avoid interactions
 * between PGXC nodes in the cluster (Co/Co, Dn/Dn and Co/Dn).
 * Pool is increased at the first GET_CONNECTION message received.
 * Returns POOL_OK if operation succeed POOL_FAIL in case of OutOfMemory
 * error and POOL_WEXIST if poll for this database already exist.
 */
#ifdef XCP
static DatabasePool *create_database_pool(const char *database,
										   const char *user_name)
#else
static DatabasePool *
create_database_pool(const char *database, const char *user_name, const char *pgoptions)
#endif
{
	MemoryContext	oldcontext;
	MemoryContext	dbcontext;
	DatabasePool   *databasePool;
	HASHCTL			hinfo;
	int				hflags;

	dbcontext = AllocSetContextCreate(PoolerCoreContext,
									  "DB Context",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(dbcontext);
	/* Allocate memory */
	databasePool = (DatabasePool *) palloc(sizeof(DatabasePool));
	if (!databasePool)
	{
		/* out of memory */
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		return NULL;
	}

	databasePool->mcxt = dbcontext;
	 /* Copy the database name */
	databasePool->database = pstrdup(database);
	 /* Copy the user name */
	databasePool->user_name = pstrdup(user_name);
#ifdef XCP
	/* Reset the oldest_idle value */
	databasePool->oldest_idle = (time_t) 0;
#else
	 /* Copy the pgoptions */
	databasePool->pgoptions = pstrdup(pgoptions);
#endif

	if (!databasePool->database)
	{
		/* out of memory */
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		pfree(databasePool);
		return NULL;
	}

	/* Init next reference */
	databasePool->next = NULL;

	/* Init node hashtable */
	MemSet(&hinfo, 0, sizeof(hinfo));
	hflags = 0;

	hinfo.keysize = sizeof(Oid);
	hinfo.entrysize = sizeof(PGXCNodePool);
	hflags |= HASH_ELEM;

	hinfo.hcxt = dbcontext;
	hflags |= HASH_CONTEXT;

	databasePool->nodePools = hash_create("Node Pool", MaxDataNodes + MaxCoords,
										  &hinfo, hflags);

	MemoryContextSwitchTo(oldcontext);

	/* Insert into the list */
	insert_database_pool(databasePool);

	return databasePool;
}


/*
 * Destroy the pool and free memory
 */
static int
destroy_database_pool(const char *database, const char *user_name)
{
	DatabasePool *databasePool;

	/* Delete from the list */
	databasePool = remove_database_pool(database, user_name);
	if (databasePool)
	{
		HASH_SEQ_STATUS hseq_status;
		PGXCNodePool   *nodePool;

		hash_seq_init(&hseq_status, databasePool->nodePools);
		while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
		{
			destroy_node_pool(nodePool);
		}
		/* free allocated memory */
		MemoryContextDelete(databasePool->mcxt);
		return 1;
	}
	return 0;
}


/*
 * Insert new database pool to the list
 */
static void
insert_database_pool(DatabasePool *databasePool)
{
	Assert(databasePool);

	/* Reference existing list or null the tail */
	if (databasePools)
		databasePool->next = databasePools;
	else
		databasePool->next = NULL;

	/* Update head pointer */
	databasePools = databasePool;
}

/*
 * Rebuild information of database pools
 */
static void
reload_database_pools(PoolAgent *agent)
{
	DatabasePool *databasePool;

	/*
	 * Release node connections if any held. It is not guaranteed client session
	 * does the same so don't ever try to return them to pool and reuse
	 */
	agent_release_connections(agent, true);

	/* Forget previously allocated node info */
	MemoryContextReset(agent->mcxt);

	/* and allocate new */
	PgxcNodeGetOids(&agent->coord_conn_oids, &agent->dn_conn_oids,
					&agent->num_coord_connections, &agent->num_dn_connections, false);

	agent->coord_connections = (PGXCNodePoolSlot **)
			palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot *));
	agent->dn_connections = (PGXCNodePoolSlot **)
			palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot *));

	/*
	 * Scan the list and destroy any altered pool. They will be recreated
	 * upon subsequent connection acquisition.
	 */
	databasePool = databasePools;
	while (databasePool)
	{
		/* Update each database pool slot with new connection information */
		HASH_SEQ_STATUS hseq_status;
		PGXCNodePool   *nodePool;

		hash_seq_init(&hseq_status, databasePool->nodePools);
		while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
		{
			char *connstr_chk = build_node_conn_str(nodePool->nodeoid, databasePool);

			if (connstr_chk == NULL || strcmp(connstr_chk, nodePool->connstr))
			{
				/* Node has been removed or altered */
				destroy_node_pool(nodePool);
				hash_search(databasePool->nodePools, &nodePool->nodeoid,
							HASH_REMOVE, NULL);
			}

			if (connstr_chk)
				pfree(connstr_chk);
		}

		databasePool = databasePool->next;
	}
}


/*
 * Find pool for specified database and username in the list
 */
#ifdef XCP
static DatabasePool *
find_database_pool(const char *database,
										 const char *user_name)
#else
static DatabasePool *
find_database_pool(const char *database, const char *user_name, const char *pgoptions)
#endif
{
	DatabasePool *databasePool;

	/* Scan the list */
	databasePool = databasePools;
	while (databasePool)
	{
#ifdef XCP
		if (strcmp(database, databasePool->database) == 0 &&
				strcmp(user_name, databasePool->user_name) == 0)
			break;
#else
		if (strcmp(database, databasePool->database) == 0 &&
			strcmp(user_name, databasePool->user_name) == 0 &&
			strcmp(pgoptions, databasePool->pgoptions) == 0)
			break;
#endif
		databasePool = databasePool->next;
	}
	return databasePool;
}


/*
 * Remove pool for specified database from the list
 */
static DatabasePool *
remove_database_pool(const char *database, const char *user_name)
{
	DatabasePool *databasePool,
			   *prev;

	/* Scan the list */
	databasePool = databasePools;
	prev = NULL;
	while (databasePool)
	{

		/* if match break the loop and return */
		if (strcmp(database, databasePool->database) == 0 &&
			strcmp(user_name, databasePool->user_name) == 0)
			break;
		prev = databasePool;
		databasePool = databasePool->next;
	}

	/* if found */
	if (databasePool)
	{

		/* Remove entry from chain or update head */
		if (prev)
			prev->next = databasePool->next;
		else
			databasePools = databasePool->next;


		databasePool->next = NULL;
	}
	return databasePool;
}

/*
 * Acquire connection
 */
static PGXCNodePoolSlot *
acquire_connection(DatabasePool *dbPool, Oid node)
{
	PGXCNodePool	   *nodePool;
	PGXCNodePoolSlot   *slot;

	Assert(dbPool);

	nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &node, HASH_FIND,
											NULL);

	/*
	 * When a Coordinator pool is initialized by a Coordinator Postmaster,
	 * it has a NULL size and is below minimum size that is 1
	 * This is to avoid problems of connections between Coordinators
	 * when creating or dropping Databases.
	 */
	if (nodePool == NULL || nodePool->freeSize == 0)
		nodePool = grow_pool(dbPool, node);

	slot = NULL;
	/* Check available connections */
	while (nodePool && nodePool->freeSize > 0)
	{
		int			poll_result;

		slot = nodePool->slot[--(nodePool->freeSize)];

	retry:
		/*
		 * Make sure connection is ok, destroy connection slot if there is a
		 * problem.
		 */
		poll_result = pqReadReady((PGconn *) slot->conn);

		if (poll_result == 0)
			break; 		/* ok, no data */
		else if (poll_result < 0)
		{
			if (errno == EAGAIN || errno == EINTR)
				goto retry;

			elog(WARNING, "Error in checking connection, errno = %d", errno);
		}
		else
			elog(WARNING, "Unexpected data on connection, cleaning.");

		destroy_slot(slot);
		slot = NULL;

		/* Decrement current max pool size */
		(nodePool->size)--;
		/* Ensure we are not below minimum size */
		nodePool = grow_pool(dbPool, node);
	}

	if (slot == NULL)
		elog(WARNING, "can not connect to node %u", node);

	return slot;
}


/*
 * release connection from specified pool and slot
 */
static void
release_connection(DatabasePool *dbPool, PGXCNodePoolSlot *slot,
				   Oid node, bool force_destroy)
{
	PGXCNodePool *nodePool;

	Assert(dbPool);
	Assert(slot);

	nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &node, HASH_FIND,
											NULL);
	if (nodePool == NULL)
	{
		/*
		 * The node may be altered or dropped.
		 * In any case the slot is no longer valid.
		 */
		destroy_slot(slot);
		return;
	}

	/* return or discard */
	if (!force_destroy)
	{
		/* Insert the slot into the array and increase pool size */
		nodePool->slot[(nodePool->freeSize)++] = slot;
#ifdef XCP
		slot->released = time(NULL);
#endif
	}
	else
	{
		elog(DEBUG1, "Cleaning up connection from pool %s, closing", nodePool->connstr);
		destroy_slot(slot);
		/* Decrement pool size */
		(nodePool->size)--;
		/* Ensure we are not below minimum size */
		grow_pool(dbPool, node);
	}
}


/*
 * Increase database pool size, create new if does not exist
 */
static PGXCNodePool *
grow_pool(DatabasePool *dbPool, Oid node)
{
#ifdef XCP
	/* if error try to release idle connections and try again */
	bool 			tryagain = true;
#endif
	PGXCNodePool   *nodePool;
	bool			found;

	Assert(dbPool);

	nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &node,
											HASH_ENTER, &found);
	if (!found)
	{
		nodePool->connstr = build_node_conn_str(node, dbPool);
		if (!nodePool->connstr)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not build connection string for node %u", node)));
		}

		nodePool->slot = (PGXCNodePoolSlot **) palloc0(MaxPoolSize * sizeof(PGXCNodePoolSlot *));
		if (!nodePool->slot)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}
		nodePool->freeSize = 0;
		nodePool->size = 0;
	}

#ifdef XCP
	while (nodePool->freeSize == 0 && nodePool->size < MaxPoolSize)
#else
	while (nodePool->size < MinPoolSize || (nodePool->freeSize == 0 && nodePool->size < MaxPoolSize))
#endif
	{
		PGXCNodePoolSlot *slot;

		/* Allocate new slot */
		slot = (PGXCNodePoolSlot *) palloc(sizeof(PGXCNodePoolSlot));
		if (slot == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		/* If connection fails, be sure that slot is destroyed cleanly */
		slot->xc_cancelConn = NULL;

		/* Establish connection */
		slot->conn = PGXCNodeConnect(nodePool->connstr);
		if (!PGXCNodeConnected(slot->conn))
		{
			destroy_slot(slot);
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("failed to connect to Datanode")));
#ifdef XCP
			/*
			 * If we failed to connect probably number of connections on the
			 * target node reached max_connections. Try and release idle
			 * connections and try again.
			 * We do not want to enter endless loop here and run maintenance
			 * procedure only once.
			 * It is not safe to run the maintenance procedure if no connections
			 * from that pool currently in use - the node pool may be destroyed
			 * in that case.
			 */
			if (tryagain && nodePool->size > nodePool->freeSize)
			{
				pools_maintenance();
				tryagain = false;
				continue;
			}
#endif
			break;
		}

		slot->xc_cancelConn = (NODE_CANCEL *) PQgetCancel((PGconn *)slot->conn);
#ifdef XCP
		slot->released = time(NULL);
		if (dbPool->oldest_idle == (time_t) 0)
			dbPool->oldest_idle = slot->released;
#endif

		/* Insert at the end of the pool */
		nodePool->slot[(nodePool->freeSize)++] = slot;

		/* Increase count of pool size */
		(nodePool->size)++;
		elog(DEBUG1, "Pooler: increased pool size to %d for pool %s",
			 nodePool->size,
			 nodePool->connstr);
	}
	return nodePool;
}


/*
 * Destroy pool slot
 */
static void
destroy_slot(PGXCNodePoolSlot *slot)
{
	if (!slot)
		return;

	PQfreeCancel((PGcancel *)slot->xc_cancelConn);
	PGXCNodeClose(slot->conn);
	pfree(slot);
}


/*
 * Destroy node pool
 */
static void
destroy_node_pool(PGXCNodePool *node_pool)
{
	int			i;

	if (!node_pool)
		return;

	/*
	 * At this point all agents using connections from this pool should be already closed
	 * If this not the connections to the Datanodes assigned to them remain open, this will
	 * consume Datanode resources.
	 */
	elog(DEBUG1, "About to destroy node pool %s, current size is %d, %d connections are in use",
		 node_pool->connstr, node_pool->freeSize, node_pool->size - node_pool->freeSize);
	if (node_pool->connstr)
		pfree(node_pool->connstr);

	if (node_pool->slot)
	{
		for (i = 0; i < node_pool->freeSize; i++)
			destroy_slot(node_pool->slot[i]);
		pfree(node_pool->slot);
	}
}


/*
 * Main handling loop
 */
static void
PoolerLoop(void)
{
	StringInfoData 	input_message;
#ifdef XCP
	time_t			last_maintenance = (time_t) 0;
#endif

	server_fd = pool_listen(PoolerPort, UnixSocketDir);
	if (server_fd == -1)
	{
		/* log error */
		return;
	}
	initStringInfo(&input_message);

	for (;;)
	{
		int			nfds;
		fd_set		rfds;
		int			retval;
		int			i;

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (!PostmasterIsAlive())
			exit(1);

		/* watch for incoming connections */
		FD_ZERO(&rfds);
		FD_SET(server_fd, &rfds);

		nfds = server_fd;

		/* watch for incoming messages */
		for (i = 0; i < agentCount; i++)
		{
			PoolAgent  *agent = poolAgents[i];
			int			sockfd = Socket(agent->port);
			FD_SET		(sockfd, &rfds);

			nfds = Max(nfds, sockfd);
		}

#ifdef XCP
		if (PoolMaintenanceTimeout > 0)
		{
			struct timeval	maintenance_timeout;
			int				timeout_val;
			double			timediff;

			/*
			 * Decide the timeout value based on when the last
			 * maintenance activity was carried out. If the last
			 * maintenance was done quite a while ago schedule the select
			 * with no timeout. It will serve any incoming activity
			 * and if there's none it will cause the maintenance
			 * to be scheduled as soon as possible
			 */
			timediff = difftime(time(NULL), last_maintenance);

			if (timediff > PoolMaintenanceTimeout)
				timeout_val = 0;
			else
				timeout_val = PoolMaintenanceTimeout - rint(timediff);

			maintenance_timeout.tv_sec = timeout_val;
			maintenance_timeout.tv_usec = 0;
			/* wait for event */
			retval = select(nfds + 1, &rfds, NULL, NULL, &maintenance_timeout);
		}
		else
#endif
		retval = select(nfds + 1, &rfds, NULL, NULL, NULL);
#ifdef XCP
		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (!PostmasterIsAlive())
			exit(1);

		/*
		 * Process any requests or signals received recently.
		 */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
#endif
		if (shutdown_requested)
		{
			for (i = agentCount - 1; i >= 0; i--)
			{
				PoolAgent  *agent = poolAgents[i];

				agent_destroy(agent);
			}
			while (databasePools)
				if (destroy_database_pool(databasePools->database,
										  databasePools->user_name) == 0)
					break;
			close(server_fd);
			exit(0);
		}
		if (retval > 0)
		{
			/*
			 * Agent may be removed from the array while processing
			 * and trailing items are shifted, so scroll downward
			 * to avoid problem
			 */
			for (i = agentCount - 1; i >= 0; i--)
			{
				PoolAgent  *agent = poolAgents[i];
				int			sockfd = Socket(agent->port);

				if (FD_ISSET(sockfd, &rfds))
					agent_handle_input(agent, &input_message);
			}
			if (FD_ISSET(server_fd, &rfds))
				agent_create();
		}
#ifdef XCP
		else if (retval == 0)
		{
			/* maintenance timeout */
			pools_maintenance();
			last_maintenance = time(NULL);
		}
#endif
	}
}

/*
 * Clean Connection in all Database Pools for given Datanode and Coordinator list
 */
int
clean_connection(List *node_discard, const char *database, const char *user_name)
{
	DatabasePool *databasePool;
	int			res = CLEAN_CONNECTION_COMPLETED;

	databasePool = databasePools;

	while (databasePool)
	{
		ListCell *lc;

		if ((database && strcmp(database, databasePool->database)) ||
				(user_name && strcmp(user_name, databasePool->user_name)))
		{
			/* The pool does not match to request, skip */
			databasePool = databasePool->next;
			continue;
		}

		/*
		 * Clean each requested node pool
		 */
		foreach(lc, node_discard)
		{
			PGXCNodePool *nodePool;
			Oid node = lfirst_oid(lc);

			nodePool = hash_search(databasePool->nodePools, &node, HASH_FIND,
								   NULL);

			if (nodePool)
			{
				/* Check if connections are in use */
				if (nodePool->freeSize < nodePool->size)
				{
					elog(WARNING, "Pool of Database %s is using Datanode %u connections",
								databasePool->database, node);
					res = CLEAN_CONNECTION_NOT_COMPLETED;
				}

				/* Destroy connections currently in Node Pool */
				if (nodePool->slot)
				{
					int i;
					for (i = 0; i < nodePool->freeSize; i++)
						destroy_slot(nodePool->slot[i]);
				}
				nodePool->size -= nodePool->freeSize;
				nodePool->freeSize = 0;
			}
		}

		databasePool = databasePool->next;
	}

	/* Release lock on Pooler, to allow transactions to connect again. */
	is_pool_locked = false;
	return res;
}

/*
 * Take a Lock on Pooler.
 * Abort PIDs registered with the agents for the given database.
 * Send back to client list of PIDs signaled to watch them.
 */
int *
abort_pids(int *len, int pid, const char *database, const char *user_name)
{
	int *pids = NULL;
	int i = 0;
	int count;

	Assert(!is_pool_locked);
	Assert(agentCount > 0);

	is_pool_locked = true;

	pids = (int *) palloc((agentCount - 1) * sizeof(int));

	/* Send a SIGTERM signal to all processes of Pooler agents except this one */
	for (count = 0; count < agentCount; count++)
	{
		if (poolAgents[count]->pid == pid)
			continue;

		if (database && strcmp(poolAgents[count]->pool->database, database) != 0)
			continue;

		if (user_name && strcmp(poolAgents[count]->pool->user_name, user_name) != 0)
			continue;

		if (kill(poolAgents[count]->pid, SIGTERM) < 0)
			elog(ERROR, "kill(%ld,%d) failed: %m",
						(long) poolAgents[count]->pid, SIGTERM);

		pids[i++] = poolAgents[count]->pid;
	}

	*len = i;

	return pids;
}

/*
 *
 */
static void
pooler_die(SIGNAL_ARGS)
{
	shutdown_requested = true;
}


/*
 *
 */
static void
pooler_quickdie(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);
	exit(2);
}


#ifdef XCP
static void
pooler_sighup(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}
#endif


#ifndef XCP
bool
IsPoolHandle(void)
{
	if (poolHandle == NULL)
		return false;
	return true;
}
#endif

/*
 * Given node identifier, dbname and user name build connection string.
 * Get node connection details from the shared memory node table
 */
static char *
build_node_conn_str(Oid node, DatabasePool *dbPool)
{
	NodeDefinition *nodeDef;
	char 		   *connstr;

	nodeDef = PgxcNodeGetDefinition(node);
	if (nodeDef == NULL)
	{
		/* No such definition, node is dropped? */
		return NULL;
	}

#ifdef XCP
	connstr = PGXCNodeConnStr(NameStr(nodeDef->nodehost),
							  nodeDef->nodeport,
							  dbPool->database,
							  dbPool->user_name,
							  IS_PGXC_COORDINATOR ? "coordinator" : "datanode",
							  PGXCNodeName);
#else
	connstr = PGXCNodeConnStr(NameStr(nodeDef->nodehost),
							  nodeDef->nodeport,
							  dbPool->database,
							  dbPool->user_name,
							  dbPool->pgoptions,
							  IS_PGXC_COORDINATOR ? "coordinator" : "datanode");
#endif
	pfree(nodeDef);

	return connstr;
}


#ifdef XCP
/*
 * Check all pooled connections, and close which have been released more then
 * PooledConnKeepAlive seconds ago.
 * Return true if shrink operation closed all the connections and pool can be
 * ddestroyed, false if there are still connections or pool is in use.
 */
static bool
shrink_pool(DatabasePool *pool)
{
	time_t 			now = time(NULL);
	HASH_SEQ_STATUS hseq_status;
	PGXCNodePool   *nodePool;
	int 			i;
	bool			empty = true;

	/* Negative PooledConnKeepAlive disables automatic connection cleanup */
	if (PoolConnKeepAlive < 0)
		return false;

	pool->oldest_idle = (time_t) 0;
	hash_seq_init(&hseq_status, pool->nodePools);
	while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
	{
		/* Go thru the free slots and destroy those that are free too long */
		for (i = 0; i < nodePool->freeSize; )
		{
			PGXCNodePoolSlot *slot = nodePool->slot[i];

			if (difftime(now, slot->released) > PoolConnKeepAlive)
			{
				/* connection is idle for long, close it */
				destroy_slot(slot);
				/* reduce pool size and total number of connections */
				(nodePool->freeSize)--;
				(nodePool->size)--;
				/* move last connection in place, if not at last already */
				if (i < nodePool->freeSize)
					nodePool->slot[i] = nodePool->slot[nodePool->freeSize];
			}
			else
			{
				if (pool->oldest_idle == (time_t) 0 ||
						difftime(pool->oldest_idle, slot->released) > 0)
					pool->oldest_idle = slot->released;

				i++;
			}
		}
		if (nodePool->size > 0)
			empty = false;
		else
		{
			destroy_node_pool(nodePool);
			hash_search(pool->nodePools, &nodePool->nodeoid, HASH_REMOVE, NULL);
		}
	}

	/*
	 * Last check, if any active agent is referencing the pool do not allow to
	 * destroy it, because there will be a problem if session wakes up and try
	 * to get a connection from non existing pool.
	 * If all such sessions will eventually disconnect the pool will be
	 * destroyed during next maintenance procedure.
	 */
	if (empty)
	{
		for (i = 0; i < agentCount; i++)
		{
			if (poolAgents[i]->pool == pool)
				return false;
		}
	}

	return empty;
}


/*
 * Scan connection pools and release connections which are idle for long.
 * If pool gets empty after releasing connections it is destroyed.
 */
static void
pools_maintenance(void)
{
	DatabasePool   *prev = NULL;
	DatabasePool   *curr = databasePools;
	time_t			now = time(NULL);
	int				count = 0;

	/* Iterate over the pools */
	while (curr)
	{
		/*
		 * If current pool has connections to close and it is emptied after
		 * shrink remove the pool and free memory.
		 * Otherwithe move to next pool.
		 */
		if (curr->oldest_idle != (time_t) 0 &&
				difftime(now, curr->oldest_idle) > PoolConnKeepAlive &&
				shrink_pool(curr))
		{
			MemoryContext mem = curr->mcxt;
			curr = curr->next;
			if (prev)
				prev->next = curr;
			else
				databasePools = curr;
			MemoryContextDelete(mem);
			count++;
		}
		else
		{
			prev = curr;
			curr = curr->next;
		}
	}
	elog(DEBUG1, "Pool maintenance, done in %f seconds, removed %d pools",
			difftime(time(NULL), now), count);
}
#endif
