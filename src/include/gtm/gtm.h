/*-------------------------------------------------------------------------
 *
 * gtm.h
 *
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GTM_H
#define _GTM_H

#include <setjmp.h>

#include "gtm/gtm_c.h"
#include "gtm/gtm_common.h"
#include "gtm/palloc.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_conn.h"
#include "gtm/elog.h"
#include "gtm/gtm_list.h"

extern char *GTMLogFile;

typedef enum GTM_ThreadStatus
{
	GTM_THREAD_STARTING,
	GTM_THREAD_RUNNING,
	GTM_THREAD_EXITING,
	GTM_THREAD_BACKUP, 		/* Backup to standby is in progress */
	/* Must be the last */
	GTM_THREAD_INVALID
} GTM_ThreadStatus;

struct GTM_ConnectionInfo;

#define ERRORDATA_STACK_SIZE  20

typedef struct GTM_ThreadInfo
{
	/*
	 * Initial few members get includes from gtm_common.h. This is to make sure
	 * that the GTMProxy_ThreadInfo and GTM_ThreadInfo structure can be
	 * typecasted to each other and these initial members can be safely
	 * accessed. If you need a member which should be common to both
	 * structures, consider adding them to GTM_COMMON_THREAD_INFO
	 */
	GTM_COMMON_THREAD_INFO

	GTM_ThreadStatus	thr_status;
	GTM_ConnectionInfo	*thr_conn;
	uint32				thr_client_id;		/* unique client identifier */

	GTM_RWLock			thr_lock;
	gtm_List				*thr_cached_txninfo;
} GTM_ThreadInfo;

typedef struct GTM_Threads
{
	uint32				gt_thread_count;
	uint32				gt_array_size;
	bool				gt_standby_ready;
	GTM_ThreadInfo		**gt_threads;
	uint32				gt_starting_client_id;
	uint32				gt_next_client_id;
	GTM_RWLock			gt_lock;
} GTM_Threads;

extern GTM_Threads *GTMThreads;

int GTM_ThreadAdd(GTM_ThreadInfo *thrinfo);
int GTM_ThreadRemove(GTM_ThreadInfo *thrinfo);
int GTM_ThreadJoin(GTM_ThreadInfo *thrinfo);
void GTM_ThreadExit(void);
void ConnFree(Port *port);
void GTM_LockAllOtherThreads(void);
void GTM_UnlockAllOtherThreads(void);
void GTM_DoForAllOtherThreads(void (* process_routine)(GTM_ThreadInfo *));
void GTM_SetInitialAndNextClientIdentifierAtPromote(void);

GTM_ThreadInfo *GTM_ThreadCreate(GTM_ConnectionInfo *conninfo,
				  void *(* startroutine)(void *));
GTM_ThreadInfo * GTM_GetThreadInfo(GTM_ThreadID thrid);
#ifdef XCP
extern void SaveControlInfo(void);
#define CONTROL_INTERVAL		1000
#endif

/*
 * pthread keys to get thread specific information
 */
extern pthread_key_t					threadinfo_key;
extern MemoryContext					TopMostMemoryContext;
extern GTM_ThreadID						TopMostThreadID;

#define SetMyThreadInfo(thrinfo)		pthread_setspecific(threadinfo_key, (thrinfo))
#define GetMyThreadInfo					((GTM_ThreadInfo *)pthread_getspecific(threadinfo_key))

#define TopMemoryContext		(GetMyThreadInfo->thr_thread_context)
#define ThreadTopContext		(GetMyThreadInfo->thr_thread_context)
#define MessageContext			(GetMyThreadInfo->thr_message_context)
#define CurrentMemoryContext	(GetMyThreadInfo->thr_current_context)
#define ErrorContext			(GetMyThreadInfo->thr_error_context)
#define errordata				(GetMyThreadInfo->thr_error_data)
#define recursion_depth			(GetMyThreadInfo->thr_error_recursion_depth)
#define errordata_stack_depth	(GetMyThreadInfo->thr_error_stack_depth)
#define CritSectionCount		(GetMyThreadInfo->thr_criticalsec_count)

#define PG_exception_stack		(GetMyThreadInfo->thr_sigjmp_buf)
#define MyConnection			(GetMyThreadInfo->thr_conn)
#define MyPort					((GetMyThreadInfo->thr_conn != NULL) ?	\
									GetMyThreadInfo->thr_conn->con_port :	\
									NULL)
#define MyThreadID				(GetMyThreadInfo->thr_id)
#define IsMainThread()			(GetMyThreadInfo->thr_id == TopMostThreadID)

#define GTM_CachedTransInfo				(GetMyThreadInfo->thr_cached_txninfo)
#define GTM_HaveFreeCachedTransInfo()	(gtm_list_length(GTM_CachedTransInfo))

#define GTM_MAX_CACHED_TRANSINFO		0
#define GTM_HaveEnoughCachedTransInfo()	(gtm_list_length(GTM_CachedTransInfo) >= GTM_MAX_CACHED_TRANSINFO)

#define START_CRIT_SECTION()  (CritSectionCount++)

#define END_CRIT_SECTION() \
	do { \
		    Assert(CritSectionCount > 0); \
		    CritSectionCount--; \
	} while(0)

#define GTM_CLIENT_ID_EQ(a, b)		\
	((a) == (b))
#define GTM_CLIENT_ID_LT(a, b)		\
	(((int32)((a) - (b)) < 0) ? true : false)
#define GTM_CLIENT_ID_GT(a, b)		\
	(!GTM_CLIENT_ID_LT(a, b) && !GTM_CLIENT_ID_EQ(a, b))
#define GTM_CLIENT_ID_NEXT(a)	\
	((((a) + 1) == UINT32_MAX) ? 1 : ((a) + 1))
#endif
