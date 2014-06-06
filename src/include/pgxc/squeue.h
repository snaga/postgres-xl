/*-------------------------------------------------------------------------
 *
 * barrier.h
 *
 *	  Definitions for the shared queue handling
 *
 *
 * Copyright (c) 2012-2014, TransLattice, Inc.
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#ifndef SQUEUE_H
#define SQUEUE_H

#include "postgres.h"
#include "executor/tuptable.h"
#include "nodes/pg_list.h"
#include "utils/tuplestore.h"

extern PGDLLIMPORT int NSQueues;
extern PGDLLIMPORT int SQueueSize;

/* Fixed size of shared queue, maybe need to be GUC configurable */
#define SQUEUE_SIZE ((long) SQueueSize * 1024L)
/* Number of shared queues, maybe need to be GUC configurable */
#define NUM_SQUEUES ((long) NSQueues)

#define SQUEUE_KEYSIZE (64)

#define SQ_CONS_SELF -1
#define SQ_CONS_NONE -2

typedef struct SQueueHeader *SharedQueue;

extern Size SharedQueueShmemSize(void);
extern void SharedQueuesInit(void);
extern void SharedQueueAcquire(const char *sqname, int ncons);
extern SharedQueue SharedQueueBind(const char *sqname, List *consNodes,
				List *distNodes, int *myindex, int *consMap);
extern void SharedQueueUnBind(SharedQueue squeue);
extern void SharedQueueRelease(const char *sqname);
extern void SharedQueuesCleanup(int code, Datum arg);

extern int	SharedQueueFinish(SharedQueue squeue, TupleDesc tupDesc,
				  Tuplestorestate **tuplestore);

extern void SharedQueueWrite(SharedQueue squeue, int consumerIdx,
				 TupleTableSlot *slot, Tuplestorestate **tuplestore,
				 MemoryContext tmpcxt);
extern bool SharedQueueRead(SharedQueue squeue, int consumerIdx,
				TupleTableSlot *slot, bool canwait);
extern void SharedQueueReset(SharedQueue squeue, int consumerIdx);
extern void SharedQueueResetNotConnected(SharedQueue squeue);
extern bool SharedQueueCanPause(SharedQueue squeue);

#endif
