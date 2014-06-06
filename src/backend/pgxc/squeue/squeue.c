/*-------------------------------------------------------------------------
 *
 * squeue.c
 *
 *	  Shared queue is for data exchange in shared memory between sessions,
 * one of which is a producer, providing data rows. Others are consumer agents -
 * sessions initiated from other datanodes, the main purpose of them is to read
 * rows from the shared queue and send then to the parent data node.
 *    The producer is usually a consumer at the same time, it sends back tuples
 * to the parent node without putting it to the queue.
 *
 * Copyright (c) 2012-2014, TransLattice, Inc.
 *
 * IDENTIFICATION
 *	  $$
 *
 *
 *-------------------------------------------------------------------------
 */

#include <sys/time.h>
#include "postgres.h"

#include "miscadmin.h"
#include "access/gtm.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/squeue.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/resowner.h"


int NSQueues = 64;
int SQueueSize = 64;

#define LONG_TUPLE -42

typedef struct ConsumerSync
{
	LWLockId	cs_lwlock; 		/* Synchronize access to the consumer queue */
	Latch 		cs_latch; 	/* The latch consumer is waiting on */
} ConsumerSync;


/*
 * Shared memory structure to store synchronization info to access shared queues
 */
typedef struct SQueueSync
{
	void 	   *queue; 			/* NULL if not assigned to any queue */
	Latch 		sqs_producer_latch; /* the latch producer is waiting on */
	ConsumerSync sqs_consumer_sync[0]; /* actual length is MaxDataNodes-1 is
										* not known on compile time */
} SQueueSync;

/* Both producer and consumer are working */
#define CONSUMER_ACTIVE 0
/* Producer have finished work successfully and waits for consumer */
#define CONSUMER_EOF 1
/* Producer encountered error and waits for consumer to disconnect */
#define CONSUMER_ERROR 2
/* Consumer is finished with the query, OK to unbind */
#define CONSUMER_DONE 3


/* State of a single consumer */
typedef struct
{
	int			cs_pid;			/* Process id of the consumer session */
	int			cs_node;		/* Node id of the consumer parent */
	/*
	 * Queue state. The queue is a cyclic queue where stored tuples in the
	 * DataRow format, first goes the lengths of the tuple in host format,
	 * because it never sent over network followed by tuple bytes.
	 */
	int			cs_ntuples; 	/* Number of tuples in the queue */
	int			cs_status;	 	/* See CONSUMER_* defines above */
	char	   *cs_qstart;		/* Where consumer queue begins */
	int			cs_qlength;		/* The size of the consumer queue */
	int			cs_qreadpos;	/* The read position in the consumer queue */
	int			cs_qwritepos;	/* The write position in the consumer queue */
#ifdef SQUEUE_STAT
	long 		stat_writes;
	long		stat_reads;
	long 		stat_buff_writes;
	long		stat_buff_reads;
	long		stat_buff_returns;
#endif
} ConsState;

/* Shared queue header */
typedef struct SQueueHeader
{
	char		sq_key[SQUEUE_KEYSIZE]; /* Hash entry key should be at the
								 * beginning of the hash entry */
	int			sq_pid; 		/* Process id of the producer session */
	int			sq_nodeid;		/* Node id of the producer parent */
	SQueueSync *sq_sync;        /* Associated sinchronization objects */
#ifdef SQUEUE_STAT
	bool		stat_finish;
	long		stat_paused;
#endif
	int			sq_nconsumers;	/* Number of consumers */
	ConsState 	sq_consumers[0];/* variable length array */
} SQueueHeader;


/*
 * Hash table where all shared queues are stored. Key is the queue name, value
 * is SharedQueue
 */
static HTAB *SharedQueues = NULL;


/*
 * Pool of synchronization items
 */
static void *SQueueSyncs;

#define SQUEUE_SYNC_SIZE \
	(sizeof(SQueueSync) + (MaxDataNodes-1) * sizeof(ConsumerSync))

#define GET_SQUEUE_SYNC(idx) \
	((SQueueSync *) (((char *) SQueueSyncs) + (idx) * SQUEUE_SYNC_SIZE))

#define SQUEUE_HDR_SIZE(nconsumers) \
	(sizeof(SQueueHeader) + (nconsumers) * sizeof(ConsState))

#define QUEUE_FREE_SPACE(cstate) \
	((cstate)->cs_ntuples > 0 ? \
		((cstate)->cs_qreadpos >= (cstate)->cs_qwritepos ? \
			(cstate)->cs_qreadpos - (cstate)->cs_qwritepos : \
			(cstate)->cs_qlength + (cstate)->cs_qreadpos \
								 - (cstate)->cs_qwritepos) \
		: (cstate)->cs_qlength)

#define QUEUE_WRITE(cstate, len, buf) \
	do \
	{ \
		if ((cstate)->cs_qwritepos + (len) <= (cstate)->cs_qlength) \
		{ \
			memcpy((cstate)->cs_qstart + (cstate)->cs_qwritepos, buf, len); \
			(cstate)->cs_qwritepos += (len); \
			if ((cstate)->cs_qwritepos == (cstate)->cs_qlength) \
				(cstate)->cs_qwritepos = 0; \
		} \
		else \
		{ \
			int part = (cstate)->cs_qlength - (cstate)->cs_qwritepos; \
			memcpy((cstate)->cs_qstart + (cstate)->cs_qwritepos, buf, part); \
			(cstate)->cs_qwritepos = (len) - part; \
			memcpy((cstate)->cs_qstart, (buf) + part, (cstate)->cs_qwritepos); \
		} \
	} while(0)


#define QUEUE_READ(cstate, len, buf) \
	do \
	{ \
		if ((cstate)->cs_qreadpos + (len) <= (cstate)->cs_qlength) \
		{ \
			memcpy(buf, (cstate)->cs_qstart + (cstate)->cs_qreadpos, len); \
			(cstate)->cs_qreadpos += (len); \
			if ((cstate)->cs_qreadpos == (cstate)->cs_qlength) \
				(cstate)->cs_qreadpos = 0; \
		} \
		else \
		{ \
			int part = (cstate)->cs_qlength - (cstate)->cs_qreadpos; \
			memcpy(buf, (cstate)->cs_qstart + (cstate)->cs_qreadpos, part); \
			(cstate)->cs_qreadpos = (len) - part; \
			memcpy((buf) + part, (cstate)->cs_qstart, (cstate)->cs_qreadpos); \
		} \
	} while(0)


static bool sq_push_long_tuple(ConsState *cstate, RemoteDataRow datarow);
static void sq_pull_long_tuple(ConsState *cstate, RemoteDataRow datarow,
							   ConsumerSync *sync);

/*
 * SharedQueuesInit
 *    Initialize the reference on the shared memory hash table where all shared
 * queues are stored. Invoked during postmaster initialization.
 */
void
SharedQueuesInit(void)
{
	HASHCTL info;
	int		hash_flags;
	bool 	found;

	info.keysize = SQUEUE_KEYSIZE;
	info.entrysize = SQUEUE_SIZE;
	hash_flags = HASH_ELEM;

	SharedQueues = ShmemInitHash("Shared Queues", NUM_SQUEUES,
								 NUM_SQUEUES, &info, hash_flags);

	/*
	 * Synchronization stuff is in separate structure because we need to
	 * initialize all items now while in the postmaster.
	 * The structure is actually an array, each array entry is assigned to
	 * each instance of SharedQueue in use.
	 */
	SQueueSyncs = ShmemInitStruct("Shared Queues Sync",
								  SQUEUE_SYNC_SIZE * NUM_SQUEUES,
								  &found);
	if (!found)
	{
		int 	i;

		for (i = 0; i < NUM_SQUEUES; i++)
		{
			SQueueSync *sqs = GET_SQUEUE_SYNC(i);
			int			j;

			sqs->queue = NULL;
			InitSharedLatch(&sqs->sqs_producer_latch);
			for (j = 0; j < MaxDataNodes-1; j++)
			{
				InitSharedLatch(&sqs->sqs_consumer_sync[j].cs_latch);
				sqs->sqs_consumer_sync[j].cs_lwlock = LWLockAssign();
			}
		}
	}
}


Size
SharedQueueShmemSize(void)
{
	Size sq_size;
	Size sqs_size;

	sq_size = mul_size(NUM_SQUEUES, SQUEUE_SIZE);
	sqs_size = mul_size(NUM_SQUEUES, SQUEUE_SYNC_SIZE);

	return add_size(sq_size, sqs_size);
}

/*
 * SharedQueueAcquire
 *     Reserve a named shared queue for future data exchange between processes
 * supplying tuples to remote Datanodes. Invoked when a remote query plan is
 * registered on the Datanode. The number of consumers is known at this point,
 * so shared queue may be formatted during reservation. The first process that
 * is acquiring the shared queue on the Datanode does the formatting.
 */
void
SharedQueueAcquire(const char *sqname, int ncons)
{
	bool		found;
	SharedQueue sq;

	Assert(IsConnFromDatanode());
	Assert(ncons > 0);

tryagain:
	LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);

	sq = (SharedQueue) hash_search(SharedQueues, sqname, HASH_ENTER, &found);
	/* First process acquiring queue should format it */
	if (!found)
	{
		int		qsize;   /* Size of one queue */
		int		i;
		char   *heapPtr;

		elog(LOG, "Format squeue %s for %d consumers", sqname, ncons);

		/* Initialize the shared queue */
		sq->sq_pid = 0;
		sq->sq_nodeid = -1;
#ifdef SQUEUE_STAT
		sq->stat_finish = false;
		sq->stat_paused = 0;
#endif
		/*
		 * Assign sync object (latches to wait on)
		 * XXX We may want to optimize this and do smart search instead of
		 * iterating the array.
		 */
		for (i = 0; i < NUM_SQUEUES; i++)
		{
			SQueueSync *sqs = GET_SQUEUE_SYNC(i);
			if (sqs->queue == NULL)
			{
				sqs->queue = (void *) sq;
				sq->sq_sync = sqs;
				break;
			}
		}

		sq->sq_nconsumers = ncons;
		/* Determine queue size for a single consumer */
		qsize = (SQUEUE_SIZE - SQUEUE_HDR_SIZE(sq->sq_nconsumers)) / sq->sq_nconsumers;

		heapPtr = (char *) sq;
		/* Skip header */
		heapPtr += SQUEUE_HDR_SIZE(sq->sq_nconsumers);
		/* Set up consumer queues */
		for (i = 0; i < ncons; i++)
		{
			ConsState *cstate = &(sq->sq_consumers[i]);

			cstate->cs_pid = 0;
			cstate->cs_node = -1;
			cstate->cs_ntuples = 0;
			cstate->cs_status = CONSUMER_ACTIVE;
			cstate->cs_qstart = heapPtr;
			cstate->cs_qlength = qsize;
			cstate->cs_qreadpos = 0;
			cstate->cs_qwritepos = 0;
			heapPtr += qsize;
		}
		Assert(heapPtr <= ((char *) sq) + SQUEUE_SIZE);
	}
	else
	{
		/*
		 * A race condition is possible here. The previous operation might  use
		 * the same Shared Queue name if that was different execution of the
		 * same Portal. So here we should try to determine if that Shared Queue
		 * belongs to this execution or that is not-yet-released Shared Queue
		 * of previous operation.
		 * Though at the moment I am not sure, but I believe the BIND stage is
		 * only happening after completion of ACQUIRE stage, so it is enough
		 * to verify the producer (the very first node that binds) is not bound
		 * yet. If it is bound, sleep for a moment and try again. No reason to
		 * sleep longer, the producer needs just a quantum of CPU time to UNBIND
		 * itself.
		 */
		if (sq->sq_pid != 0)
		{
			int 		selfid;  /* Node Id of the parent data node */
			int			i;
			char		ntype = PGXC_NODE_DATANODE;
			bool		old_squeue = true;

			selfid = PGXCNodeGetNodeIdFromName(PGXC_PARENT_NODE, &ntype);
			for (i = 0; i < sq->sq_nconsumers; i++)
			{
				ConsState *cstate = &(sq->sq_consumers[i]);
				if (cstate->cs_node == selfid)
				{
					SQueueSync *sqsync = sq->sq_sync;

					LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock,
								  LW_EXCLUSIVE);
					/* verify status */
					if (cstate->cs_status != CONSUMER_DONE)
						old_squeue = false;

					LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
					break;
				}
			}
			if (old_squeue)
			{
				LWLockRelease(SQueuesLock);
				pg_usleep(1L);
				goto tryagain;
			}

		}
	}
	LWLockRelease(SQueuesLock);
}


/*
 * SharedQueueBind
 *    Bind to the shared queue specified by sqname either as a consumer or as a
 * producer. The first process that binds to the shared queue becomes a producer
 * and receives the consumer map, others become consumers and receive queue
 * indexes to read tuples from.
 * The consNodes int list identifies the nodes involved in the current step.
 * The distNodes int list describes result distribution of the current step.
 * The consNodes should be a subset of distNodes.
 * The myindex and consMap parameters are binding results. If caller process
 * is bound to the query as a producer myindex is set to -1 and index of the
 * each consumer (order number in the consNodes) is stored to the consMap array
 * at the position of the node in the distNodes. For the producer node
 * SQ_CONS_SELF is stored, nodes from distNodes list which are not members of
 * consNodes or if it was reported they won't read results, they are represented
 * as SQ_CONS_NONE.
 */
SharedQueue
SharedQueueBind(const char *sqname, List *consNodes,
								   List *distNodes, int *myindex, int *consMap)
{
	bool		found;
	SharedQueue sq;
	int 		selfid;  /* Node Id of the parent data node */
	char		ntype = PGXC_NODE_DATANODE;

	LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);

	selfid = PGXCNodeGetNodeIdFromName(PGXC_PARENT_NODE, &ntype);

	sq = (SharedQueue) hash_search(SharedQueues, sqname, HASH_FIND, &found);
	if (!found)
		elog(PANIC, "Shared queue %s not found", sqname);
	if (sq->sq_pid == 0)
	{
		/* Producer */
		int		i;
		ListCell *lc;

		Assert(consMap);

		elog(LOG, "Bind node %s to squeue of step %s as a producer",
			 PGXC_PARENT_NODE, sqname);

		/* Initialize the shared queue */
		sq->sq_pid = MyProcPid;
		sq->sq_nodeid = selfid;
		OwnLatch(&sq->sq_sync->sqs_producer_latch);

		i = 0;
		foreach(lc, distNodes)
		{
			int			nodeid = lfirst_int(lc);

			/*
			 * Producer won't go to shared queue to hand off tuple to itself,
			 * so we do not need to create queue for that entry.
			 */
			if (nodeid == selfid)
			{
				/* Producer must be in the consNodes list */
				Assert(list_member_int(consNodes, nodeid));
				consMap[i++] = SQ_CONS_SELF;
			}
			/*
			 * This node may connect as a consumer, store consumer id to the map
			 * and initialize consumer queue
			 */
			else if (list_member_int(consNodes, nodeid))
			{
				ConsState  *cstate;
				int 		j;

				for (j = 0; j < sq->sq_nconsumers; j++)
				{
					cstate = &(sq->sq_consumers[j]);
					if (cstate->cs_node == nodeid)
					{
						/* The process already reported that queue won't read */
						elog(LOG, "Node %d of step %s is released already",
							 nodeid, sqname);
						consMap[i++] = SQ_CONS_NONE;
						break;
					}
					else if (cstate->cs_node == -1)
					{
						/* found unused slot, assign the consumer to it */
						consMap[i++] = j;
						cstate->cs_node = nodeid;
						break;
					}
				}
 			}
			/*
			 * Consumer from this node won't ever connect as upper level step
			 * is not executed on the node. Discard resuls that may go to that
			 * node, if any.
			 */
			else
			{
				consMap[i++] = SQ_CONS_NONE;
			}
		}

		if (myindex)
			*myindex = -1;
	}
	else
	{
		int 	nconsumers;
		ListCell *lc;

		/* Producer should be different process */
		Assert(sq->sq_pid != MyProcPid);

		elog(LOG, "Bind node %s to squeue of step %s as a consumer of process %d", PGXC_PARENT_NODE, sqname, sq->sq_pid);

		/* Sanity checks */
		Assert(myindex);
		*myindex = -1;
		/* Ensure the passed in consumer list matches the queue */
		nconsumers = 0;
		foreach (lc, consNodes)
		{
			int 		nodeid = lfirst_int(lc);
			int			i;

			if (nodeid == sq->sq_nodeid)
			{
				/*
				 * This node is a producer it should be in the consumer list,
				 * but no consumer queue for it
				 */
				continue;
			}

			/* find consumer queue for the node */
			for (i = 0; i < sq->sq_nconsumers; i++)
			{
				ConsState *cstate = &(sq->sq_consumers[i]);
				if (cstate->cs_node == nodeid)
				{
					nconsumers++;
					if (nodeid == selfid)
					{
						/*
						 * Current consumer queue is that from which current
						 * session will be sending out data rows.
						 * Initialize the queue to let producer know we are
						 * here and runnng.
						 */
						SQueueSync *sqsync = sq->sq_sync;

						LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock,
									  LW_EXCLUSIVE);
						/* Make sure no consumer bound to the queue already */
						Assert(cstate->cs_pid == 0);
						/* make sure the queue is ready to read */
						Assert(cstate->cs_qlength > 0);
						/* verify status */
						if (cstate->cs_status == CONSUMER_ERROR ||
								cstate->cs_status == CONSUMER_DONE)
						{
							/*
							 * Producer failed by the time the consumer connect.
							 * Change status to "Done" to allow producer unbind
							 * and report problem to the parent.
							 */
							cstate->cs_status = CONSUMER_DONE;
							/* Producer may be waiting for status change */
							SetLatch(&sqsync->sqs_producer_latch);
							LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
							LWLockRelease(SQueuesLock);
							ereport(ERROR,
									(errcode(ERRCODE_PRODUCER_ERROR),
									 errmsg("producer error")));
						}
						/*
						 * Any other status is acceptable. Normally it would be
						 * ACTIVE. If producer have had only few rows to emit
						 * and it is already done the status would be EOF.
						 */
						/* Set up the consumer */
						cstate->cs_pid = MyProcPid;
						/* return found index */
						*myindex = i;
						OwnLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
						LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
					}
					break;
				}
			}
			/* Check if entry was found and therefore loop was broken */
			Assert(i < sq->sq_nconsumers);
		}
		/* Check the consumer is found */
		Assert(*myindex != -1);
		Assert(sq->sq_nconsumers == nconsumers);
	}
	LWLockRelease(SQueuesLock);
	return sq;
}


/*
 * Push data from the local tuplestore to the queue for specified consumer.
 * Return true if succeeded and the tuplestore is now empty. Return false
 * if specified queue has not enough room for the next tuple.
 */
static bool
SharedQueueDump(SharedQueue squeue, int consumerIdx,
						   TupleTableSlot *tmpslot, Tuplestorestate *tuplestore)
{
	ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);

	/* discard stored data if consumer is not active */
	if (cstate->cs_status != CONSUMER_ACTIVE)
	{
		tuplestore_clear(tuplestore);
		return true;
	}

	/*
	 * Tuplestore does not clear eof flag on the active read pointer, causing
	 * the store is always in EOF state once reached when there is a single
	 * read pointer. We do not want behavior like this and workaround by using
	 * secondary read pointer. Primary read pointer (0) is active when we are
	 * writing to the tuple store, also it is used to bookmark current position
	 * when reading to be able to roll back and return just read tuple back to
	 * the store if we failed to write it out to the queue.
	 * Secondary read pointer is for reading, and its eof flag is cleared if a
	 * tuple is written to the store.
	 */
	tuplestore_select_read_pointer(tuplestore, 1);

	/* If we have something in the tuplestore try to push this to the queue */
	while (!tuplestore_ateof(tuplestore))
	{
		/* save position */
		tuplestore_copy_read_pointer(tuplestore, 1, 0);

		/* Try to get next tuple to the temporary slot */
		if (!tuplestore_gettupleslot(tuplestore, true, false, tmpslot))
		{
			/* false means the tuplestore in EOF state */
			break;
		}
#ifdef SQUEUE_STAT
		cstate->stat_buff_reads++;
#endif

		/* The slot should contain a data row */
		Assert(tmpslot->tts_datarow);

		/* check if queue has enough room for the data */
		if (QUEUE_FREE_SPACE(cstate) < sizeof(int) + tmpslot->tts_datarow->msglen)
		{
			/*
			 * If stored tuple does not fit empty queue we are entering special
			 * procedure of pushing it through.
			 */
			if (cstate->cs_ntuples <= 0)
			{
				/*
				 * If pushing throw is completed wake up and proceed to next
				 * tuple, there could be enough space in the consumer queue to
				 * fit more.
				 */
				bool done = sq_push_long_tuple(cstate, tmpslot->tts_datarow);

				/*
				 * sq_push_long_tuple writes some data anyway, so wake up
				 * the consumer.
				 */
				SetLatch(&squeue->sq_sync->sqs_consumer_sync[consumerIdx].cs_latch);

				if (done)
					continue;
			}

			/* Restore read position to get same tuple next time */
			tuplestore_copy_read_pointer(tuplestore, 0, 1);
#ifdef SQUEUE_STAT
			cstate->stat_buff_returns++;
#endif

			/* We might advance the mark, try to truncate */
			tuplestore_trim(tuplestore);

			/* Prepare for writing, set proper read pointer */
			tuplestore_select_read_pointer(tuplestore, 0);

			/* ... and exit */
			return false;
		}
		else
		{
			/* Enqueue data */
			QUEUE_WRITE(cstate, sizeof(int), (char *) &tmpslot->tts_datarow->msglen);
			QUEUE_WRITE(cstate, tmpslot->tts_datarow->msglen, tmpslot->tts_datarow->msg);

			/* Increment tuple counter. If it was 0 consumer may be waiting for
			 * data so try to wake it up */
			if ((cstate->cs_ntuples)++ == 0)
				SetLatch(&squeue->sq_sync->sqs_consumer_sync[consumerIdx].cs_latch);
		}
	}

	/* Remove rows we have just read */
	tuplestore_trim(tuplestore);

	/* prepare for writes, set read pointer 0 as active */
	tuplestore_select_read_pointer(tuplestore, 0);

	return true;
}


/*
 * SharedQueueWrite
 *    Write data from the specified slot to the specified queue. If the
 * tuplestore passed in has tuples try and write them first.
 * If specified queue is full the tuple is put into the tuplestore which is
 * created if necessary
 */
void
SharedQueueWrite(SharedQueue squeue, int consumerIdx,
							TupleTableSlot *slot, Tuplestorestate **tuplestore,
							MemoryContext tmpcxt)
{
	ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);
	SQueueSync *sqsync = squeue->sq_sync;
	LWLockId    clwlock = sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock;
	RemoteDataRow datarow;
	bool		free_datarow;

	Assert(cstate->cs_qlength > 0);

	LWLockAcquire(clwlock, LW_EXCLUSIVE);

#ifdef SQUEUE_STAT
	cstate->stat_writes++;
#endif

	/*
	 * If we have anything in the local storage try to dump this first,
	 * but do not try to dump often to avoid overhead of creating temporary
	 * tuple slot. It should be OK to dump if queue is half empty.
	 */
	if (*tuplestore)
	{
		bool dumped = false;

		if (QUEUE_FREE_SPACE(cstate) > cstate->cs_qlength / 2)
		{
			TupleTableSlot *tmpslot;

			tmpslot = MakeSingleTupleTableSlot(slot->tts_tupleDescriptor);
			dumped = SharedQueueDump(squeue, consumerIdx, tmpslot, *tuplestore);
			ExecDropSingleTupleTableSlot(tmpslot);
		}
		if (!dumped)
		{
			/* No room to even dump local store, append the tuple to the store
			 * and exit */
#ifdef SQUEUE_STAT
			cstate->stat_buff_writes++;
#endif
			LWLockRelease(clwlock);
			tuplestore_puttupleslot(*tuplestore, slot);
			return;
		}
	}

	/* Get datarow from the tuple slot */
	if (slot->tts_datarow)
	{
		/*
		 * The function ExecCopySlotDatarow always make a copy, but here we
		 * can optimize and avoid copying the data, so we just get the reference
		 */
		datarow = slot->tts_datarow;
		free_datarow = false;
	}
	else
	{
		datarow = ExecCopySlotDatarow(slot, tmpcxt);
		free_datarow = true;
	}
	if (QUEUE_FREE_SPACE(cstate) < sizeof(int) + datarow->msglen)
	{
		/* Not enough room, store tuple locally */
		LWLockRelease(clwlock);

		/* clean up */
		if (free_datarow)
			pfree(datarow);

		/* Create tuplestore if does not exist */
		if (*tuplestore == NULL)
		{
			int			ptrno;
			char 		storename[64];

#ifdef SQUEUE_STAT
			elog(LOG, "Start buffering %s node %d, %d tuples in queue, %ld writes and %ld reads so far",
				 squeue->sq_key, cstate->cs_node, cstate->cs_ntuples, cstate->stat_writes, cstate->stat_reads);
#endif
			*tuplestore = tuplestore_begin_datarow(false, work_mem, tmpcxt);
			/* We need is to be able to remember/restore the read position */
			snprintf(storename, 64, "%s node %d", squeue->sq_key, cstate->cs_node);
			tuplestore_collect_stat(*tuplestore, storename);
			/*
			 * Allocate a second read pointer to read from the store. We know
			 * it must have index 1, so needn't store that.
			 */
			ptrno = tuplestore_alloc_read_pointer(*tuplestore, 0);
			Assert(ptrno == 1);
		}

#ifdef SQUEUE_STAT
		cstate->stat_buff_writes++;
#endif
		/* Append the slot to the store... */
		tuplestore_puttupleslot(*tuplestore, slot);

		/* ... and exit */
		return;
	}
	else
	{
		/* do not supply data to closed consumer */
		if (cstate->cs_status == CONSUMER_ACTIVE)
		{
			/* write out the data */
			QUEUE_WRITE(cstate, sizeof(int), (char *) &datarow->msglen);
			QUEUE_WRITE(cstate, datarow->msglen, datarow->msg);
			/* Increment tuple counter. If it was 0 consumer may be waiting for
			 * data so try to wake it up */
			if ((cstate->cs_ntuples)++ == 0)
				SetLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch);
		}

		/* clean up */
		if (free_datarow)
			pfree(datarow);
	}
	LWLockRelease(clwlock);
}


/*
 * SharedQueueRead
 *    Read one data row from the specified queue into the provided tupleslot.
 * Returns true if EOF is reached on the specified consumer queue.
 * If the queue is empty, behavior is controlled by the canwait parameter.
 * If canwait is true it is waiting while row is available or EOF or error is
 * reported, if it is false, the slot is emptied and false is returned.
 */
bool
SharedQueueRead(SharedQueue squeue, int consumerIdx,
							TupleTableSlot *slot, bool canwait)
{
	ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);
	SQueueSync *sqsync = squeue->sq_sync;
	RemoteDataRow datarow;
	int 		datalen;

	Assert(cstate->cs_qlength > 0);

	LWLockAcquire(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock, LW_EXCLUSIVE);

	Assert(cstate->cs_status != CONSUMER_DONE);
	while (cstate->cs_ntuples <= 0)
	{
		if (cstate->cs_status == CONSUMER_EOF)
		{
			/* Inform producer the consumer have done the job */
			cstate->cs_status = CONSUMER_DONE;
			/* no need to receive notifications */
			DisownLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch);
			/* producer done the job and no more rows expected, clean up */
			LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
			ExecClearTuple(slot);
			/*
			 * notify the producer, it may be waiting while consumers
			 * are finishing
			 */
			SetLatch(&sqsync->sqs_producer_latch);
			elog(LOG, "EOF reached while reading from squeue, exiting");
			return true;
		}
		else if (cstate->cs_status == CONSUMER_ERROR)
		{
			/*
			 * There was a producer error while waiting.
			 * Release all the locks and report problem to the caller.
			 */
			LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
			/*
			 * Reporting error will cause transaction rollback and clean up of
			 * all portals. We can not mark the portal so it does not access
			 * the queue so we should hold it for now. We should prevent queue
			 * unbound in between.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_PRODUCER_ERROR),
					 errmsg("producer error")));
		}
		if (canwait)
		{
			/* Prepare waiting on empty buffer */
			ResetLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch);
			LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
			/* Wait for notification about available info */
			WaitLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1);
			/* got the notification, restore lock and try again */
			LWLockAcquire(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock, LW_EXCLUSIVE);
		}
		else
		{
			LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
			ExecClearTuple(slot);
			return false;
		}
	}
	/* have at least one row, read it in and store to slot */
	QUEUE_READ(cstate, sizeof(int), (char *) (&datalen));
	datarow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + datalen);
	datarow->msgnode = InvalidOid;
	datarow->msglen = datalen;
	if (datalen > cstate->cs_qlength - sizeof(int))
		sq_pull_long_tuple(cstate, datarow,
						   &sqsync->sqs_consumer_sync[consumerIdx]);
	else
		QUEUE_READ(cstate, datalen, datarow->msg);
	ExecStoreDataRowTuple(datarow, slot, true);
	(cstate->cs_ntuples)--;
#ifdef SQUEUE_STAT
	cstate->stat_reads++;
#endif
	/* sanity check */
	Assert((cstate->cs_ntuples == 0) == (cstate->cs_qreadpos == cstate->cs_qwritepos));
	LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
	return false;
}


/*
 * Mark specified consumer as closed discarding all input which may already be
 * in the queue.
 * If consumerIdx is -1 the producer is cleaned up. Producer need to wait for
 * consumers before releasing the queue, so if there are yet active consumers,
 * they are notified about the problem and they should disconnect from the
 * queue as soon as possible.
 */
void
SharedQueueReset(SharedQueue squeue, int consumerIdx)
{
	SQueueSync *sqsync = squeue->sq_sync;

	if (consumerIdx == -1)
	{
		int i;

		/* check queue states */
		for (i = 0; i < squeue->sq_nconsumers; i++)
		{
			ConsState *cstate = &squeue->sq_consumers[i];
			LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);

			/*
			 * If producer being reset before it is reached the end of the
			 * result set, that means consumer probably would not get all
			 * the rows and it should report error if the consumer's parent ever
			 * try to read. No need to raise error if consumer is just closed.
			 * If consumer is done already we do not need to change the status.
			 */
			if (cstate->cs_status != CONSUMER_EOF &&
					cstate->cs_status != CONSUMER_DONE)
			{
				elog(LOG, "Consumer %d of producer %s is cancelled", i, squeue->sq_key);
				cstate->cs_status = CONSUMER_ERROR;
				/* discard tuples which may already be in the queue */
				cstate->cs_ntuples = 0;
				/* keep consistent with cs_ntuples*/
				cstate->cs_qreadpos = cstate->cs_qwritepos = 0;

				/* wake up consumer if it is sleeping */
				SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
			}
			LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
		}
		elog(LOG, "Reset producer %s", squeue->sq_key);
	}
	else
	{
		ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);
		LWLockAcquire(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock,
					  LW_EXCLUSIVE);

		if (cstate->cs_status != CONSUMER_DONE)
		{
			/* Inform producer the consumer have done the job */
			cstate->cs_status = CONSUMER_DONE;
			/*
			 * No longer need to receive notifications. If consumer has not
			 * connected the latch is not owned
			 */
			if (cstate->cs_pid > 0)
				DisownLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch);
			/*
			 * notify the producer, it may be waiting while consumers
			 * are finishing
			 */
			SetLatch(&sqsync->sqs_producer_latch);
			elog(LOG, "Reset consumer %d of %s", consumerIdx, squeue->sq_key);
		}

		LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
	}
}


/*
 * Assume that not yet connected consumers won't connect and reset them.
 * That should allow to Finish/UnBind the queue gracefully and prevent
 * producer hanging.
 */
void
SharedQueueResetNotConnected(SharedQueue squeue)
{
	SQueueSync *sqsync = squeue->sq_sync;
	int result = 0;
	int i;

	/* check queue states */
	for (i = 0; i < squeue->sq_nconsumers; i++)
	{
		ConsState *cstate = &squeue->sq_consumers[i];
		LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);

		if (cstate->cs_pid == 0 &&
				cstate->cs_status != CONSUMER_EOF &&
				cstate->cs_status != CONSUMER_DONE)
		{
			result++;
			elog(LOG, "Consumer %d of producer %s is cancelled", i, squeue->sq_key);
			cstate->cs_status = CONSUMER_ERROR;
			/* discard tuples which may already be in the queue */
			cstate->cs_ntuples = 0;
			/* keep consistent with cs_ntuples*/
			cstate->cs_qreadpos = cstate->cs_qwritepos = 0;

			/* wake up consumer if it is sleeping */
			SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
		}
		LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
	}
	elog(LOG, "Reset producer %s", squeue->sq_key);
}


/*
 * Determine if producer can safely pause work.
 * The producer can pause if all consumers have enough data to read while
 * producer is sleeping.
 * Obvoius case when the producer can not pause if at least one queue is empty.
 */
bool
SharedQueueCanPause(SharedQueue squeue)
{
	SQueueSync *sqsync = squeue->sq_sync;
	bool 		result = true;
	int 		usedspace;
	int			ncons;
	int 		i;

	usedspace = 0;
	ncons = 0;
	for (i = 0; result && (i < squeue->sq_nconsumers); i++)
	{
		ConsState *cstate = &(squeue->sq_consumers[i]);
		LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_SHARED);
		/*
		 * Count only consumers that may be blocked.
		 * If producer has finished scanning and pushing local buffers some
		 * consumers may be finished already.
		 */
		if (cstate->cs_status == CONSUMER_ACTIVE)
		{
			/* can not pause if some queue is empty */
			result = (cstate->cs_ntuples > 0);
			usedspace += (cstate->cs_qwritepos > cstate->cs_qreadpos ?
							  cstate->cs_qwritepos - cstate->cs_qreadpos :
							  cstate->cs_qlength + cstate->cs_qwritepos
												 - cstate->cs_qreadpos);
			ncons++;
		}
		LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
	}
	/*
	 * Pause only if average consumer queue is full more then on half.
	 */
	if (result)
		result = (usedspace / ncons > squeue->sq_consumers[0].cs_qlength / 2);
#ifdef SQUEUE_STAT
	if (result)
		squeue->stat_paused++;
#endif
	return result;
}


int
SharedQueueFinish(SharedQueue squeue, TupleDesc tupDesc,
							  Tuplestorestate **tuplestore)
{
	SQueueSync *sqsync = squeue->sq_sync;
	TupleTableSlot *tmpslot = NULL;
	int 			i;
	int 			nstores = 0;

	for (i = 0; i < squeue->sq_nconsumers; i++)
	{
		ConsState *cstate = &squeue->sq_consumers[i];
		LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);
#ifdef SQUEUE_STAT
		if (!squeue->stat_finish)
			elog(LOG, "Finishing %s node %d, %ld writes and %ld reads so far, %ld buffer writes, %ld buffer reads, %ld tuples returned to buffer",
				 squeue->sq_key, cstate->cs_node, cstate->stat_writes, cstate->stat_reads, cstate->stat_buff_writes, cstate->stat_buff_reads, cstate->stat_buff_returns);
#endif
		/*
		 * if the tuplestore has data and consumer queue has space for some
		 * try to push rows to the queue. We do not want to do that often
		 * to avoid overhead of temp tuple slot allocation.
		 */
		if (tuplestore[i])
		{
			/* If the consumer is not reading just destroy the tuplestore */
			if (cstate->cs_status != CONSUMER_ACTIVE)
			{
				tuplestore_end(tuplestore[i]);
				tuplestore[i] = NULL;
			}
			else
			{
				nstores++;
				/*
				 * Attempt to dump tuples from the store require tuple slot
				 * allocation, that is not a cheap operation, so proceed if
				 * target queue has enough space.
				 */
				if (QUEUE_FREE_SPACE(cstate) > cstate->cs_qlength / 2)
				{
					if (tmpslot == NULL)
						tmpslot = MakeSingleTupleTableSlot(tupDesc);
					if (SharedQueueDump(squeue, i, tmpslot, tuplestore[i]))
					{
						tuplestore_end(tuplestore[i]);
						tuplestore[i] = NULL;
						cstate->cs_status = CONSUMER_EOF;
						nstores--;
					}
					/* Consumer may be sleeping, wake it up */
					SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
				}
			}
		}
		else
		{
			/* it set eof if not yet set */
			if (cstate->cs_status == CONSUMER_ACTIVE)
			{
				cstate->cs_status = CONSUMER_EOF;
				SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
			}
		}
		LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
	}
	if (tmpslot)
		ExecDropSingleTupleTableSlot(tmpslot);

#ifdef SQUEUE_STAT
	squeue->stat_finish = true;
#endif

	return nstores;
}


/*
 * SharedQueueUnBind
 *    Cancel binding of current process to the shared queue. If the process
 * was a producer it should pass in the array of tuplestores where tuples were
 * queueed when it was unsafe to block. If any of the tuplestores holds data
 * rows they are written to the queue. The length of the array of the
 * tuplestores should be the same as the count of consumers. It is OK if some
 * entries are NULL. When a consumer unbinds from the shared queue it should
 * set the tuplestore parameter to NULL.
 */
void
SharedQueueUnBind(SharedQueue squeue)
{
	SQueueSync *sqsync = squeue->sq_sync;
	int			wait_result = 0;

	/* loop while there are active consumers */
	for (;;)
	{
		int i;
		int c_count = 0;

		/* check queue states */
		for (i = 0; i < squeue->sq_nconsumers; i++)
		{
			ConsState *cstate = &squeue->sq_consumers[i];
			LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);
			/* is consumer working yet ? */
			if (cstate->cs_status == CONSUMER_ACTIVE)
				cstate->cs_status = CONSUMER_ERROR;
			if (cstate->cs_status != CONSUMER_DONE)
			{
				c_count++;
				/* Wake up consumer if it is sleeping */
				SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
				/* producer will continue waiting */
				ResetLatch(&sqsync->sqs_producer_latch);
			}
#ifdef SQUEUE_STAT
			else
				elog(LOG, "Done %s node %d, %ld writes and %ld reads, %ld buffer writes, %ld buffer reads, %ld tuples returned to buffer",
					 squeue->sq_key, cstate->cs_node, cstate->stat_writes, cstate->stat_reads, cstate->stat_buff_writes, cstate->stat_buff_reads, cstate->stat_buff_returns);
#endif

			LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
		}
		if (c_count == 0)
			break;
		elog(LOG, "Wait while %d squeue readers finishing", c_count);
		/* wait for a notification */
		wait_result = WaitLatch(&sqsync->sqs_producer_latch,
								WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
								10000L);
		if (wait_result & WL_TIMEOUT)
			break;
		/* got notification, continue loop */
	}
#ifdef SQUEUE_STAT
	elog(LOG, "Producer %s is done, there were %ld pauses", squeue->sq_key, squeue->stat_paused);
#endif

	LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);
	/* All is done, clean up */
	DisownLatch(&sqsync->sqs_producer_latch);

	/* Now it is OK to remove hash table entry */
	squeue->sq_sync = NULL;
	sqsync->queue = NULL;
	if (hash_search(SharedQueues, squeue->sq_key, HASH_REMOVE, NULL) != squeue)
		elog(PANIC, "Shared queue data corruption");

	LWLockRelease(SQueuesLock);
	elog(LOG, "Finalized squeue");
	if (wait_result & WL_TIMEOUT)
		elog(FATAL, "Timeout while waiting for Consumers finishing");
}


/*
 * If queue with specified name still exists set mark respective consumer as
 * "Done". Due to executor optimization consumer may never connect the queue,
 * and should allow producer to finish it up if it is known the consumer will
 * never connect.
 */
void
SharedQueueRelease(const char *sqname)
{
	bool					found;
	volatile SharedQueue 	sq;

	elog(LOG, "Shared Queue release: %s", sqname);

	LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);

	sq = (SharedQueue) hash_search(SharedQueues, sqname, HASH_FIND, &found);
	if (found)
	{
		volatile SQueueSync    *sqsync = sq->sq_sync;
		int 					myid;  /* Node Id of the parent data node */
		int						i;
		char					ntype = PGXC_NODE_DATANODE;

		Assert(sqsync && sqsync->queue == sq);

		/*
		 * Case if the shared queue was never bound.
		 * Just remove it from the hash table.
		 */
		if (sq->sq_nodeid == -1)
		{
			sq->sq_sync = NULL;
			sqsync->queue = NULL;
			if (hash_search(SharedQueues, sqname, HASH_REMOVE, NULL) != sq)
				elog(PANIC, "Shared queue data corruption");
			elog(LOG, "Finalized squeue %s", sqname);
			LWLockRelease(SQueuesLock);
			return;
		}

		myid = PGXCNodeGetNodeIdFromName(PGXC_PARENT_NODE, &ntype);
		/*
		 * Do not bother releasing producer, all necessary work will be
		 * done upon UnBind.
		 */
		if (sq->sq_nodeid != myid)
		{
			elog(LOG, "Looking for consumer %d in %s", myid, sqname);
			/* find specified node in the consumer lists */
			for (i = 0; i < sq->sq_nconsumers; i++)
			{
				ConsState *cstate = &(sq->sq_consumers[i]);
				if (cstate->cs_node == myid)
				{
					LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock,
								  LW_EXCLUSIVE);
					if (cstate->cs_status != CONSUMER_DONE)
					{
						/* Inform producer the consumer have done the job */
						cstate->cs_status = CONSUMER_DONE;
						/* no need to receive notifications */
						if (cstate->cs_pid > 0)
						{
							DisownLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
							cstate->cs_pid = 0;
						}
						/*
						 * notify the producer, it may be waiting while
						 * consumers are finishing
						 */
						SetLatch(&sqsync->sqs_producer_latch);
						elog(LOG, "Release consumer %d of %s", i, sqname);
					}
					LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
					/* exit */
					LWLockRelease(SQueuesLock);
					return;
				}
			}
			/*
			 * The consumer was never bound. Find empty consumer slot and
			 * register node here to let producer know that the node will never
			 * be consuming.
			 */
			for (i = 0; i < sq->sq_nconsumers; i++)
			{
				ConsState *cstate = &(sq->sq_consumers[i]);
				if (cstate->cs_node == -1)
				{
					LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock,
								  LW_EXCLUSIVE);
					/* Inform producer the consumer have done the job */
					cstate->cs_status = CONSUMER_DONE;
					SetLatch(&sqsync->sqs_producer_latch);
					elog(LOG, "Release not bound consumer %d of %s", i, sqname);
					LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
				}
			}
		}
	}
	LWLockRelease(SQueuesLock);
}


/*
 * Called when the backend is ending.
 */
void
SharedQueuesCleanup(int code, Datum arg)
{
	/* Need to be able to look into catalogs */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "SharedQueuesCleanup");

	/*
	 * Release all registered prepared statements.
	 * If a shared queue name is associated with the statement this queue will
	 * be released.
	 */
	DropAllPreparedStatements();

	/* Release everything */
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
	CurrentResourceOwner = NULL;
}


/*
 * sq_push_long_tuple
 *    Routine to push through the consumer state tuple longer the the consumer
 *    queue. Long tuple is written by a producer partially, and only when the
 *    consumer queue is empty.
 *    The consumer can determine that the tuple being read is long if the length
 *    of the tuple which is read before data is exceeding queue length.
 * 	  Consumers is switching to the long tuple mode and read in the portion of
 *	  data which is already in the queue. After reading in each portion of data
 *    consumer sets cs_ntuples to LONG_TUPLE to indicate it is in long tuple
 *    mode, and writes out number of already read bytes to the beginning of the
 *    queue.
 *    While Consumer is reading in tuple data Producer may work on other task:
 *    execute query and send tuples to other Customers. If Producer sees the
 *    LONG_TUPLE indicator it may write out next portion. The tuple remains
 *    current in the tuplestore, and Producer just needs to read offset from
 *    the buffer to know what part of data to write next.
 *    After tuple is completely written the Producer is advancing to next tuple
 *    and continue operation in normal mode.
 */
static bool
sq_push_long_tuple(ConsState *cstate, RemoteDataRow datarow)
{
	if (cstate->cs_ntuples == 0)
	{
		/* the tuple is too big to fit the queue, start pushing it through */
		int len;
		/*
		 * Output actual message size, to prepare consumer:
		 * allocate memory and set up transmission.
		 */
		QUEUE_WRITE(cstate, sizeof(int), (char *) &datarow->msglen);
		/* Output as much as possible */
		len = cstate->cs_qlength - sizeof(int);
		Assert(datarow->msglen > len);
		QUEUE_WRITE(cstate, len, datarow->msg);
		cstate->cs_ntuples = 1;
		return false;
	}
	else
	{
		int offset;
		int	len;

		/* Continue pushing through long tuple */
		Assert(cstate->cs_ntuples == LONG_TUPLE);
		/*
		 * Consumer outputs number of bytes already read at the beginning of
		 * the queue.
		 */
		memcpy(&offset, cstate->cs_qstart, sizeof(int));

		Assert(offset > 0 && offset < datarow->msglen);

		/* remaining data */
		len = datarow->msglen - offset;
		/*
		 * We are sending remaining lengs just for sanity check at the consumer
		 * side
		 */
		QUEUE_WRITE(cstate, sizeof(int), (char *) &len);
		if (len > cstate->cs_qlength - sizeof(int))
		{
			/* does not fit yet */
			len = cstate->cs_qlength - sizeof(int);
			QUEUE_WRITE(cstate, len, datarow->msg + offset);
			cstate->cs_ntuples = 1;
			return false;
		}
		else
		{
			/* now we are done */
			QUEUE_WRITE(cstate, len, datarow->msg + offset);
			cstate->cs_ntuples = 1;
			return true;
		}
	}
}


/*
 * sq_pull_long_tuple
 *    Read in from the queue data of a long tuple which does not the queue.
 *    See sq_push_long_tuple for more details
 */
static void
sq_pull_long_tuple(ConsState *cstate, RemoteDataRow datarow,
							   ConsumerSync *sync)
{
	int offset = 0;
	int len = datarow->msglen;

	for (;;)
	{
		/* determine how many bytes to read */
		if (len > cstate->cs_qlength - sizeof(int))
			len = cstate->cs_qlength - sizeof(int);

		/* read data */
		QUEUE_READ(cstate, len, datarow->msg + offset);

		/* remember how many we read already */
		offset += len;

		/* check if we are done */
		if (offset == datarow->msglen)
			return;

		/* need more, set up queue to accept data from the producer */
		Assert(cstate->cs_ntuples == 1); /* allow exactly one incomplete tuple */
		cstate->cs_ntuples = LONG_TUPLE; /* long tuple mode marker */
		/* Inform producer how many bytes we have already */
		memcpy(cstate->cs_qstart, &offset, sizeof(int));
		/* Release locks and wait until producer supply more data */
		while (cstate->cs_ntuples == LONG_TUPLE)
		{
			/* prepare wait */
			ResetLatch(&sync->cs_latch);
			LWLockRelease(sync->cs_lwlock);
			/* Wait for notification about available info */
			WaitLatch(&sync->cs_latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1);
			/* got the notification, restore lock and try again */
			LWLockAcquire(sync->cs_lwlock, LW_EXCLUSIVE);
		}
		/* Read length of remaining data */
		QUEUE_READ(cstate, sizeof(int), (char *) &len);

		/* Make sure we are doing the same tuple */
		Assert(offset + len == datarow->msglen);

		/* next iteration */
	}
}
