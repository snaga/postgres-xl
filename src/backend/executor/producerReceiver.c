/*-------------------------------------------------------------------------
 *
 * producerReceiver.c
 *	  An implementation of DestReceiver that distributes the result tuples to
 *	  multiple customers via a SharedQueue.
 *
 *
 * Copyright (c) 2012-2014, TransLattice, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/executor/producerReceiver.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/producerReceiver.h"
#include "pgxc/nodemgr.h"
#include "tcop/pquery.h"
#include "utils/tuplestore.h"

typedef struct
{
	DestReceiver pub;
	/* parameters: */
	DestReceiver *consumer;		/* where to put the tuples for self */
	AttrNumber distKey;			/* distribution key attribute in the tuple */
	Locator *locator;			/* locator is determining destination nodes */
	int *distNodes;				/* array where to get locator results */
	int *consMap;				/* map of consumers: consMap[node-1] indicates
								 * the target consumer */
	SharedQueue squeue;			/* a SharedQueue for result distribution */
	MemoryContext tmpcxt;       /* holds temporary data */
	Tuplestorestate **tstores;	/* storage to buffer data if destination queue
								 * is full */
	TupleDesc typeinfo;			/* description of received tuples */
	long tcount;
	long selfcount;
	long othercount;
} ProducerState;


/*
 * Prepare to receive tuples from executor.
 */
static void
producerStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	ProducerState *myState = (ProducerState *) self;

	if (ActivePortal)
	{
		/* Normally ExecutorContext is current here. However we should better
		 * create local producer storage in the Portal's context: producer
		 * may keep pushing records to consumers after executor is destroyed.
		 */
		MemoryContext savecontext;
		savecontext = MemoryContextSwitchTo(PortalGetHeapMemory(ActivePortal));
		myState->typeinfo = CreateTupleDescCopy(typeinfo);
		MemoryContextSwitchTo(savecontext);
	}
	else
		myState->typeinfo = typeinfo;

	if (myState->consumer)
		(*myState->consumer->rStartup) (myState->consumer, operation, typeinfo);
}

/*
 * Receive a tuple from the executor and dispatch it to the proper consumer
 */
static void
producerReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
{
	ProducerState *myState = (ProducerState *) self;
	Datum		value;
	bool		isnull;
	int 		ncount, i;

	if (myState->distKey == InvalidAttrNumber)
	{
		value = (Datum) 0;
		isnull = true;
	}
	else
		value = slot_getattr(slot, myState->distKey, &isnull);
	ncount = GET_NODES(myState->locator, value, isnull, NULL);

	myState->tcount++;
	/* Dispatch the tuple */
	for (i = 0; i < ncount; i++)
	{
		int consumerIdx = myState->distNodes[i];

		if (consumerIdx == SQ_CONS_NONE)
		{
			continue;
		}
		else if (consumerIdx == SQ_CONS_SELF)
		{
			Assert(myState->consumer);
			(*myState->consumer->receiveSlot) (slot, myState->consumer);
			myState->selfcount++;
		}
		else if (myState->squeue)
		{
			/*
			 * If the tuple will not fit to the consumer queue it will be stored
			 * in the local tuplestore. The tuplestore should be in the portal
			 * context, because ExecutorContext may be destroyed when tuples
			 * are not yet pushed to the consumer queue.
			 */
			MemoryContext savecontext;
			Assert(ActivePortal);
			savecontext = MemoryContextSwitchTo(PortalGetHeapMemory(ActivePortal));
			SharedQueueWrite(myState->squeue, consumerIdx, slot,
							 &myState->tstores[consumerIdx], myState->tmpcxt);
			MemoryContextSwitchTo(savecontext);
			myState->othercount++;
		}
	}
}


/*
 * Clean up at end of an executor run
 */
static void
producerShutdownReceiver(DestReceiver *self)
{
	ProducerState *myState = (ProducerState *) self;

	if (myState->consumer)
		(*myState->consumer->rShutdown) (myState->consumer);
}


/*
 * Destroy receiver when done with it
 */
static void
producerDestroyReceiver(DestReceiver *self)
{
	ProducerState *myState = (ProducerState *) self;

	elog(LOG, "Producer stats: total %ld tuples, %ld tuples to self, %ld to other nodes",
		 myState->tcount, myState->selfcount, myState->othercount);

	if (myState->consumer)
		(*myState->consumer->rDestroy) (myState->consumer);

	/* Make sure all data are in the squeue */
	while (myState->tstores)
	{
		if (SharedQueueFinish(myState->squeue, myState->typeinfo,
							  myState->tstores) == 0)
		{
			pfree(myState->tstores);
			myState->tstores = NULL;
		}
		else
		{
			pg_usleep(10000l);
			/*
			 * Do not wait for consumers that was not even connected after 10
			 * seconds after start waiting for their disconnection.
			 * That should help to break the loop which would otherwise endless.
			 * The error will be emitted later in SharedQueueUnBind
			 */
			SharedQueueResetNotConnected(myState->squeue);
		}
	}

	/* wait while consumer are finishing and release shared resources */
	if (myState->squeue)
		SharedQueueUnBind(myState->squeue);
	myState->squeue = NULL;

	/* Release workspace if any */
	if (myState->locator)
		freeLocator(myState->locator);
	pfree(myState);
}


/*
 * Initially create a DestReceiver object.
 */
DestReceiver *
CreateProducerDestReceiver(void)
{
	ProducerState *self = (ProducerState *) palloc0(sizeof(ProducerState));

	self->pub.receiveSlot = producerReceiveSlot;
	self->pub.rStartup = producerStartupReceiver;
	self->pub.rShutdown = producerShutdownReceiver;
	self->pub.rDestroy = producerDestroyReceiver;
	self->pub.mydest = DestProducer;

	/* private fields will be set by SetTuplestoreDestReceiverParams */
	self->tcount = 0;
	self->selfcount = 0;
	self->othercount = 0;

	return (DestReceiver *) self;
}


/*
 * Set parameters for a ProducerDestReceiver
 */
void
SetProducerDestReceiverParams(DestReceiver *self,
							  AttrNumber distKey,
							  Locator *locator,
							  SharedQueue squeue)
{
	ProducerState *myState = (ProducerState *) self;

	Assert(myState->pub.mydest == DestProducer);
	myState->distKey = distKey;
	myState->locator = locator;
	myState->squeue = squeue;
	myState->typeinfo = NULL;
	myState->tmpcxt = NULL;
	/* Create workspace */
	myState->distNodes = (int *) getLocatorResults(locator);
	if (squeue)
		myState->tstores = (Tuplestorestate **)
			palloc0(NumDataNodes * sizeof(Tuplestorestate *));
}


/*
 * Set a DestReceiver to receive tuples targeted to "self".
 * Returns old value of the self consumer
 */
DestReceiver *
SetSelfConsumerDestReceiver(DestReceiver *self,
							DestReceiver *consumer)
{
	ProducerState *myState = (ProducerState *) self;
	DestReceiver *oldconsumer;

	Assert(myState->pub.mydest == DestProducer);
	oldconsumer = myState->consumer;
	myState->consumer = consumer;
	return oldconsumer;
}


/*
 * Set a memory context to hold temporary data
 */
void
SetProducerTempMemory(DestReceiver *self, MemoryContext tmpcxt)
{
	ProducerState *myState = (ProducerState *) self;
	DestReceiver *oldconsumer;

	Assert(myState->pub.mydest == DestProducer);
	myState->tmpcxt = tmpcxt;
}


/*
 * Push data from the local tuplestores to the shared memory so consumers can
 * read them. Returns true if all data are pushed, false if something remains
 * in the tuplestores yet.
 */
bool
ProducerReceiverPushBuffers(DestReceiver *self)
{
	ProducerState *myState = (ProducerState *) self;

	Assert(myState->pub.mydest == DestProducer);
	if (myState->tstores)
	{
		if (SharedQueueFinish(myState->squeue, myState->typeinfo,
							  myState->tstores) == 0)
		{
			pfree(myState->tstores);
			myState->tstores = NULL;
		}
		else
			return false;
	}
	return true;
}
