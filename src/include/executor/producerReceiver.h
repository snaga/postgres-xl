/*-------------------------------------------------------------------------
 *
 * producerReceiver.h
 *	  prototypes for producerReceiver.c
 *
 *
 * Copyright (c) 2012-2014, TransLattice, Inc.
 *
 * src/include/executor/producerReceiver.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PRODUCER_RECEIVER_H
#define PRODUCER_RECEIVER_H

#include "tcop/dest.h"
#include "pgxc/locator.h"
#include "pgxc/squeue.h"


extern DestReceiver *CreateProducerDestReceiver(void);

extern void SetProducerDestReceiverParams(DestReceiver *self,
							  AttrNumber distKey,
							  Locator *locator,
							  SharedQueue squeue);
extern DestReceiver *SetSelfConsumerDestReceiver(DestReceiver *self,
							DestReceiver *consumer);
extern void SetProducerTempMemory(DestReceiver *self, MemoryContext tmpcxt);
extern bool ProducerReceiverPushBuffers(DestReceiver *self);

#endif   /* PRODUCER_RECEIVER_H */
