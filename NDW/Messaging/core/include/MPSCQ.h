
#ifndef _NWD_MPSCQ_H
#define _NWD_MPSCQ_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>

#include "ndw_types.h"
#include "NDW_Utils.h"
#include "RegistryData.h"
#include "AbstractMessaging.h"


#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

typedef struct ndw_MPSCQNode
{
    void* data;                     // Pointer to the payload
    struct ndw_MPSCQNode* next;     // Pointer to the next node
    LONG_T sequence;                // Unique sequence number for testing/debugging
} ndw_MPSCQNode_T;

typedef struct ndw_MPSCQ
{
    LONG_T max_queue_items;
    ndw_MPSCQNode_T* consumer_last_accessed_node; // Used to release previously returned node

    ndw_MPSCQNode_T* consumer_cached_head;    // Local cached head pointer for consumer
    ndw_MPSCQNode_T* consumer_cached_limit;   // Local cached limit pointer for consumer

    ndw_MPSCQNode_T* _Atomic consumer_limit;                 // Stores pointer cast to (intptr_t)
                                                // Shared atomic limit pointer updated by producers

    atomic_long consumer_pull_count;           // Number of items pulled by the consumer
    atomic_long producer_push_count;           // Number of items pushed by all producers

    pthread_mutex_t producer_lock;             // Mutex to synchronize multiple producers

    ndw_MPSCQNode_T* producer_queue_tail;      // Tail pointer used by producers to append
} ndw_MPSCQ_T;


extern ndw_MPSCQ_T* ndw_CreateMPSCQueue(LONG_T max_items);

INT_T ndw_mpscq_insert(ndw_MPSCQ_T* queue, void* data);

void* ndw_mpscq_get(ndw_MPSCQ_T* queue, LONG_T timeout_in_milliseconds);

extern void ndw_mpscq_release(ndw_MPSCQNode_T* node);

extern void ndw_mpscq_cleanup(ndw_MPSCQ_T* queue);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /*  _NWD_MPSCQ_H */


