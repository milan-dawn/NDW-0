
#ifndef _NDWQUEUEIMPL_H
#define _NDWQUEUEIMPL_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>

#include "ndw_types.h"
#include "NDW_Utils.h"

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

typedef struct NDW_QImpl NDW_QImpl_T;

typedef struct NDW_Q_T
{
    void*    impl;
} NDW_Q_T;

typedef struct NDW_QData
{
    void*   data;
    LONG_T  consumption_sequence_number;
    ULONG_T  consumption_insertion_time;
    LONG_T  consumer_pending_items;
    LONG_T  total_consumed_items;

    LONG_T  producer_sequence_number;
    LONG_T  producer_pending_items;
    LONG_T  total_produced_items;

} NDW_QData_T;

extern NDW_Q_T* ndw_CreateInboundDataQueue(const char* queue_type, LONG_T max_items);
extern void ndw_ReleaseInboundDataQueue(NDW_Q_T* Q);

extern NDW_QData_T* ndw_CreateQData();
extern void ndw_ReleaseQData(NDW_QData_T* data);

INT_T   ndw_QInsert(NDW_Q_T* Q, void* data);
INT_T   ndw_QGet(NDW_Q_T* Q, NDW_QData_T* data, LONG_T timeout_us);
void    ndw_QDeleteCurrent(NDW_Q_T* Q);

typedef void (*ndw_QueueCleanupOperator)(void* data);

void    ndw_QSetCleanupOperator(NDW_Q_T* Q, ndw_QueueCleanupOperator cleanup_operator);
void    ndw_QCleanup(NDW_Q_T* Q);
void    ndw_QPrintDebug(NDW_Q_T* Q);

#ifdef __cplusplus
}
#endif /* _cplusplus */

#endif /* _NDWQUEUEIMPL_H */

