
#include "QueueImpl.h"

#define NDW_Q_BATCH_ID 1
#define NDW_Q_SWEEP_ID 2

#define NDW_Q_BATCH_NAME "QBatch"
#define NDW_Q_SWEEP_NAME "QSweep"

typedef struct NDW_QImpl NDW_QImpl_T;

typedef struct NDW_QImpl
{
    const char*   q_name;
    INT_T   q_id;
    void*   q;

    INT_T   (*insert)(NDW_QImpl_T* impl, void* data);
    INT_T   (*get)(NDW_QImpl_T* impl, NDW_QData_T* data, LONG_T timeout_ms);
    void    (*delete_current)(NDW_QImpl_T* impl);
    void    (*set_cleanup_operator)(NDW_QImpl_T* impl, ndw_QueueCleanupOperator); 
    void    (*cleanup)(NDW_QImpl_T* impl);
    void    (*print_debug)(NDW_QImpl_T* impl);

} NDW_QImpl_T;


typedef struct ndw_QNode ndw_QNode_T;

typedef struct ndw_QNode
{
    void* data;                     // user data
    void *pData;                    // Private data
    ndw_QNode_T* next;              // Pointer to next node
    LONG_T sequence_number;       // Queue sequence number. Always increases on every insert.
    ULONG_T insert_time;            // Insert time in UTC

} ndw_QNode_T;

/*
 * BEGIN: NDW_QBatch Implementation.
 */
typedef struct NDW_QBatch
{
    const char* q_name;
    INT_T q_id;
    LONG_T max_queue_items;

    ndw_QNode_T* q_producer_head;
    ndw_QNode_T* q_producer_tail;
    LONG_T q_producer_items;
    pthread_mutex_t producer_lock;
    LONG_T q_producer_sequence_number;
    LONG_T q_producer_last_insert_time;

    ndw_QNode_T* q_consumer_head;
    ndw_QNode_T* q_consumer_tail;
    LONG_T q_consumer_items;
    LONG_T q_consumer_total_consumed;
    ndw_QNode_T* consumer_last_node_consumed;

    ndw_QueueCleanupOperator cleanup_operator;

} NDW_QBatch_T;

NDW_QBatch_T* ndw_qBatch_GetImpl(NDW_QImpl_T* impl)
{
    if (NULL ==  impl) {
        NDW_LOGERR("NULL implementation Pointer!\n");
        exit(EXIT_FAILURE);
    }

    NDW_QBatch_T* q = (NDW_QBatch_T*) impl->q;
    if (NULL == q) {
        NDW_LOGERR("NULL implementation Pointer insdie NDW_QImpl_T*!\n");
        exit(EXIT_FAILURE);
    }

    if (NDW_Q_BATCH_ID != impl->q_id) {
        NDW_LOGERR("QBatch does not match Implementation Pointer. Q_ID<%d> Expected %d\n",
            impl->q_id, NDW_Q_BATCH_ID);
        exit(EXIT_FAILURE);
    }

    return q;
}


// Implementation: ndw_CreateQBatchQueue
NDW_QBatch_T* ndw_CreateQBatchQueue(LONG_T max_items)
{
    NDW_QBatch_T* q = (NDW_QBatch_T*) calloc(1, sizeof(NDW_QBatch_T));
    if (!q) {
        fprintf(stderr, "Failed to allocate QBatch queue.\n");
        return NULL;
    }

    q->max_queue_items = max_items;

    if (pthread_mutex_init(&q->producer_lock, NULL) != 0) {
        fprintf(stderr, "Failed to initialize producer mutex.\n");
        free(q);
        return NULL;
    }

    // All other fields are zeroed out by calloc
    return q;
}

INT_T ndw_qBatch_insert(NDW_QImpl_T* impl, void* data)
{
    NDW_QBatch_T* q = ndw_qBatch_GetImpl(impl);
    if (q == NULL || data == NULL)
        return -1;

    ndw_QNode_T* node = (ndw_QNode_T*) malloc(sizeof(ndw_QNode_T));
    if (!node)
        return -1;

    node->data = data;
    node->next = NULL;

    pthread_mutex_lock(&q->producer_lock);

    if (q->max_queue_items > 0 && q->q_producer_items >= q->max_queue_items) {
        pthread_mutex_unlock(&q->producer_lock);
        free(node);
        return -1; // Queue full
    }

    node->sequence_number = ++q->q_producer_sequence_number;
    node->insert_time = ndw_GetCurrentNanoSeconds();

    if (q->q_producer_tail) {
        q->q_producer_tail->next = node;
    } else {
        q->q_producer_head = node;
    }

    q->q_producer_tail = node;
    q->q_producer_items++;

    pthread_mutex_unlock(&q->producer_lock);

    return 0;
}

// Hybrid implementation of transfer logic for qBatch queue

static inline void ndw_qBatch_transfer_producer_locked(NDW_QBatch_T* q)
{
    if (q->q_producer_head) {
        q->q_consumer_head = q->q_producer_head;
        q->q_consumer_tail = q->q_producer_tail;
        q->q_consumer_items = q->q_producer_items;

        q->q_producer_head = NULL;
        q->q_producer_tail = NULL;
        q->q_producer_items = 0;
    }
}

static inline void ndw_qBatch_transfer_producer(NDW_QBatch_T* q)
{
    pthread_mutex_lock(&q->producer_lock);
    ndw_qBatch_transfer_producer_locked(q);
    pthread_mutex_unlock(&q->producer_lock);
}

INT_T ndw_qBatch_get(NDW_QImpl_T* impl, NDW_QData_T* data, LONG_T timeout_us)
{
    NDW_QBatch_T* q = ndw_qBatch_GetImpl(impl);
    memset(data, 0, sizeof(NDW_QData_T));

    if (timeout_us < 0)
        timeout_us = 1;

    ndw_QNode_T* node = NULL;

    if (q->q_consumer_head)
        goto out;

    ndw_qBatch_transfer_producer(q);  // First attempt

    if (!q->q_consumer_head) {
        if (timeout_us > 0) {
            struct timespec delay = {
                .tv_sec = timeout_us / 1000000,
                .tv_nsec = (timeout_us % 1000000) * 1000
            };
            nanosleep(&delay, NULL);
        }

        ndw_qBatch_transfer_producer(q);  // Retry after sleep

        if (!q->q_consumer_head)
            return 0;
    }

out:
    node = q->q_consumer_head;
    q->q_consumer_head = node->next;
    if (!q->q_consumer_head)
        q->q_consumer_tail = NULL;

    q->q_consumer_items--;
    q->q_consumer_total_consumed++;
    q->consumer_last_node_consumed = node;

    node->next = NULL;

    data->data = node->data;
    data->consumption_sequence_number = node->sequence_number;
    data->consumption_insertion_time = node->insert_time;
    data->consumer_pending_items = q->q_consumer_items;
    data->total_consumed_items = q->q_consumer_total_consumed;

    return 1;
}


void ndw_qBatch_delete_current(NDW_QImpl_T* impl)
{
    NDW_QBatch_T* q = ndw_qBatch_GetImpl(impl);
    ndw_QNode_T* node = q->consumer_last_node_consumed;
    if (NULL == node) {
        fprintf(stderr, "*** FATAL ERROR: Attempted to delete NULL node. "
                "There is no q->consumer_last_node_consumed!\n");
        ndw_exit(EXIT_FAILURE);
    }

    if ((NULL != node->data) && (NULL != q->cleanup_operator)) {
        q->cleanup_operator(node->data);
    }


    memset(node, 0, sizeof(ndw_QNode_T));
    free(node);
    q->consumer_last_node_consumed = NULL;
}

void ndw_qBatch_set_cleanup_operator(NDW_QImpl_T* impl, ndw_QueueCleanupOperator cleanup_operator)
{
    NDW_QBatch_T* q = ndw_qBatch_GetImpl(impl);
    q->cleanup_operator = cleanup_operator;
}

void ndw_qBatch_cleanup(NDW_QImpl_T* impl)
{
    NDW_QBatch_T* q = ndw_qBatch_GetImpl(impl);
    if (!q)
        return;

    // Clean up any remaining producer nodes
    pthread_mutex_lock(&q->producer_lock);
    ndw_QNode_T* node = q->q_producer_head;
    while (node) {
        ndw_QNode_T* next = node->next;
        if (NULL != q->cleanup_operator)
            q->cleanup_operator(node->data);
        free(node);
        node = next;
    }
    pthread_mutex_unlock(&q->producer_lock);

    // Clean up any remaining consumer nodes
    node = q->q_consumer_head;
    while (node) {
        ndw_QNode_T* next = node->next;
        if (NULL != q->cleanup_operator)
            q->cleanup_operator(node->data);
        free(node);
        node = next;
    }

    // Also free last node if consumer forgot to
    if (q->consumer_last_node_consumed) {
        if (NULL != q->cleanup_operator)
            q->cleanup_operator(q->consumer_last_node_consumed->data);
        free(q->consumer_last_node_consumed);
        q->consumer_last_node_consumed = NULL;
    }

    pthread_mutex_destroy(&q->producer_lock);
    free(q);
}

void ndw_qBatch_print_debug(NDW_QImpl_T* impl)
{
    NDW_QBatch_T* q = ndw_qBatch_GetImpl(impl);
    pthread_mutex_lock(&q->producer_lock);
    NDW_LOG("QBatch<%d, %s>: max_queue_items<%ld> producer_items<%ld> producer_sequence_number<%ld> "
            "consumer_items<%ld> consumer_total_consumed<%ld>\n",
            q->q_id, q->q_name, q->max_queue_items, q->q_producer_items, q->q_producer_sequence_number,
            q->q_consumer_items, q->q_consumer_total_consumed);
    pthread_mutex_unlock(&q->producer_lock);
}


/*
 * END: NDW_QBatch Implementation.
 */

/*
 * BEGIN: ndw_QSweep Implementation.
 */

/*
 * END: ndw_QSweep Implementation.
 */

/*
 * BEGIN: Queue Implementation Factory.
 */

NDW_QData_T* ndw_CreateQData()
{
    NDW_QData_T* data = calloc(1, sizeof(NDW_QData_T));
    return data;
}

void ndw_ReleaseQData(NDW_QData_T* data)
{
    if (data)
        free(data);
}

NDW_Q_T*
ndw_CreateInboundDataQueue(const char* queue_type, LONG_T max_items)
{
    if (NDW_ISNULLCHARPTR(queue_type)) {
        NDW_LOGERR("NULL queue_type specified!\n");
        exit(EXIT_FAILURE);
    }

    NDW_Q_T* Q = (NDW_Q_T*) calloc(1, sizeof(NDW_Q_T));
    NDW_QImpl_T* impl = calloc(1, sizeof(NDW_QImpl_T));
    Q->impl = impl;

    if (0 == strcasecmp(NDW_Q_BATCH_NAME, queue_type))
    {
        NDW_QBatch_T* q = ndw_CreateQBatchQueue(max_items);
        impl->q = q;
        impl->q_name = queue_type;
        impl->q_id = NDW_Q_BATCH_ID;
        q->q_id = NDW_Q_BATCH_ID;
        q->q_name = NDW_Q_BATCH_NAME;

        impl->insert = ndw_qBatch_insert;
        impl->get = ndw_qBatch_get;
        impl->delete_current = ndw_qBatch_delete_current;
        impl->set_cleanup_operator = ndw_qBatch_set_cleanup_operator;
        impl->cleanup = ndw_qBatch_cleanup;
        impl->print_debug = ndw_qBatch_print_debug;
    }
    else
    {
        NDW_LOGERR("Invalid Inbound Queue Type Name<%s> specified\n", queue_type);

        ndw_ReleaseInboundDataQueue(Q);
        exit(EXIT_FAILURE);
    }

    return Q;

} // end method ndw_CreateInboundDataQueues

void ndw_ReleaseInboundDataQueue( NDW_Q_T* Q)
{
    if (NULL != Q) {
        if (NULL != Q->impl) {
            free(Q->impl);
            Q->impl = NULL;
        }
        free(Q);
    }
}

INT_T
ndw_QInsert(NDW_Q_T* Q, void* data)
{
    NDW_QImpl_T* impl = (NDW_QImpl_T*) Q->impl;
    return impl->insert(impl, data);
}

INT_T
ndw_QGet(NDW_Q_T* Q, NDW_QData_T* data, LONG_T timeout_ms)
{
    NDW_QImpl_T* impl = (NDW_QImpl_T*) Q->impl;
    return impl->get(impl, data, timeout_ms);
}

void
ndw_QDeleteCurrent(NDW_Q_T* Q)
{
    NDW_QImpl_T* impl = (NDW_QImpl_T*) Q->impl;
    impl->delete_current(impl);
}

void
ndw_QSetCleanupOperator(NDW_Q_T* Q, ndw_QueueCleanupOperator cleanup_operator)
{
    NDW_QImpl_T* impl = (NDW_QImpl_T*) Q->impl;
    impl->set_cleanup_operator(impl, cleanup_operator);
}

void
ndw_QCleanup(NDW_Q_T* Q)
{
    NDW_QImpl_T* impl = (NDW_QImpl_T*) Q->impl;
    impl->cleanup(impl);
    free(impl);
}

void
ndw_QPrintDebug(NDW_Q_T* Q)
{
    NDW_QImpl_T* impl = (NDW_QImpl_T*) Q->impl;
    impl->print_debug(impl);
}


/*
 * END: Queue Implementation Factory.
 */

