
#include "MPSCQ.h"


ndw_MPSCQ_T*
ndw_CreateMPSCQueue(LONG_T max_items)
{
    ndw_MPSCQ_T* queue = calloc(1, sizeof(ndw_MPSCQ_T));
    if (NULL == queue) {
        fprintf(stderr, "Failed to allocate memory for MPSC queue.\n");
        return NULL;
    }

    if (0 != pthread_mutex_init(&queue->producer_lock, NULL)) {
        fprintf(stderr, "Failed to initialize producer mutex.\n");
        free(queue);
        return NULL;
    }

    queue->max_queue_items = max_items;
    return queue;
}

// Insert into queue (returns 0 or -1 on error)
INT_T
ndw_mpscq_insert(ndw_MPSCQ_T* queue, void* data)
{
    ndw_MPSCQNode_T* node = calloc(1, sizeof(ndw_MPSCQNode_T));
    if (NULL == node) {
        fprintf(stderr, "Failed to allocate memory for new node.\n");
        return -1;
    }
    node->data = data;

    pthread_mutex_lock(&queue->producer_lock);

    node->sequence = queue->producer_push_count + 1;

    if (0 == queue->producer_push_count) {
        queue->consumer_cached_head = node;
        queue->producer_queue_tail = node;
        atomic_store(&queue->consumer_limit, node);
    } else {
        queue->producer_queue_tail->next = node;
        queue->producer_queue_tail = node;
        atomic_store(&queue->consumer_limit, node);
    }

    atomic_fetch_add(&queue->producer_push_count, 1);

    pthread_mutex_unlock(&queue->producer_lock);
    return 0;
} 

void*
ndw_mpscq_get(ndw_MPSCQ_T* queue, LONG_T timeout_in_milliseconds)
{
    // free previously accessed node
    if (queue->consumer_last_accessed_node) {
        free(queue->consumer_last_accessed_node);
        queue->consumer_last_accessed_node = NULL;
    }

    // First pull: initialize cache limit
    if (0 == atomic_load(&queue->consumer_pull_count)) {
        ndw_MPSCQNode_T* limit = (ndw_MPSCQNode_T*)(intptr_t)atomic_load(&queue->consumer_limit);
        if (!limit) {
        struct timespec req = {
            .tv_sec  = timeout_in_milliseconds / 1000,
            .tv_nsec = (timeout_in_milliseconds % 1000) * 1000000
        };
        nanosleep(&req, NULL);

            limit = (ndw_MPSCQNode_T*)(intptr_t)atomic_load(&queue->consumer_limit);
            if (!limit) return NULL;
        }

        queue->consumer_cached_limit = limit;
        ndw_MPSCQNode_T* node = queue->consumer_cached_head;
        atomic_fetch_add(&queue->consumer_pull_count, 1);
        return node;
    }

// Fetching subsequent nodes
fetch_next:
    if (queue->consumer_cached_head != queue->consumer_cached_limit) {
        ndw_MPSCQNode_T* next = queue->consumer_cached_head->next;
        queue->consumer_last_accessed_node = queue->consumer_cached_head;
        queue->consumer_cached_head = next;
        atomic_fetch_add(&queue->consumer_pull_count, 1);
        return next;
    }

    // Refresh limit and try again
    ndw_MPSCQNode_T* new_limit = (ndw_MPSCQNode_T*)(intptr_t)atomic_load(&queue->consumer_limit);
    if (new_limit == queue->consumer_cached_limit) {
        struct timespec req = {
            .tv_sec  = timeout_in_milliseconds / 1000,
            .tv_nsec = (timeout_in_milliseconds % 1000) * 1000000
        };
        nanosleep(&req, NULL);

        new_limit = (ndw_MPSCQNode_T*)(intptr_t)atomic_load(&queue->consumer_limit);
        if (new_limit == queue->consumer_cached_limit) return NULL;
    }

    queue->consumer_cached_limit = new_limit;
    goto fetch_next;
}

void
ndw_mpscq_release(ndw_MPSCQNode_T* node)
{
    (void) node;
}

void
ndw_mpscq_cleanup(ndw_MPSCQ_T* queue)
{
    pthread_mutex_lock(&queue->producer_lock);

    ndw_MPSCQNode_T* node = queue->consumer_cached_head;
    while (node) {
        ndw_MPSCQNode_T* next = node->next;
        free(node);
        node = next;
    }

    queue->consumer_cached_head = NULL;
    queue->consumer_last_accessed_node = NULL;
    queue->consumer_cached_limit = NULL;
    queue->producer_queue_tail = NULL;
    atomic_store(&queue->consumer_limit, NULL);

    pthread_mutex_unlock(&queue->producer_lock);
}

