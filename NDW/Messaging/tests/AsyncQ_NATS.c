

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <stdatomic.h>

#include "NDW_Essentials.h"


// Thread-safe printf implementation
pthread_mutex_t printf_mutex = PTHREAD_MUTEX_INITIALIZER;

void safe_printf(FILE *stream, const char *format, ...) {
    va_list args;
    pthread_mutex_lock(&printf_mutex);
    va_start(args, format);
    vfprintf(stream, format, args);
    va_end(args);
    pthread_mutex_unlock(&printf_mutex);
}

// Domain topics
const char *domains[] = {"DomainA", NULL};

const char *publisher_topics[] = {
    "DomainA^NATSConn1^ACME.Orders",
};

int num_publisher_topics = sizeof(publisher_topics) / sizeof(publisher_topics[0]);

const char *subscriber_topics[] = {
    "DomainA^NATSConn1^ACME.Orders"
};

int num_subscriber_topics = sizeof(subscriber_topics) / sizeof(subscriber_topics[0]);

// Thread args
typedef struct {
    int num_topics;
    const char **topic_names;
    int num_msgs;
    int msg_size;
    int delay_us;
    int ret_code;
} publisher_args;

typedef struct {
    int num_topics;
    const char **topic_names;
    int expected_messages;
    int timeout_sec;
    int ret_code;
} subscribe_args;

publisher_args pub_args;
subscribe_args sub_args;

// Thread function pointer type
typedef void* (*thread_func_t)(void *);

atomic_long total_published = 0;
atomic_long total_received = 0;
atomic_long total_async_received = 0;
int consumer_delay_us = 100000;
int num_messages = 1;
int message_size = 1024;
int subscriber_timeout_sec = 6;
int publisher_delay_us = 10;
int simulated_delay_ms = 500;

pthread_mutex_t start_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t start_cond = PTHREAD_COND_INITIALIZER;
int start_flag = 0;

void print_timestamp(const char *label) {
    struct timespec ts;
    struct tm tm_info;
    clock_gettime(CLOCK_REALTIME, &ts);
    localtime_r(&ts.tv_sec, &tm_info);

    long ms = ts.tv_nsec / 1000000;
    long us = (ts.tv_nsec / 1000) % 1000;
    long ns = ts.tv_nsec % 1000;

    NDW_LOG("[%s] HH:MM:SS:ms:us:ns %02d:%02d:%02d:%03ld:%03ld:%03ld\n",
                label, tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec, ms, us, ns);
}

int receive_message() {
    if (simulated_delay_ms > 0) usleep(simulated_delay_ms * 1000);
    int r = rand() % 10;
    if (r < 6) return 0;
    else if (r == 9) return -1;
    else return 1;
}

ndw_Topic_T**
getTopicsFromNames(int num_topics, const char** topic_names)
{
    ndw_Topic_T** topics = calloc(1, num_topics * sizeof(ndw_Topic_T*));
    for (int i = 0; i < num_topics; i++) {
        ndw_Topic_T* t = ndw_GetTopicFromFullPath(topic_names[i]);
        if (NULL == t) {
            NDW_LOGX("FATAL ERROR: Failed to get Topic from Name<%s\n", topic_names[i]);
            free(topics);
            return NULL;
        }
        else {
            topics[i] = t;
        }
    }

    return topics;
}

void*
publish(void *arg)
{
    NDW_LOGX("Publish Thread ID<%lu>\n", pthread_self());
    ndw_ThreadInit();

    publisher_args *args = (publisher_args *)arg;
    int print_frequency = (args->num_msgs < 100) ? 1 : (args->num_msgs / 10000);
    long sequence_number = 0;
    int data_size = -1;
    long publish_count = 0;
    int publish_code = -1;

    NDW_LOGX("Publisher Args: num_topics<%d> num_msgs<%d> msg_size<%d> delay_us<%d>\n",
                args->num_topics, args->num_msgs, args->msg_size, args->delay_us);

    ndw_Topic_T** topics = getTopicsFromNames(args->num_topics, args->topic_names);
    if (NULL == topics) {
        ndw_ThreadExit();
        args->ret_code = -1;
        return args;
    }

    sleep(2);

    for (int i = 0; i < args->num_msgs; i++)
    {
        for (int j = 0; j < args->num_topics; ++j)
        {
            ++sequence_number;
            ndw_Topic_T* topic = topics[j];

            char* data = ndw_CreateTestJsonStringofNSize(topic, "Sample Message",
                            sequence_number, args->msg_size, &data_size);
            data_size += 1;

            ndw_OutMsgCxt_T* cxt = ndw_CreateOutMsgCxt(
                                    topic, NDW_MSGHEADER_1, NDW_ENCODING_FORMAT_JSON,
                                    (unsigned char*) data, data_size);
            if (NULL == cxt) {
                NDW_LOGERR("Failed to create Output Message Context!\n");
                free(data);
                args->ret_code = -1;

                ndw_ThreadExit();
                free(topics);
                return NULL;
            }

            if (0 != (publish_code = ndw_PublishMsg())) {
               NDW_LOGERR("*** ERROR: ndw_PublishMsg() failed! "
                 "sequencer_number<%ld> with total publish count<%ld> "
                 "error code <%d>\n", sequence_number, publish_count, publish_code);

                free(data);
                args->ret_code = -2;

                ndw_ThreadExit();
                free(topics);
                return NULL;
            }

            ++publish_count;
            atomic_fetch_add(&total_published, 1);
            if (args->delay_us > 0) {
                usleep(args->delay_us);
            }

            if (0 == (i % print_frequency)) {
                NDW_LOG(">>> Published Message: sequence_number<%ld> data <%s> "
                          "data_size <%d>\n", sequence_number, data, data_size);
            }

            free(data);
        }
    }

    ndw_ThreadExit();

    free(topics);
    args->ret_code = 0;
    return args;
} // end method publish

int
ndw_HandleAsyncMessage(ndw_Topic_T* topic, void* opaque)
{
    long num_async_msgs = atomic_fetch_add(&total_async_received, 1);
    char* msg = (char*) topic->last_msg_received;
    int msg_size = topic->last_msg_received_size;
    uint64_t queued_msgs = 0;

    ndw_GetQueuedMsgCount(topic, &queued_msgs);

    if ((num_async_msgs < 25) || (0 == (num_async_msgs % 100000))) {
    NDW_LOGX("<<<ASYNC<<< Received Message: TopicId<%d, %s> num_async_msgs<%ld> "
            "total_received_msgs<%ld> queued_msgs<%" PRIu64 "> msg_size<%d> msg<%s>\n",
            topic->topic_unique_id, topic->topic_unique_name, num_async_msgs,
            topic->total_received_msgs, queued_msgs,
            msg_size, ((NULL == msg) ? "*** NULL! ***" : msg));
    }

    return 0;
} // end method ndw_HandleAsyncMessage

void*
subscribe(void *arg)
{
    NDW_LOGX("Subscribe Thread ID<%lu>\n", pthread_self());
    ndw_ThreadInit();

    subscribe_args *args = (subscribe_args *)arg;

    NDW_LOGX("Subscriber Args: num_topics<%d> expected_messages<%d>, timeout_sec<%d>\n",
               args->num_topics, args->expected_messages, args->timeout_sec);

    ndw_Topic_T** topics = getTopicsFromNames(args->num_topics, args->topic_names);
    if (NULL == topics) {
        args->ret_code = -1;
        ndw_ThreadExit();
        return args;
    }

    for (int i = 0; i < args->num_topics; i++) {
        ndw_Topic_T* topic = topics[i];
        if (0 != ndw_SubscribeAsync(topic)) {
            NDW_LOGERR("FAILED to Subscribe to Topic <%s>\n", topic->debug_desc);
            args->ret_code = -1;
            ndw_ThreadExit();
            free(topics);
            return args;
        }
    }
                
    NDW_LOGX("Subscriber thread waiting for start signal...\n");

    sleep(2);

    time_t idle_start_time = 0;

    LONG_T q_sleep_time_us = 100; // Microseconds
    LONG_T num_timed_outs = 0;

    while (1)
    {
        int at_least_one_received = 0;
        time_t now = time(NULL);

        for (int i = 0; i < args->num_topics; ++i)
        {
            ndw_Topic_T* topic = topics[i];
            int num_items = ndw_PollAsyncQueue(topic, q_sleep_time_us);

            if (1 == num_items) {
                at_least_one_received = 1;
                atomic_fetch_add(&total_received, 1);
                char* msg = (char*) topic->last_msg_received;
                int msg_size = topic->last_msg_received_size;
                NDW_LOGX("<<< Received Async Queued Message: msg<%s> msg_size<%d> ON %s",
                            msg, msg_size, topic->debug_desc);
                ndw_CommitAsyncQueuedMessge(topic);
            }
            else if (0 == num_items) {
                // No elements in queue.
                ++num_timed_outs;
            }
            else {
                NDW_LOGERR("Subscriber encountered error polling for Async Messages on %s\n",
                            topic->debug_desc);
                args->ret_code = -1;
                ndw_ThreadExit();
                free(topics);
                return args;
            }
        } // end for each Topic

        if (at_least_one_received) {
            idle_start_time = 0;
        } else
        {
            if (idle_start_time == 0) {
                idle_start_time = now;
            } else if (difftime(now, idle_start_time) > args->timeout_sec) {
                NDW_LOGX("Subscriber timeout: no message received on any topic in %d seconds.\n", args->timeout_sec);
                break;
            }
        }

#if 0
        // If we really want to wait more!
        if (consumer_delay_us > 0) {
            usleep(consumer_delay_us);
        }
#endif
    } // end while

    args->ret_code = 0;

    ndw_ThreadExit();

    free(topics);
    return NULL;
} // end method subscribe

int
InitMessagingSystem()
{
    if (0 != ndw_Init()) {
        safe_printf(stderr, "*** FATAL ERROR: ndw_Init() Failed!\n");
        return -1;
    }

    if (0 != ndw_ConnectToDomains(domains)) {
        NDW_LOGERR("*** FATAL ERROR: ndw_ConnectToDomains Failed!\n");
        return -1;
    }

    return 0;
} // end method InitMessagingSystem

void
shutdownMessagingSystem()
{
    ndw_Shutdown();
} // end method shutdownMessagingSystem

int
main(int argc, char *argv[])
{
    if (argc >= 2) {
        num_messages = atoi(argv[1]);
        if (num_messages <= 0) {
            safe_printf(stderr, "ERROR: Invalid number of messages. Must be > 0.\n");
            return EXIT_FAILURE;
        }
    }

    if (argc >= 3) {
        message_size = atoi(argv[2]);
        if (message_size <= 0) {
            safe_printf(stderr, "ERROR: Invalid message size. Must be > 0.\n");
            return EXIT_FAILURE;
        }
    }

    safe_printf(stdout, "TARGET: Publish %d message(s) of %d bytes each.\n",
                num_messages, message_size);

    if (InitMessagingSystem() < 0) {
        safe_printf(stderr, "*** FATAL ERROR: Failed to initialize messaging system.\n");
        return EXIT_FAILURE;
    }

    pub_args = (publisher_args){
        .num_topics = num_publisher_topics,
        .topic_names = publisher_topics,
        .num_msgs = num_messages,
        .msg_size = message_size,
        .delay_us = publisher_delay_us,
        .ret_code = -1
    };

    sub_args = (subscribe_args){
        .num_topics = num_subscriber_topics,
        .topic_names = subscriber_topics,
        .expected_messages = num_messages,
        .timeout_sec = subscriber_timeout_sec,
        .ret_code = -1
    };

    NDW_LOGX("Main Thread ID<%lu>\n", pthread_self());

    pthread_t pub_thread, sub_thread;
    pthread_create(&pub_thread, NULL, (thread_func_t)publish, &pub_args);
    pthread_create(&sub_thread, NULL, (thread_func_t)subscribe, &sub_args);

    sleep(1);
    print_timestamp("Main start time:");

    sleep(2);

    pthread_join(pub_thread, NULL);
    pthread_join(sub_thread, NULL);

    print_timestamp("Main end time:");

    NDW_LOG("TOTAL Published Messages <%ld> TOTAL Received Messages <%ld>\n",
                atomic_load(&total_published), atomic_load(&total_received));

    shutdownMessagingSystem();

    return EXIT_SUCCESS;
} // end method main

