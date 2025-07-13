
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
const char *domain_topics[] = {"DomainA", NULL};

const char *publisher_topics[] = {
    "DomainA^NATSConn1^ACME.Orders",
};

const char *subscriber_topics[] = {
    "DomainA^NATSConn1^ACME.Orders"
};

// Thread args
typedef struct {
    int num_messages;
    int delay_us;
    const char **topics;
    int num_topics;
} publisher_args;

typedef struct {
    int timeout_sec;
    int expected_messages;
    const char **topics;
    int num_topics;
} subscribe_args;

publisher_args pub_args;
subscribe_args sub_args;

// Thread function pointer type
typedef void *(*thread_func_t)(void *);

atomic_long total_published = 0;
atomic_long total_received = 0;
int consumer_delay_us = 100000;
int num_messages = 1;
int message_size = 1024;
int subscriber_timeout_sec = 6;
int publisher_delay_us = 1000;
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

    safe_printf(stdout, "[%s] HH:MM:SS:ms:us:ns %02d:%02d:%02d:%03ld:%03ld:%03ld\n",
                label, tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec, ms, us, ns);
}

int receive_message() {
    if (simulated_delay_ms > 0) usleep(simulated_delay_ms * 1000);
    int r = rand() % 10;
    if (r < 6) return 0;
    else if (r == 9) return -1;
    else return 1;
}

void *publish(void *arg) {
    safe_printf(stdout, "Publisher thread waiting for start signal...\n");
    pthread_mutex_lock(&start_mutex);
    while (!start_flag)
        pthread_cond_wait(&start_cond, &start_mutex);
    pthread_mutex_unlock(&start_mutex);
    safe_printf(stdout, "Publisher thread received start signal.\n");

    publisher_args *args = (publisher_args *)arg;
    for (int i = 0; i < args->num_messages; ++i) {
        const char *topic = args->topics[i % args->num_topics];
        safe_printf(stdout, "Publishing message %d to topic %s of size %d\n", i + 1, topic, message_size);
        atomic_fetch_add(&total_published, 1);
        if (args->delay_us > 0) usleep(args->delay_us);
    }

    return NULL;
}

void *subscribe(void *arg)
{
    safe_printf(stdout, "Subscriber thread waiting for start signal...\n");
    pthread_mutex_lock(&start_mutex);
    while (!start_flag)
        pthread_cond_wait(&start_cond, &start_mutex);
    pthread_mutex_unlock(&start_mutex);
    safe_printf(stdout, "Subscriber thread received start signal.\n");

    subscribe_args *args = (subscribe_args *)arg;
    time_t idle_start_time = 0;

    while (1) {
        int at_least_one_received = 0;
        time_t now = time(NULL);

        for (int t = 0; t < args->num_topics; ++t) {
            const char *topic = args->topics[t];
            int result = receive_message();

            if (result == 1) {
                safe_printf(stdout, "Subscribing a message from topic %s of size %d\n", topic, message_size);
                atomic_fetch_add(&total_received, 1);
                at_least_one_received = 1;
            } else if (result < 0) {
                safe_printf(stdout, "Subscriber encountered an error on topic %s. Exiting.\n", topic);
                return NULL;
            }
        }

        if (at_least_one_received) {
            idle_start_time = 0;
        } else {
            if (idle_start_time == 0)
                idle_start_time = now;
            else if (difftime(now, idle_start_time) > args->timeout_sec) {
                safe_printf(stdout, "Subscriber timeout: no message received on any topic in %d seconds.\n", args->timeout_sec);
                break;
            }
        }

        if (consumer_delay_us > 0)
            usleep(consumer_delay_us);
    }
    return NULL;
}

int
InitMessagingSystem()
{
    if (simulated_delay_ms > 0) usleep(simulated_delay_ms * 1000);
    return 1;
}

void
shutdownMessagingSystem()
{
    if (simulated_delay_ms > 0) usleep(simulated_delay_ms * 1000);
}

int main(int argc, char *argv[])
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

    if (!InitMessagingSystem()) {
        safe_printf(stderr, "*** FATAL ERROR: Failed to initialize messaging system.\n");
        return EXIT_FAILURE;
    }

    pub_args = (publisher_args){
        .num_messages = num_messages,
        .delay_us = publisher_delay_us,
        .topics = publisher_topics,
        .num_topics = 3
    };

    sub_args = (subscribe_args){
        .timeout_sec = subscriber_timeout_sec,
        .expected_messages = num_messages,
        .topics = subscriber_topics,
        .num_topics = 2
    };

    pthread_t pub_thread, sub_thread;
    pthread_create(&pub_thread, NULL, (thread_func_t)publish, &pub_args);
    pthread_create(&sub_thread, NULL, (thread_func_t)subscribe, &sub_args);

    sleep(1);
    print_timestamp("Main start time:");

    pthread_mutex_lock(&start_mutex);
    start_flag = 1;
    pthread_cond_broadcast(&start_cond);
    pthread_mutex_unlock(&start_mutex);

    pthread_join(pub_thread, NULL);
    pthread_join(sub_thread, NULL);

    print_timestamp("Main end time:");

    safe_printf(stdout, "TOTAL Published Messages <%ld> TOTAL Received Messages <%ld>\n",
                atomic_load(&total_published), atomic_load(&total_received));

    shutdownMessagingSystem();

    return EXIT_SUCCESS;
}

