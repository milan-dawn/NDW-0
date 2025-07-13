
#ifndef _TESTHARNESS_H
#define _TESTHARNESS_H

#include "NDW_Essentials.h"
#include <cJSON.h>

extern INT_T test_test_initialize(int argc, char** argv);
extern INT_T test_initialize(int argc, char** argv);

typedef INT_T (*publish_function)(ndw_Topic_T* topic);
typedef INT_T (*subscribe_function)(ndw_Topic_T* topic);
typedef INT_T (*poll_function)(ndw_Topic_T* topic, LONG_T timeout_us);
typedef INT_T (*poll_commit_function)(ndw_Topic_T* topic);

typedef struct publish_function_ptr_data
{
    publish_function function;
    const char* function_name;
} publish_function_ptr_data_T;

typedef struct subscribe_function_ptr_data
{
    subscribe_function function;
    const char* function_name;
} subscribe_function_ptr_data_T;

typedef struct poll_function_ptr_data
{
    poll_function function;
    const char* function_name;
} poll_function_ptr_data_T;

typedef struct poll_commit_function_ptr_data
{
    poll_commit_function function;
    const char* function_name;
} poll_commit_function_ptr_data_T;


typedef enum {
    NATS,
    JS
} PublishType;

typedef enum {
    Unknown,
    NATSAsync,
    NATSPoll,
    JSPush,
    JSPull,
    QAsync
} SubscribeType;

typedef struct AppTopic {
    ndw_Topic_T* topic;
    int topic_number;
    char LogicalUniqueName[256];
    char Domain[256];
    char Connection[256];
    char TopicName[256];
    bool Enabled;
    bool Disabled;
    bool Publish_Enabled;
    bool Subscribe_Enabled;
    bool Publish;
    bool Subscribe;
    int poll_timeout_us;

    PublishType PublishType;
    SubscribeType SubscribeType;

    long msgs_published_count;
    long msgs_received_count;

    long msgs_commit_count;
    long msgs_ack_count;
    long msgs_failed_ack_count;

    publish_function_ptr_data_T function_publish_data;

    subscribe_function_ptr_data_T function_subscribe_data;

    bool is_poll_activity;
    poll_function_ptr_data_T function_poll_data;
    poll_commit_function_ptr_data_T function_commit_data;
} AppTopic_T;

extern int num_topics;

typedef INT_T (*publish_msg_function)(ndw_Topic_T*);

// Thread args
typedef struct {
    pthread_t thread_id;
    INT_T num_msgs;
    INT_T msg_size;
    useconds_t delay_us;
    INT_T ret_code;
} publisher_args;


typedef struct {
    pthread_t thread_id;
    INT_T timeout_sec;
    INT_T ret_code;
} poll_subscriber_args;


extern bool is_async_message_handling_enabled;

extern publisher_args* pub_args;
extern poll_subscriber_args* poll_sub_args;


extern atomic_long total_published; // Set to zero.
extern atomic_long total_received; // Set to zero.
extern atomic_long total_async_received; // Set to zero.
extern atomic_long total_poll_received; // Set to zero.

extern INT_T num_messages; // Set to 1.
extern INT_T message_size; // Set to 1024.
extern INT_T poll_timeout_sec; // Set to 6.
extern useconds_t publisher_delay_us; // Set to 10 us.
extern useconds_t poll_delay_us; // Set to 10 us.

#endif /* _TESTHARNESS_H */

