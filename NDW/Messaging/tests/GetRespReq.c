
#include "TestHarness.h"

#define APP_CONFIG_TOPIC_FILE_ENV "NDW_APP_TOPIC_FILE"

int num_topics = 0;
int num_publications = 0;
int num_subscriptions = 0;
int num_poll_subscriptions = 0;

static resp_request_args r_args;

resp_request_args* res_req_args = &r_args;

atomic_long total_published = 0;
atomic_long total_poll_received = 0;
atomic_long total_async_received = 0;
INT_T num_messages = 1;
INT_T message_size = 1024;

static AppTopic_T* Topics = NULL;

pthread_mutex_t start_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t start_cond = PTHREAD_COND_INITIALIZER;
int start_flag = 0;

char* start_timestamp = NULL;
char* end_timestamp = NULL;

static LONG_T print_frequency_modulo = 1000;

// Thread function pointer type
typedef void *(*thread_func_t)(void *);

static void
wait_for_start_signal()
{
    pthread_mutex_lock(&start_mutex);
    while (!start_flag)
        pthread_cond_wait(&start_cond, &start_mutex);
    pthread_mutex_unlock(&start_mutex);
} // end method wait_for_start_signal

static void
send_start_signal()
{
    pthread_mutex_lock(&start_mutex);
    start_flag = 1;
    pthread_cond_broadcast(&start_cond);
    pthread_mutex_unlock(&start_mutex);
} // end method send_start_signal

static INT_T default_publish_msg_function(ndw_Topic_T* topic) {
    (void) topic;

    return ndw_PublishMsg();
}

/*
 * Subscription functions for various Subscription Types.
 */
static INT_T NATS_Subscribe_Async(ndw_Topic_T* topic) { return ndw_SubscribeAsync(topic); }
static INT_T NATS_Subscribe_Sync(ndw_Topic_T* topic) { return ndw_SubscribeSynchronous(topic); }
static INT_T JS_subscribe_Async(ndw_Topic_T* topic) { return ndw_SubscribeAsync(topic); }
static INT_T JS_Subscribe_Sync(ndw_Topic_T* topic) { return ndw_SubscribeSynchronous(topic); }
static INT_T AsyncQ_Subscribe(ndw_Topic_T* topic) { return ndw_SubscribeAsync(topic); }
static INT_T NATS_GetRespForReq(ndw_Topic_T* topic, LONG_T timeout) { return ndw_GetResponseForRequestMsg(topic, timeout); }

static INT_T NATS_Poll(ndw_Topic_T* topic, LONG_T timeout_us) {
    LONG_T dropped_messages;
    (void) dropped_messages;
    LONG_T timeout_ms = (timeout_us < 1000) ? 1 : (timeout_us / 1000);
    return ndw_SynchronousPollForMsg(topic, timeout_ms, &dropped_messages);
}

static INT_T JS_Poll(ndw_Topic_T* topic, LONG_T timeout_us) {
    LONG_T dropped_messages;
    (void) dropped_messages;
    LONG_T timeout_ms = (timeout_us < 1000) ? 1 : (timeout_us / 1000);
    return ndw_SynchronousPollForMsg(topic, timeout_ms, &dropped_messages);
}

static void set_commit_counters(ndw_Topic_T* topic)
{
    if (NULL != topic) {
        AppTopic_T* a_topic = (AppTopic_T*) topic->app_opaque;
        if (NULL != a_topic) {
            a_topic->msgs_commit_count = topic->msgs_commit_count;
            a_topic->msgs_ack_count = topic->msgs_ack_count;
            a_topic->msgs_failed_ack_count = topic->msgs_failed_ack_count;
        }
    }
}

static INT_T AsyncQ_Poll(ndw_Topic_T* topic, LONG_T timeout_us) { return ndw_PollAsyncQueue(topic, timeout_us); } 

static INT_T NATS_Commit(ndw_Topic_T* topic)
{
    INT_T ret_code = ndw_CommitLastMsg(topic);
    set_commit_counters(topic);
    return ret_code;
}

static INT_T JS_Commit(ndw_Topic_T* topic)
{
    INT_T ret_code = ndw_CommitLastMsg(topic);
    set_commit_counters(topic);
    return ret_code;
}

static INT_T AsyncQ_Commit(ndw_Topic_T* topic)
{
    INT_T ret_code = ndw_CommitAsyncQueuedMessge(topic);
    set_commit_counters(topic);
    return ret_code;
}

static publish_function_ptr_data_T publish_function_data = { default_publish_msg_function, "default_publish_msg_function" };

static subscribe_function_ptr_data_T nats_subscribe_Async_function_data = { NATS_Subscribe_Async, "NATS_Subscribe_Async" };
static subscribe_function_ptr_data_T nats_subscribe_Sync_function_data = { NATS_Subscribe_Sync, "NATS_Subscribe_Sync" };
static subscribe_function_ptr_data_T js_subscribe_Async_function_data = { JS_subscribe_Async, "JS_subscribe_Async," };
static subscribe_function_ptr_data_T js_subscribe_Sync_function_data = { JS_Subscribe_Sync, "JS_Subscribe_Sync" };
static subscribe_function_ptr_data_T asyncq_subscribe_function_data = { AsyncQ_Subscribe, "AsyncQ_Subscribe" };

static poll_function_ptr_data_T nats_poll_function_data = { NATS_Poll, "NATS_Poll" };
static poll_function_ptr_data_T js_poll_function_data = { JS_Poll, "JS_Poll" };
static poll_function_ptr_data_T asyncq_poll_function_data = { AsyncQ_Poll, "AsyncQ_Poll" };

static poll_commit_function_ptr_data_T nats_commit_function_data = { NATS_Commit, "NATS_Commit" };
static poll_commit_function_ptr_data_T js_commit_function_data = { JS_Commit, "JS_Commit" };
static poll_commit_function_ptr_data_T asyncq_commit_function_data = { AsyncQ_Commit, "AsyncQ_Commit" };

static resreq_function_ptr_data_T nats_getresreq_function_data =
    { NATS_Subscribe_Async, NATS_GetRespForReq, "NATS_GetRespForReq" };

void
print_app_topic_config(AppTopic_T* topic)
{
    printf("------\n");
    printf("Topic Number: %d\n", topic->topic_number);
    printf("LogicalUniqueName: %s\n", topic->LogicalUniqueName);
    printf("Domain: %s\n", topic->Domain);
    printf("Connection: %s\n", topic->Connection);
    printf("TopicName: %s\n", topic->TopicName);
    printf("Enabled: %s\n", topic->Enabled ? "true" : "false");
    printf("Disabled: %s\n", topic->Disabled ? "true" : "false");
    printf("Publish_Enabled: %s\n", topic->Publish_Enabled ? "true" : "false");
    printf("Subscribe_Enabled: %s\n", topic->Subscribe_Enabled ? "true" : "false");
    printf("publish_function_data: <%s>\n",
                (NULL == topic->function_publish_data.function) ? "NULL" : topic->function_publish_data.function_name);
    printf("subscribe_function_data: <%s>\n",
                (NULL == topic->function_subscribe_data.function) ? "NULL" : topic->function_subscribe_data.function_name);
    printf("poll_function_data: <%s>\n",
                (NULL == topic->function_poll_data.function) ? "NULL" : topic->function_poll_data.function_name);
    printf("commit_function_data: <%s>\n",
                (NULL == topic->function_commit_data.function) ? "NULL" : topic->function_commit_data.function_name);

    switch (topic->PublishType) {
        case NATS:
            printf("PublishType: NATS\n");
            break;
        case JS:
            printf("PublishType: JS\n");
            break;
    }

    switch (topic->SubscribeType) {
        case NATSAsync:
            printf("SubscribeType: NATSAsync\n");
            break;
        case NATSPoll:
            printf("SubscribeType: NATSPoll\n");
            break;
        case JSPush:
            printf("SubscribeType: JSPush\n");
            break;
        case JSPull:
            printf("SubscribeType: JSPull\n");
            break;
        case QAsync:
            printf("SubscribeType: QAsync\n");
            break;
        case NATSGetResp:
            printf("SubscribeType: NATS_GetRespForReq\n");
            break;
        case Unknown:
            printf("SubscribeType: Unknown\n");
        default:
            printf("SubscribeType: ??\n");
            break;
    }
    printf("is_poll_activity: %s\n", topic->is_poll_activity ? "true" : "false");

} // end method print_app_topic_config

void
print_app_topic_configs()
{
    if (NULL == Topics)
        return;

    printf("\n");
    printf("================= BEGIN: Print App Topic Configurations ==========================\n");
    printf("\n");
    for (int i = 0; i < num_topics; i++) {
        printf("Topic %d:\n", i + 1);
        print_app_topic_config(&Topics[i]);
        printf("\n");
    }
    printf("================= END: Print App Topic Configurations ==========================\n");
    printf("\n");
} // end method print_app_topic_configs

AppTopic_T*
load_app_topic_configs(const char* json_file)
{
    // Open and read the JSON file
    FILE* file = fopen(json_file, "r");
    if (!file) {
        fprintf(stderr, "Error opening file '%s'\n", json_file);
        return NULL;
    }

    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    rewind(file);

    char* json_str = malloc(file_size + 1);
    fread(json_str, file_size, 1, file);
    json_str[file_size] = '\0';

    fclose(file);

    // Parse the JSON string
    cJSON* json = cJSON_Parse(json_str);
    free(json_str);

    if (!json) {
        fprintf(stderr, "*** ERROR parsing JSON\n");
        return NULL;
    }

    // Get the Topics array
    cJSON* topics_json = cJSON_GetObjectItem(cJSON_GetObjectItem(json, "NDWAppConfig"), "Topics");
    if (!topics_json) {
        fprintf(stderr, "Error getting Topics array\n");
        cJSON_Delete(json);
        return NULL;
    }

    // Allocate memory for the topics array
    int topic_count = cJSON_GetArraySize(topics_json);
    AppTopic_T* topics = calloc(topic_count, sizeof(AppTopic_T));

    // Load each topic
    for (int i = 0; i < topic_count; i++)
    {
        cJSON* topic_json = cJSON_GetArrayItem(topics_json, i);

        // Load topic attributes
        cJSON* logical_unique_name_json = cJSON_GetObjectItem(topic_json, "LogicalUniqueName");
        cJSON* domain_json = cJSON_GetObjectItem(topic_json, "Domain");
        cJSON* connection_json = cJSON_GetObjectItem(topic_json, "Connection");
        cJSON* topic_name_json = cJSON_GetObjectItem(topic_json, "TopicName");
        cJSON* enabled_json = cJSON_GetObjectItem(topic_json, "Enabled");
        cJSON* publish_json = cJSON_GetObjectItem(topic_json, "Publish");
        cJSON* subscribe_json = cJSON_GetObjectItem(topic_json, "Subscribe");
        cJSON* publish_type_json = cJSON_GetObjectItem(topic_json, "PublishType");
        cJSON* subscribe_type_json = cJSON_GetObjectItem(topic_json, "SubscribeType");
        cJSON* poll_timeout_us_json = cJSON_GetObjectItem(topic_json, "PollTimeoutus");

        AppTopic_T* t = &topics[i];
        t->topic_number = i;

        // Copy topic attributes
        strcpy(t->LogicalUniqueName, logical_unique_name_json->valuestring);
        strcpy(t->Domain, domain_json->valuestring);
        strcpy(t->Connection, connection_json->valuestring);
        strcpy(t->TopicName, topic_name_json->valuestring);
        t->Enabled = enabled_json->valueint;
        t->Disabled = (! t->Enabled);
        t->Publish_Enabled = publish_json->valueint;
        t->Subscribe_Enabled = subscribe_json->valueint;

        if (NULL != poll_timeout_us_json) {
            int timeout_us = poll_timeout_us_json->valueint;
            t->poll_timeout_us = (timeout_us < 0) ? 0 : timeout_us;
        }

        if (t->Publish_Enabled) {
            if (strcasecmp(publish_type_json->valuestring, "NATS") == 0) {
                t->PublishType = NATS;
                t->function_publish_data = publish_function_data;
            } else if (strcasecmp(publish_type_json->valuestring, "JS") == 0) {
                t->PublishType = JS;
                t->function_publish_data = publish_function_data;
            }
            else {
                fprintf(stderr, "Invalid publish_type <%s>\n", publish_type_json->valuestring);
                return NULL;
            }

            ++num_publications;
        } // end if Topic Publication is enabled.

        if (t->Subscribe_Enabled)
        {
            if (strcasecmp(subscribe_type_json->valuestring, "NATSAsync") == 0)
            {
                t->SubscribeType = NATSAsync;
                t->function_subscribe_data = nats_subscribe_Async_function_data;
                t->function_commit_data = nats_commit_function_data;
                t->is_poll_activity = false;
            }
            else if (strcasecmp(subscribe_type_json->valuestring, "NATSPoll") == 0)
            {
                ++num_poll_subscriptions;
                t->SubscribeType = NATSPoll;

                t->function_subscribe_data = nats_subscribe_Sync_function_data;

                t->is_poll_activity = true;
                t->function_poll_data = nats_poll_function_data;
                t->function_commit_data = nats_commit_function_data;
            }
            else if (strcasecmp(subscribe_type_json->valuestring, "JSPush") == 0)
            {
                t->SubscribeType = JSPush;

                t->function_subscribe_data = js_subscribe_Async_function_data;

                t->is_poll_activity = false;
                t->function_commit_data = js_commit_function_data;
            }
            else if (strcasecmp(subscribe_type_json->valuestring, "JSPull") == 0)
            {
                ++num_poll_subscriptions;
                t->SubscribeType = JSPull;

                t->function_subscribe_data = js_subscribe_Sync_function_data;

                t->is_poll_activity = true;
                t->function_poll_data = js_poll_function_data;
                t->function_commit_data = js_commit_function_data;
            }
            else if (strcasecmp(subscribe_type_json->valuestring, "QAsync") == 0)
            {
                ++num_poll_subscriptions;
                t->SubscribeType = QAsync;

                t->function_subscribe_data = asyncq_subscribe_function_data;

                t->is_poll_activity = true;
                t->function_poll_data = asyncq_poll_function_data;
                t->function_commit_data = asyncq_commit_function_data;
            } else if (strcasecmp(subscribe_type_json->valuestring, "NATSGetResp") == 0)
            {
                t->SubscribeType = NATSGetResp;
                t->function_subscribe_data = nats_subscribe_Async_function_data;
                t->is_poll_activity = false;
                t->function_resreq_data = nats_getresreq_function_data;
                t->function_commit_data = nats_commit_function_data;
            }
            else {
                fprintf(stderr, "Invalid subscribe_type<%s>\n", subscribe_type_json->valuestring);
                return NULL;
            }

            ++num_subscriptions;

        } // End if subscription is enabled.

    } // end cJSON parsing of each Topic.

    // Clean up
    cJSON_Delete(json);

    // Return the topics array and its size
    num_topics = topic_count;
    Topics = topics;
    return topics;
} // end method load_app_topic_configs

int
populate_topic_pointers()
{
    if (num_topics <= 0)
        return -1;

    for (int i = 0; i < num_topics; i++) {
        AppTopic_T* t = &Topics[i];
        ndw_Domain_T* domain = ndw_GetDomainByName(t->Domain);
        if (NULL == domain) {
            NDW_LOGERR("Failed to get NDW_Domain_T* from Domain Name <%s>\n", t->Domain);
            return -2;
        }

        ndw_Connection_T* connection = ndw_GetConnectionByNameFromDomain(domain, t->Connection);
        if (NULL == connection) {
            NDW_LOGERR("Failed to get NDW_Connection_T* from Connection Name<%s> for Domain<%s>\n",
                        t->Connection, t->Domain);
            return -3;
        }

        t->topic = ndw_GetTopicByNameFromConnection(connection, t->TopicName);
        if (NULL == t->topic) {
            NDW_LOGERR("Failed to get ndw_Topic_T* from Topic Name <%s> for Domain<%s> and Connection<%s>\n",
                        t->TopicName, t->Connection, t->Domain);
            return -4;
        }

        t->topic->app_opaque = t;
    }

    return 0;
} // end method populate_topic_pointers

char*
get_current_timestamp(const char* label)
{
    struct timespec ts;
    struct tm tm_info;
    clock_gettime(CLOCK_REALTIME, &ts);
    localtime_r(&ts.tv_sec, &tm_info);

    long ms = ts.tv_nsec / 1000000;
    long us = (ts.tv_nsec / 1000) % 1000;
    long ns = ts.tv_nsec % 1000;

    int buffer_size = snprintf(NULL, 0,
             "[%s] HH:MM:SS:ms:us:ns %02d:%02d:%02d:%03ld:%03ld:%03ld",
             label, tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec, ms, us, ns);

    if (buffer_size < 0) {
        // Handle error
        return NULL;
    }

    char* timestamp_buffer = malloc((buffer_size + 1024) * sizeof(char));
    if (timestamp_buffer == NULL) {
        // Handle memory allocation error
        return NULL;
    }

    snprintf(timestamp_buffer, buffer_size + 1,
             "[%s] HH:MM:SS:ms:us:ns %02d:%02d:%02d:%03ld:%03ld:%03ld",
             label, tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec, ms, us, ns);

    return timestamp_buffer;
} // end method get_current_timestamp

int
ndw_HandleAsyncMessage(ndw_Topic_T* topic, void* opaque)
{
    AppTopic_T* a_topic = topic->app_opaque;
    if (NULL == a_topic) {
        NDW_LOGERR("*** ERROR: topic->app_opaque is NULL for <%s>\n", topic->debug_desc);
        return -100;
    }

    a_topic->msgs_received_count += 1;

    long num_async_msgs = atomic_fetch_add(&total_async_received, 1);
    char* msg = (char*) topic->last_msg_received;
    int msg_size = topic->last_msg_received_size;
    uint64_t queued_msgs = 0;

    ndw_GetQueuedMsgCount(topic, &queued_msgs);

    if (ndw_Publish_ResponseForRequestMsg(topic) != 0) {
        NDW_LOGERR("*** ERROR: function_publish_respose is NULL for <%s>\n", topic->debug_desc);
        return -101;
    }

    if (0 == (topic->total_received_msgs % print_frequency_modulo)) {
        NDW_LOGX("<<<Received ASYNC Message: TopicId<%d, %s> num_async_msgs<%ld> "
            "topic->total_received_msgs<%ld> queued_msgs<%" PRIu64 "> msg_size<%d> msg<%s>\n",
            topic->topic_unique_id, topic->topic_unique_name, num_async_msgs,
            topic->total_received_msgs, queued_msgs,
            msg_size, ((NULL == msg) ? "*** NULL! ***" : msg));
    }

    if (NULL == a_topic->function_commit_data.function) {
        NDW_LOGERR("*** ERROR: function_commit is NULL for <%s>\n", topic->debug_desc);
        return -102;
    }

    INT_T commit_code = a_topic->function_commit_data.function(topic);
    if (0 != commit_code) {
        NDW_LOGERR("*** ERROR: function_commit failed with error code<%d> on <%s>\n",
                     commit_code, topic->debug_desc);
        return commit_code;
    }

    return 0;
} // end method ndw_HandleAsyncMessage

void*
thread_getresreq_wait(void *arg)
{
    printf("GetResponseForRequest Thread ID<%lu>\n", pthread_self());

    resp_request_args *args = (resp_request_args *) arg;
    if (args->timeout_msec <= 0) {
        args->timeout_msec = 200;
    }

    ndw_ThreadInit();

    args->thread_id = pthread_self();
    NDW_LOGX("GetResponseForRequest Thread ID<%lu>\n", pthread_self());

    args->ret_code = -1;

    wait_for_start_signal();


    AppTopic_T* a_topic = &Topics[0];
    ndw_Topic_T* topic = a_topic->topic;

    int num_items = a_topic->function_resreq_data.function(topic);

    if (num_items != 0 ) {
        NDW_LOGERR("Subscriber encountered error polling for Async Messages on %s\n",
                    topic->debug_desc);
        goto on_poll_exit;
    }

    int data_size;
    long sequence_number = ++topic->sequence_number;
    char* data = ndw_CreateTestJsonString(topic, "Sample Message", sequence_number, &data_size);

    data_size += 1;
    ndw_OutMsgCxt_T* cxt = ndw_CreateOutMsgCxt(topic, NDW_MSGHEADER_1, NDW_ENCODING_FORMAT_JSON,
                                    (unsigned char*) data, data_size);
    if (NULL == cxt) {
        NDW_LOGERR("Failed to create Output Message Context!\n");
        free(data);
        goto on_poll_exit;
    }

    INT_T status = a_topic->function_resreq_data.resreq_function(topic, (LONG_T)args->timeout_msec);

    if (status != 1) {
        NDW_LOGERR("GetResponseForRequestMsg test failed %d on %s\n", status, topic->debug_desc);
    } else {
        args->ret_code = 0;
    }

    free(data);

on_poll_exit:
    ndw_ThreadExit();
    return args;
} // end method thread_getresreq_sub -- subscribe async

static void
print_statistics()
{
    fprintf(stdout, "\n---------------- BEGIN: STATISTICS ---------------------\n");

    for (int i = 0; i < num_topics; i++)
    {
        AppTopic_T* a_topic = &Topics[i];
        if (a_topic->Disabled) {
            continue;
        }

        fprintf(stdout, "[%d] <%s> ==> msg_published_count<%ld> msg_received_count<%ld> commits<%ld> acks<%ld> failed_acks<%ld>\n",
                a_topic->topic_number, a_topic->TopicName,
                a_topic->msgs_published_count, a_topic->msgs_received_count,
                a_topic->msgs_commit_count, a_topic->msgs_ack_count, a_topic->msgs_failed_ack_count);
    }
    fprintf(stdout, "\n");

    if (NULL != start_timestamp)
        fprintf(stdout, "Start Time %s\n", start_timestamp);

    if (NULL != end_timestamp)
        fprintf(stdout, "End Time   %s\n", end_timestamp);

    fprintf(stdout, "\nTOTALS: Published<%ld> AsyncReceived<%ld> PollReceived<%ld>\n",
            atomic_load(&total_published), atomic_load(&total_async_received), atomic_load(&total_poll_received));

    fprintf(stdout, "\n---------------- END: STATISTICS ---------------------\n\n");

} // end method print_statistics

INT_T
test_InitiateConnections()
{
    for (int i = 0; i < num_topics; i++) {
        AppTopic_T* t = &Topics[i];
        NDW_LOGX("Trying to Connect to <%s, %s>\n", t->Domain, t->Connection);
        INT_T ret_code = ndw_Connect(t->Domain, t->Connection);
        if (0 != ret_code) {
            NDW_LOGERR("*** ERROR: Failed to Connect to <%s, %s>\n", t->Domain, t->Connection);
            return ret_code;
        }
    }

    return 0;
} // end method test_InitiateConnections

void
set_print_frequency(INT_T max_msgs_per_topic)
{
    if (max_msgs_per_topic <= 100)
        print_frequency_modulo = 1;
    else if (max_msgs_per_topic <= 1000)
        print_frequency_modulo = 100;
    else if (max_msgs_per_topic <= 10000)
        print_frequency_modulo = 1000;
    else if (max_msgs_per_topic <= 100000)
        print_frequency_modulo = 10000;
    else if (max_msgs_per_topic <= 1000000)
        print_frequency_modulo = 100000;
    else if (max_msgs_per_topic <= 3000000)
        print_frequency_modulo = 300000;
    else
        print_frequency_modulo = 1000000;

    printf("print_frequency_modulo<%ld>\n", print_frequency_modulo);
    
} // end method set_print_frequency

int
test_initialize(int argc, char** argv)
{
    printf("Main Thread ID<%lu>\n", pthread_self());

    NDW_TestArgs_T* program_options = ndw_ParseProgramOptions(argc, argv);
    if (NULL == program_options) {
        fprintf(stderr, "*** ERROR: test_initialize(): Failed to parse program options!\n");
        return -1;
    }

    if (program_options->max_msgs <= 0) {
        program_options->max_msgs = 1;
    }

    if (program_options->bytes_size <= 0) {
        program_options->bytes_size = 1024;
    }

    if (program_options->wait_time_seconds <= 0) {
        program_options->wait_time_seconds = 6;
    }

    if (program_options->resreq_time_msecs <= 0) {
        program_options->resreq_time_msecs = 200;
    }

    set_print_frequency(program_options->max_msgs);

    const char* app_json_config_file = getenv(APP_CONFIG_TOPIC_FILE_ENV);

    if (NDW_ISNULLCHARPTR(app_json_config_file)) {
        fprintf(stderr, "*** ERROR: test_initialize(): NULL app_json_config_file specified\n");
        return -2;
    }

    load_app_topic_configs(app_json_config_file);
    if (num_topics <= 0) {
        fprintf(stderr, "*** ERROR: test_initialize(): Invalid number of topics<%d> in App JSON Config File <%s>\n",
                        num_topics, app_json_config_file);
        return -3;
    }

    if ((num_publications <= 0) && (num_subscriptions <= 0)) {
        fprintf(stderr, "*** ERROR: test_initialize(): There are NO Publications or Subscriptions enabled in <%s>\n",
                        app_json_config_file);
        return -4;
    }

    print_app_topic_configs();

    if (0 != ndw_Init()) {
        fprintf(stderr, "*** ERROR: ndw_Init() failed!\n");
        return -5;
    }

    if (0 != populate_topic_pointers()) {
        NDW_LOGERR("*** ERROR: test_initialize(): Failed to find Topic Pointers!\n");
        ndw_Shutdown();
        return -6;

    }

    NDW_LOGX("==> test_initialize() completed. num_topics<%d> num_publications<%d> "
            "num_subscriptions<%d> num_poll_subscriptions<%d>\n",
            num_topics, num_publications, num_subscriptions, num_poll_subscriptions);


    if (0 != test_InitiateConnections()) {
        NDW_LOGERR("*** ERROR: Failed to initialze Connections!\n");
        goto shutdown;
    }


    resp_request_args* res_req_args = &r_args;
    res_req_args->timeout_msec = program_options->resreq_time_msecs;
    res_req_args->ret_code = -1;

    pthread_t grs_thread;

    // create, subscribe and wait
    pthread_create(&grs_thread, NULL, (thread_func_t) thread_getresreq_wait, res_req_args);

    start_timestamp = get_current_timestamp("Start Time => ");

    send_start_signal();

    pthread_join(grs_thread, NULL);

    end_timestamp = get_current_timestamp("Start Time => ");

    //int sleep_wait_time = 60 * 60;
    int sleep_wait_time = program_options->wait_time_seconds;
    long published = 0;
    while (1) {
        published = atomic_load(&total_published);
        long prev_received = atomic_load(&total_poll_received) + atomic_load(&total_async_received);

        NDW_LOGX("\n---> main(): Before Sleep<%d>: total_published<%ld> prev_received<%ld>\n",
                sleep_wait_time, published, prev_received);

        sleep(sleep_wait_time);

        published = atomic_load(&total_published);
        long received = atomic_load(&total_poll_received) + atomic_load(&total_async_received);
        NDW_LOGX("\n---> main(): After Sleep<%d> total_published<%ld> prev_received<%ld> received<%ld>\n",
                    sleep_wait_time, published, prev_received, received);
        if (prev_received == received) {
            break;
        }
        else {
            prev_received = received;
        }
    }
    NDW_LOGX("main(): At end: total received<%ld>\n", atomic_load(&total_poll_received) + atomic_load(&total_async_received));

shutdown:
    ndw_Shutdown();

    print_statistics();

    free(start_timestamp);
    free(end_timestamp);
    free(Topics);
    
    return 0;
} // end method test_initialize

int main(int argc, char** argv)
{
    test_initialize(argc, argv);
    return 0;
} /* end method main */
