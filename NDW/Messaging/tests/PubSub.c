
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "NDW_Essentials.h"
#include "TestHarness.h"

#if 0
NDW_TestArgs_T* args;

#define NUM_TOPICS 3
#define NUM_MSGS 2
static long wait_time_millis = 2 * 1000;
static long print_frequency = 1;

const char *domainNames[] = { "DomainA", NULL, };

char* topic_names[NUM_TOPICS + 1] = {
    "DomainA^NATSConn1^ACME.Orders",
    "DomainA^NATSConn1^ACME.Invoices",
    "DomainA^NATSConn2^ACME.Shipments",
    NULL,
};

ndw_Topic_T* topic_array[NUM_TOPICS + 1];

static ndw_Counter_T counter_published;
static ndw_Counter_T counter_received;


void ndw_HandleBadMessage(ndw_BadMessage_T* bad_msg) {
    ndw_print_BadMessage(bad_msg);
}

int ndw_HandleAsyncMessage(ndw_Topic_T* topic, void* opaque)
{
    char* msg = (char*) topic->last_msg_received;
    int msg_size = topic->last_msg_received_size;
    int total_received_msgs = topic->total_received_msgs;
    uint64_t queued_msgs = 0;
    ndw_GetQueuedMsgCount(topic, &queued_msgs);
    if (0 == (total_received_msgs % print_frequency)) {
        NDW_LOGX("<<< Received Message: TopicId<%d, %s> total_received_msgs<%ld> queued_msgs<%" PRIu64 "> msg_size<%d> msg<%s>\n",
                topic->topic_unique_id, topic->topic_unique_name, topic->total_received_msgs, queued_msgs,
                msg_size, ((NULL == msg) ? "*** NULL! ***" : msg));
    }
    ndw_CounterUpdate(&counter_received, 1);
    return 0;
} // end method ndw_HandleAsyncMessage


int test_subscriptions()
{
    return ndw_SubscribeAsyncToTopicNames(topic_names);
} // end method test_subscribe


int test_publish()
{
    ndw_Topic_T* topic;
    long sequence_number;
    int data_size;
    int ret_code;
    long publish_count = 0;
    for (int i = 0; i < args->max_msgs; i++) {
            int j = 0;
            for (topic = topic_array[j]; NULL != topic; topic = topic_array[++j]) {
                if (topic->disabled)
                    continue;

                sequence_number = ++topic->sequence_number;
                char* data = (args->bytes_size > 0) ?
                    ndw_CreateTestJsonStringofNSize(topic, "Sample Message: ", sequence_number, args->bytes_size, &data_size) :
                    ndw_CreateTestJsonString(topic, "Sample Message", sequence_number, &data_size);

                data_size += 1;
                ndw_OutMsgCxt_T* cxt = ndw_CreateOutMsgCxt(topic, NDW_MSGHEADER_1, NDW_ENCODING_FORMAT_JSON,
                                        (unsigned char*) data, data_size);
                if (NULL == cxt) {
                    NDW_LOGERR("Failed to create Output Message Context!\n");
                    free(data);
                    return -1;
                }

                //cxt->loopback_test = true;

                if (0 == (i % print_frequency)) {
                    NDW_LOG(">>> Published Message: data <%s>\ndata_size <%d>\n", data, data_size);
                }

                if (0 != (ret_code = ndw_PublishMsg())) {
                   NDW_LOGERR("ndw_PublishMsg() failed! sequencer_number<%ld> with total publish count<%ld>. "
                                "error code <%d>\n", sequence_number, publish_count, ret_code); 
                    free(data);
                    return -2;
                }
                free(data);
                publish_count = ndw_CounterUpdate(&counter_published, 1);
            }
    }

    return 0;

} // end method test_publish

int main(int argc, char** argv)
{
    args = ndw_ParseProgramOptions(argc, argv);

    if (0 != ndw_Init())
        exit(-1);

    ndw_PrintCurrentTime();
    NDW_LOG("\n\n");


    long total_messages = args->max_msgs * NUM_TOPICS;
    print_frequency = (args->max_msgs < 100) ? 1 : (args->max_msgs / 10);
    NDW_LOGX("Maximum Messages Per Topic<%d> Total Target Message<%ld>\n", args->max_msgs, total_messages);

    ndw_ConnectToDomains(domainNames);

    for (int i = 0; i < NUM_TOPICS; i++)
        topic_array[i] = ndw_GetTopicFromFullPath(topic_names[i]);
        

    if (test_subscriptions() < 0) {
        ndw_Shutdown();
        exit(-2);
    }

    
    NDW_LOG("*** Start Publishing ***\n");
    long start_time_nanos = ndw_GetCurrentNanoSeconds();
    ndw_PrintCurrentTime();
    int ret_code = test_publish();
    if (0 != ret_code) {
        NDW_LOGERR("test_publish() failed with error code<%d>\n", ret_code);
        for (int i = 0; i < NUM_TOPICS; i++)
            ndw_Unsubscribe(topic_array[i]);
    }
    else {
        long prev_msg_count = 0;
        long next_msg_count = 0;
    
        while (true) {
            prev_msg_count = ndw_CounterCurrentValue(&counter_received);
            if (ndw_CounterCheck(&counter_received, total_messages, wait_time_millis)) {
                break;
            }
            else {
                next_msg_count = ndw_CounterCurrentValue(&counter_received);
                if (next_msg_count >= total_messages) {
                    break;
                } else if (prev_msg_count == next_msg_count) {
                    NDW_LOGERR("Bailing out as no new messages in <%ld> milliseconds "
                    "prev_msg_count<%ld> next_msg_count<%ld>\n",
                    wait_time_millis, prev_msg_count, next_msg_count);
                    break;
                }
                else {
                    NDW_LOGERR("NOTE: Continuing after waiting for <%ld> milliseconds "
                    "prev_msg_count<%ld> next_msg_count<%ld>\n",
                    wait_time_millis, prev_msg_count, next_msg_count);
                }
            }
        }
    }

    ndw_PrintTimeFromNanos(ndw_GetCurrentNanoSeconds() - start_time_nanos);
    NDW_LOGX("NOTE: TotalPublishedMsgs<%ld> Expected<%ld>\n",
                ndw_CounterCurrentValue(&counter_published), total_messages);
    NDW_LOGX("NOTE: TotalReceivedMsgs<%ld> Expected<%ld>\n",
                ndw_CounterCurrentValue(&counter_received), total_messages);

    ndw_PrintStatsForDomains(domainNames);
    ndw_Shutdown();

    //pthread_exit(NULL);
    return 0;
} /* end method main */
#else
int main(int argc, char** argv)
{
    int ret_code = test_initialize(argc, argv);

    return ret_code;
}

#endif

