
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "NDW_Essentials.h"
#include "TestHarness.h"

#if 0

#define NUM_TOPICS 3
#define NUM_MSGS 1
static long max_messages = (NUM_TOPICS * NUM_MSGS);
static long message_bytes_size = 0;
static long print_frequency = 1;

const char *domainNames[] = { "DomainA", NULL, };

char* topic_names[NUM_TOPICS + 1] = {
    "DomainA^NATSConnNews^NEWS_Sports_ALL",
    "DomainA^NATSConnNews^NEWS_Sports_Football",
    "DomainA^NATSConnNews^NEWS_Sports_College_Football",
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
    if (0 == (total_received_msgs % print_frequency)) {
        NDW_LOGX("<<< RECEIVED MSG<%s>: msg_size<%d> msg<%s> total_received_msgs<%ld> "
                "IPAddr<%s> DurableAcKSequence<%lu> DurableAckGlobalSequence<%lu> "
                " TotalDelivered<%lu> TotalPending<%lu> "
                " ON %s\n",
                topic->topic_unique_name, msg_size,
                ((NULL == msg) ? "*** NULL! ***" : msg),
                topic->total_received_msgs,
                topic->last_received_ip_address,
                topic->last_received_durable_ack_sequence,
                topic->last_received_durable_ack_global_sequence,
                topic->last_received_durable_total_delivered,
                topic->last_received_durable_total_pending,
                topic->debug_desc);
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
    pid_t pid = getpid();
    NDW_LOGX("PID<%jd>\n", (intmax_t) pid);
    char message_prefix[100];
    snprintf(message_prefix, sizeof(message_prefix), "[PID: %d]: SAMPLE MESSAGE: ", (int) pid);

    ndw_Topic_T* topic;
    long sequence_number;
    int data_size;
    int ret_code;
    long publish_count = 0;
    for (int i = 0; i < max_messages; i++) {
            int j = 0;
            for (topic = topic_array[j]; NULL != topic; topic = topic_array[++j]) {
                if (topic->disabled || (! topic->is_pub_enabled)) {
                    NDW_LOG("NOTE: Skipping Topic %s\n", topic->debug_desc); 
                    continue;
                }

                sequence_number = ++topic->sequence_number;
                char* data = (message_bytes_size > 0) ?
                    ndw_CreateTestJsonStringofNSize(topic, message_prefix, sequence_number, message_bytes_size, &data_size) :
                    ndw_CreateTestJsonString(topic, message_prefix, sequence_number, &data_size);

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
                    NDW_LOG(">>> PUBLISHED MESSAGE: data <%s>\ndata_size <%d>\n --> to %s\n", data, data_size, topic->debug_desc);
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
    if (0 != ndw_Init())
        exit(-1);

    ndw_PrintCurrentTime();
    NDW_LOG("\n\n");

    if (argc > 1) {
        int value = atoi(*(argv+1)); // Number of messages per Topic (Subject)
        if (value > 0) {
            max_messages = value;
        }

        if (argc > 2) {
            value = atoi(*(argv+2)); // Number of approximately bytes per message.
            if (value > 100)
                message_bytes_size = value;
        }
    }

    long total_messages = max_messages * NUM_TOPICS;
    print_frequency = (max_messages < 100) ? 1 : (max_messages / 10);
    NDW_LOGX("Maximum Messages Per Topic<%ld> Total Target Message<%ld>\n", max_messages, total_messages);

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
        NDW_LOGERR("FATAL ERROR: test_publish() failed with error code<%d>\n", ret_code);
        for (int i = 0; i < NUM_TOPICS; i++)
            ndw_Unsubscribe(topic_array[i]);
    }
    else {
        long wait_unit_seconds = 5;
        long max_wait_iterations = 3;
        long wait_iterations = 0;

        while (true) {
            long prev_msg_count = ndw_CounterCurrentValue(&counter_received);
            sleep(wait_unit_seconds);
            long next_msg_count = ndw_CounterCurrentValue(&counter_received);
            long msg_count_delta = next_msg_count - prev_msg_count;
            ndw_PrintTimeFromNanos(ndw_GetCurrentNanoSeconds());
            NDW_LOG("CurrentMsgsReceived<%ld> PreviousMsgReceived<%ld> delta<%ld>\n",
                    next_msg_count, prev_msg_count, msg_count_delta);

            wait_iterations = (msg_count_delta > 0) ? 0 : (wait_iterations + 1);
            if (wait_iterations == max_wait_iterations) {
                next_msg_count = ndw_CounterCurrentValue(&counter_received);
                NDW_LOGERR("Bailing out as no new messages after <%ld> seconds!\n",
                    (wait_iterations * max_wait_iterations));
                break;
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
    return 0;
} /* end method main */

#else
int main(int argc, char** argv)
{
    int ret_code = test_initialize(argc, argv);

    return ret_code;
}

#endif

