
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "NDW_Essentials.h"
#include "TestHarness.h"

#if 0

const char *domainNames[] = { "DomainA", NULL, };

#define NUM_TOPICS  3

char* topic_names[NUM_TOPICS + 1] = {
    "DomainA^NATSConnNews^NEWS_MOVIES_ALL",
    "DomainA^NATSConnNews^NEWS_Movies_Romance",
    "DomainA^NATSConnNews^NEWS_Movies_Westerns",
    NULL,
};


char* publisher_message_prefix[NUM_TOPICS + 1] =
    { "???", "ROMANCE Movie: ", "WESTERN Movie: " };

ndw_Topic_T* topic_array[NUM_TOPICS + 1];

static long message_bytes_size = 0;
static long print_frequency = 1;

static ndw_Counter_T counter_published;
static ndw_Counter_T counter_received;

void ndw_HandleBadMessage(ndw_BadMessage_T* bad_msg) {
    ndw_print_BadMessage(bad_msg);
}

int ndw_HandleAsyncMessage(ndw_Topic_T* topic, void* opaque)
{
    NDW_LOGERR("*** ERROR: This is not part of the NATS Poll based Subscription test!\n");
    return 0;
} // end method ndw_HandleAsyncMessage

int
set_topic_array()
{
    for (int i = 0; i < NUM_TOPICS; i++) {
        if (NULL == (topic_array[i] = ndw_GetTopicFromFullPath(topic_names[i]))) {
            NDW_LOGERR("Invalid topic pointer for <%s>\n", topic_names[i]);
            return -1;
        }
    }

    return 0;
} // end method set_topic_array

int
Subscribe()
{
    NDW_LOGX("===> BEGIN: Subscribe ===>\n\n");

    for (int i = 0; i < NUM_TOPICS; i++) {
        int ret_code = ndw_SubscribeSynchronouslyWithTopicName(topic_names[i]);
        if (0 != ret_code) {
            NDW_LOGERR("*** ERROR: Failed to Subscribe Synchronously for %s\n", topic_names[i]);
            return -1;
        }
    }
    NDW_LOGX("===> END: Subscribe ===>\n\n");

    return 0;
    
} // end method Subscribe

void
Unsubscribe()
{
    NDW_LOGX("===> BEGIN: Unsubscribe ===>\n\n");
    for (int i = 0; i < NUM_TOPICS; i++) {
        ndw_Topic_T* topic = topic_array[i];
        if (! topic->is_sub_enabled)
            ndw_Unsubscribe(topic_array[i]);
    }
    NDW_LOGX("===> END: Unsubscribe ===>\n\n");
} // end method Unsubscribe

int
publish_messages(int num_messages)
{
    NDW_LOGX("===> BEGIN: Start Publishing Messages ===>\n\n");

    long publish_count = 0;
    for (int i = 0; i < num_messages; i++)
    {
        for (int j = 0; j < NUM_TOPICS; j++)
        {
            ndw_Topic_T* topic = topic_array[j];
            if (topic->disabled || (!topic->is_pub_enabled)) {
                NDW_LOGX("SKIPPING TOPIC: <%s>\n", topic_names[j]);
                continue;
            }

            long sequence_number = ++topic->sequence_number;
            int data_size = 0;
            char* data = (message_bytes_size > 0) ?
                ndw_CreateTestJsonStringofNSize(topic,
                    publisher_message_prefix[j], sequence_number, message_bytes_size, &data_size) :
                ndw_CreateTestJsonString(topic, publisher_message_prefix[j], sequence_number, &data_size);

                data_size += 1;

                ndw_OutMsgCxt_T* cxt = ndw_CreateOutMsgCxt(topic,
                    NDW_MSGHEADER_1, NDW_ENCODING_FORMAT_JSON, (unsigned char*) data, data_size);
                if (NULL == cxt) {
                    NDW_LOGERR("Failed to create Output Message Context for <%s>!\n", topic_names[j]);
                    free(data);
                    return -2;
                }

                if (0 == (i % print_frequency)) {
                    NDW_LOG(">>> Outbounddata <%s>\ndata_size <%d> ON %s\n",
                            data, data_size, topic->debug_desc);
                }

                int ret_code = -1;
                if (0 != (ret_code = ndw_PublishMsg())) {
                    NDW_LOGERR("ndw_PublishMsg() failed! sequencer_number<%ld> with "
                        "total publish count<%ld>. ret_code<%d>\n",
                        sequence_number, publish_count, ret_code);
                    free(data);
                    return -2;
                }

            free(data);
            publish_count = ndw_CounterUpdate(&counter_published, 1);
        } // for each Topic
    } // for num_mesages

    NDW_LOGX("===> END: Start Publishing Messages ===>\n\n");

    return 0;
} // end method publish_messages

void
poll_for_messages()
{
    long timeout_ms = 100;
    ndw_TimeDuration_T duration;

    long no_msg_count = 0;
    long wait_time_for_no_messages = 7; // In seconds
    ndw_TimeDurationSecondsStart(&duration, wait_time_for_no_messages);
    long prev_msg_count = ndw_CounterCurrentValue(&counter_received);
    bool bail_out = false;

    while (! bail_out)
    {
        for (int i = 0; i < NUM_TOPICS; i++)
        {
            ndw_Topic_T* topic = topic_array[i];
            if (! topic->is_sub_enabled) {
                continue;
            }

            long dropped_msgs = 0;
            int received_msg_count = ndw_SynchronousPollForMsg(topic, timeout_ms, &dropped_msgs);

            if (received_msg_count <= 0)
            {
                long next_msg_count = ndw_CounterCurrentValue(&counter_received);
                long msg_count_delta = next_msg_count - prev_msg_count;
                if (0 == msg_count_delta) {
                    if (0 == no_msg_count) {
                        NDW_LOGX("NOTE: No message has arrived...\n");
                        ndw_TimeDurationSecondsStart(&duration, wait_time_for_no_messages);
                    }
                    else {
                        if (ndw_TimeDurationExpired(&duration)) {
                            ndw_PrintCurrentTime();
                            NDW_LOGX("Bailing out: no_msg_count<%ld> as no new messages after "
                                    "<%ld> seconds!\n", no_msg_count, wait_time_for_no_messages);
                            bail_out = true;
                            break;
                        }
                    }
                }

                no_msg_count += 1;
                continue;
            }

            no_msg_count = 0;
            ndw_CounterUpdate(&counter_received, 1);
            prev_msg_count = ndw_CounterCurrentValue(&counter_received);

            char* msg = (char*) topic->last_msg_received;
            int msg_size = topic->last_msg_received_size;
            long total_topic_msgs = topic->total_received_msgs;

            if ((NULL == msg) || (msg_size < 4)) {
                NDW_LOGERR("*** ERROR: Invalid msg_size: %d for %s\n", msg_size, topic->debug_desc);
                continue;
            }

            if (0 == (i % print_frequency)) {
                NDW_LOGX("<<< [%s] RECEIVED MESSAGE: Received TotalMsgs<%ld> TotalTopicMsgs<%ld> msg<%s> msg_length<%d> dropped_msgs<%ld> "
                    "for %s\n", topic->topic_unique_name, ndw_CounterCurrentValue(&counter_received), total_topic_msgs,
                    msg, msg_size, dropped_msgs, topic->debug_desc);
            }
        } // end for
    } // end while

} // end method poll_for_messages

int main(int argc, char** argv)
{
    long max_messages = 1;
    if (argc > 1) {
        int value = atoi(*(argv+1)); // Number of messages per Topic (Subject)
        if (value > 0)
            max_messages = value;

        if (argc > 2) {
            value = atoi(*(argv+2)); // Number of approximately bytes per message.
        if (value > 100)
            message_bytes_size = value;
        }
    }


    print_frequency = (max_messages < 100) ? 1 : (max_messages / 10);

    if (0 != ndw_Init())
        exit(-1);

    NDW_LOGX("max_messages<%ld> message_bytes_size<%ld>\n", max_messages, message_bytes_size);

    ndw_ConnectToDomains(domainNames);

    if (0 != set_topic_array())
        exit(-1);

    if (0 != Subscribe())
        exit(-1);

    if (0 != publish_messages(max_messages)) {
        exit(-1);
    }

    NDW_LOG("*** Start JS Poll Subscribe ***\n");
    poll_for_messages();
    
    NDW_LOGX("NOTE: TotalReceivedMsgs<%ld>\n", ndw_CounterCurrentValue(&counter_received));

    Unsubscribe();
    ndw_PrintStatsForDomains(domainNames);
    ndw_PrintCurrentTime();
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

