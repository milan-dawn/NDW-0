
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "NDW_Essentials.h"

const char *domainNames[] = { "DomainA", NULL, };

#define NUM_TOPICS 3
char* topic_names[NUM_TOPICS + 1] = {
    "DomainA^NATSConn1^ACME.Orders",
    "DomainA^NATSConn1^ACME.Invoices",
    "DomainA^NATSConn2^ACME.Shipments",
    NULL,
};

ndw_Topic_T* topic_array[NUM_TOPICS + 1];

static ndw_Counter_T counter_received;

void ndw_HandleBadMessage(ndw_BadMessage_T* bad_msg) {
    ndw_print_BadMessage(bad_msg);
}

int ndw_HandleAsyncMessage(ndw_Topic_T* topic, void* opaque)
{
    NDW_LOGERR("This is not part of the test!\n");
    return 0;
} // end method ndw_HandleAsyncMessage

int main(int argc, char** argv)
{
    long total_time_seconds = 15;

    if (argc > 1) {
        long value = atol(*(argv+1));
        if (value > total_time_seconds)
            total_time_seconds = value;
    }

    if (0 != ndw_Init())
        exit(-1);

    ndw_PrintCurrentTime();
    NDW_LOG("Total Sleep Seconds <%ld>\n\n", total_time_seconds);

    ndw_ConnectToDomains(domainNames);

    for (int i = 0; i < NUM_TOPICS; i++) {
        topic_array[i] = ndw_GetTopicFromFullPath(topic_names[i]);
        if (0 != ndw_SubscribeSynchronous(topic_array[i])) {
            NDW_LOGERR("Failed to ndw_SubscribeSyncronouslyToTopic<%d, %s>\n",
                topic_array[i]->topic_unique_id, topic_array[i]->topic_unique_name);
            break;
        }
    }
        

    NDW_LOG("*** Start SyncSubscribe ***\n");
    
    long timeout_ms = 250;
    long dropped_msgs = 0;
    long start_time_nanos = ndw_GetCurrentNanoSeconds();
    ndw_PrintTimeFromNanos(ndw_GetCurrentNanoSeconds() - start_time_nanos);

    long time_to_print_ms = 1 * 1000;
    ndw_TimeDuration_T duration_print;
    ndw_TimeDurationMillisStart(&duration_print, time_to_print_ms);

    ndw_TimeDuration_T duration;
    ndw_TimeDurationMillisStart(&duration, total_time_seconds * 1000);
    

    while (! ndw_TimeDurationExpired(&duration)) {
        for (int i = 0; i < NUM_TOPICS; i++) {
            ndw_Topic_T* topic = topic_array[i] = ndw_GetTopicFromFullPath(topic_names[i]);
            dropped_msgs = 0;

            if (1 != ndw_SynchronousPollForMsg(topic, timeout_ms, &dropped_msgs)) {
                if (ndw_TimeDurationExpired(&duration_print)) {
                    NDW_LOG("[%ld]: ...\n", ndw_CounterCurrentValue(&counter_received));
                    ndw_TimeDurationMillisStart(&duration_print, time_to_print_ms);
                }
                continue;
            }
    
            char* msg = (char*) topic->last_msg_received;
            int msg_size = topic->last_msg_received_size;
            long total_topic_msgs = topic->total_received_msgs;
            if ((NULL != msg) && (msg_size > 0)) {
                ndw_CounterUpdate(&counter_received, 1);
                NDW_LOG("Total Messages:[%ld] Topic Total Messages:[%ld] "
                    "Received Topic<%d, %s> msg<%s> msg_length<%d> dropped_msgs<%ld>\n",
                    ndw_CounterCurrentValue(&counter_received), total_topic_msgs,
                    topic->topic_unique_id, topic->topic_unique_name, msg, msg_size, dropped_msgs);
            }
        }
    }

    ndw_PrintTimeFromNanos(ndw_GetCurrentNanoSeconds() - start_time_nanos);
    NDW_LOGX("NOTE: TotalReceivedMsgs<%ld>\n", ndw_CounterCurrentValue(&counter_received));

    ndw_PrintStatsForDomains(domainNames);
    ndw_PrintCurrentTime();
    ndw_Shutdown();
    return 0;
} /* end method main */

