
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <nats/nats.h>

typedef struct {
    const char *subject;
    const char *durable;
} ConsumerInfo;

#define NUM_CONSUMER_SUBJECTS 3
ConsumerInfo consumers[NUM_CONSUMER_SUBJECTS] = {
    { "News.Archive.Movies.>", "MoviesArchiveConsumer" },
    { "News.Archive.Movies.Romance", "RomanceMoviesArchiveConsumer" },
    { "News.Archive.Movies.Westerns", "WesternsMoviesArchiveConsumer" },
};

#define NUM_PUBLICATION_SUBJECTS 2
const char* publisher_Streams_Subject[NUM_PUBLICATION_SUBJECTS] =
    { "News.Archive.Movies.Romance", "News.Archive.Movies.Westerns" };

char* publisher_message_prefix[NUM_PUBLICATION_SUBJECTS] =
    { "Romance Movie: ", "Western Movie: " };

#define NUM_CONSUMERS (sizeof(consumers) / sizeof(consumers[0]))

static natsConnection *conn = NULL;
static jsCtx *js = NULL;
static natsSubscription *subs[NUM_CONSUMERS] = { NULL };

void cleanup(void) {
    for (size_t i = 0; i < NUM_CONSUMERS; i++) {
        if (subs[i]) {
            natsSubscription_Destroy(subs[i]);
            subs[i] = NULL;
        }
    }
    if (js) {
        jsCtx_Destroy(js);
        js = NULL;
    }
    if (conn) {
        natsConnection_Destroy(conn);
        conn = NULL;
    }
    nats_Close();
}

void publishTestMessages(void) {
    char msgBuf[512];
    jsPubAck *ack = NULL;
    natsStatus s;

    for (int i = 0; i < NUM_PUBLICATION_SUBJECTS; i++) {
        snprintf(msgBuf, sizeof(msgBuf), "%s # %d", publisher_message_prefix[i], 1);

        s = js_Publish(&ack, js, publisher_Streams_Subject[i], msgBuf, (int)strlen(msgBuf), NULL, NULL);
        if (s != NATS_OK) {
            printf("---> ERROR: Publish error on <%s> ErrorText<%s>\n", publisher_Streams_Subject[i], natsStatus_GetText(s));
        } else {
            printf("---> Published on <%s>\n", publisher_Streams_Subject[i]);
            jsPubAck_Destroy(ack);
        }
    }
}

void pullMessagesForConsumer(int idx) {
    natsMsgList msgList;
    memset(&msgList, 0, sizeof(msgList));

    printf("Pulling messages for consumer '%s'...\n", consumers[idx].durable);

    natsStatus s = natsSubscription_Fetch(&msgList, subs[idx], 5, 2000, NULL);  // 5 messages, 2s timeout
    if (s != NATS_OK) {
        printf("Fetch failed for '%s': %s\n", consumers[idx].durable, natsStatus_GetText(s));
        return;
    }

    for (int i = 0; i < msgList.Count; i++) {
        natsMsg *msg = msgList.Msgs[i];

        jsMsgMetaData *meta = NULL;
        natsMsg_GetMetaData(&meta, msg);
        uint64_t stream_seq = (NULL == meta) ? 0 : meta->Sequence.Stream;
        uint64_t consumer_seq = (NULL == meta) ? 0 : meta->Sequence.Consumer;

        printf("<=== [%s] (SeqStream<%lu>, ConsumerSeq<%lu> Received: %.*s\n",
               consumers[idx].durable, stream_seq, consumer_seq,
               natsMsg_GetDataLength(msg), natsMsg_GetData(msg));
        natsMsg_Ack(msg, NULL);
        natsMsg_Destroy(msg);
    }

    free(msgList.Msgs); // Important
}

void handleSignal(int sig) {
    (void)sig;
    cleanup();
    exit(0);
}

int main(void) {
    natsStatus s;
    jsErrCode errCode;

    signal(SIGINT, handleSignal);
    atexit(cleanup);

    s = natsConnection_ConnectTo(&conn, "nats://localhost:4222");
    if (s != NATS_OK) {
        printf("Connection error: %s\n", natsStatus_GetText(s));
        return 1;
    }

    s = natsConnection_JetStream(&js, conn, NULL);
    if (s != NATS_OK) {
        printf("JetStream context error: %s\n", natsStatus_GetText(s));
        return 1;
    }

    for (size_t i = 0; i < NUM_CONSUMERS; i++) {
        s = js_PullSubscribe(&subs[i], js, consumers[i].subject, consumers[i].durable, NULL, NULL, &errCode);
        if (s != NATS_OK) {
            printf("Subscribe error for '%s': %s (JetStream code: %d)\n",
                   consumers[i].durable, natsStatus_GetText(s), (int)errCode);
            return 1;
        }
    }

    publishTestMessages();
    sleep(1);

    for (size_t i = 0; i < NUM_CONSUMERS; i++) {
        pullMessagesForConsumer(i);
    }

    return 0;
}

