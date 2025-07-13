
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <nats/nats.h>

// #define N 8
#define N 2

static const char *pubSubjects[N] = {
    "News.Sports.Football",
    "News.Sports.Football.College",
};

static const char *pubMessages[N] = {
    "Football score: 3-1",
    "Football COLLEGE score: 10-6",
};

typedef struct {
    const char *subject;
    const char *durable;
} ConsumerBinding;

static const ConsumerBinding consumers[] = {
    { "News.Sports.>",                  "SportsConsumer" },
    { "News.Sports.Football",           "FootballConsumer" },
    { "News.Sports.Football.College",   "CollegeFootballConsumer" },
};

static const int consumerCount = sizeof(consumers) / sizeof(consumers[0]);

static void
msgHandler(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    const char *subscribedTo = (const char *)closure;

    jsMsgMetaData *meta = NULL;
    if ((NATS_OK == natsMsg_GetMetaData(&meta, msg)) && (NULL != meta)) {
      printf("\n<=== [JS PUSH] subject: %s (consumer: %s) StreamSeq: %" PRIu64 " ConsumerSeq: %" PRIu64 "  %.*s\n",
               natsMsg_GetSubject(msg),
               subscribedTo ? subscribedTo : "(unknown)",
               meta->Sequence.Stream,
               meta->Sequence.Consumer,
               (int)natsMsg_GetDataLength(msg),
               natsMsg_GetData(msg));
    }
    else {
        printf("\n<=== [JS PUSH] subject: %s (consumer: %s) Seq:%lld  %.*s\n",
           natsMsg_GetSubject(msg),
           subscribedTo ? subscribedTo : "(unknown)",
           (long long)natsMsg_GetSequence(msg),
           (int)natsMsg_GetDataLength(msg),
           natsMsg_GetData(msg));
    }

    natsMsg_Ack(msg, NULL);
    natsMsg_Destroy(msg);
}

int main(int argc, char **argv)
{
    bool do_pub_only = false;
    bool do_sub_only = false;
    bool do_both = true;

    if (argc > 1) {
        char* option = *(argv+1);
        printf("option <%s>\n", option);
        if (0 == strcmp("pub", option))
            do_pub_only = true;
        else if (0 == strcmp("sub", option))
            do_sub_only = true;
        else
        {
            fprintf(stderr, "Invalid option<%s> Valid options are \"pub\" or \"sub\"\n", option);
            exit(-1);
        }
    }

    do_both =  (do_pub_only || do_sub_only) ? false : true;

    printf("do_pub_only<%s> do_sub_only<%s> do_both<%s>\n",
                do_pub_only ? "True" : "false", do_sub_only ? "True" : "false", do_both ? "True" : "false");

    bool do_pub = do_both || do_pub_only;
    bool do_sub = do_both || do_sub_only;
    printf("do_pub<%s> do_sub<%s>\n", do_pub ? "True" : "false", do_sub ? "True" : "false");

    natsStatus s;
    natsConnection *conn = NULL;
    jsCtx *js = NULL;
    natsSubscription **subs = NULL;

    s = natsConnection_ConnectTo(&conn, NATS_DEFAULT_URL);
    if (s != NATS_OK) {
        fprintf(stderr, "connect: %s\n", natsStatus_GetText(s));
        return 1;
    }

    s = natsConnection_JetStream(&js, conn, NULL);
    if (s != NATS_OK) {
        fprintf(stderr, "JetStream context: %s\n", natsStatus_GetText(s));
        natsConnection_Destroy(conn);
        return 1;
    }

    if (do_sub) {
        subs = calloc(consumerCount, sizeof(natsSubscription*));
        for (int i = 0; i < consumerCount; i++) {
            jsErrCode errCode = 0;
            jsSubOptions subOpts;
            memset(&subOpts, 0, sizeof(subOpts));
            subOpts.Stream = "NEWS";
            subOpts.Consumer = consumers[i].durable;
            subOpts.Config.AckPolicy = js_AckExplicit;
            subOpts.ManualAck = true;

            s = js_Subscribe(&subs[i],
                         js,
                         consumers[i].subject,  // This must match the subject filter
                         msgHandler,
                         (void *)consumers[i].durable,
                         NULL, // no jsOptions
                         &subOpts,
                         &errCode);

            if (s != NATS_OK) {
                fprintf(stderr, "Subscribe %s failed (subject: %s): natsStatus_GetText(\"%s\") (errCode=%d)\n",
                    consumers[i].durable, consumers[i].subject, natsStatus_GetText(s), errCode);
                goto cleanup;
            }

            printf("Subscribed to existing durable: %s (filter: %s) (errCode=%d)\n",
               consumers[i].durable, consumers[i].subject, errCode);
        }
    }

    if (do_pub) {
        // Publish messages to various subjects
        for (int i = 0; i < N; i++) {
            jsPubAck *ack = NULL;
            s = js_Publish(&ack, js, pubSubjects[i],
                           (const void *)pubMessages[i],
                           (int)strlen(pubMessages[i]),
                           NULL, NULL);
            if (s != NATS_OK) {
                fprintf(stderr, "*** ERROR ===> Publish %s: %s\n", pubSubjects[i], natsStatus_GetText(s));
            } else {
                printf("---> Published to %s: %s\n", pubSubjects[i], pubMessages[i]);
            }
            jsPubAck_Destroy(ack);
        }
    }

    printf("Waiting 5s ...\n");
    nats_Sleep(5000);

cleanup:
    for (int i = 0; i < consumerCount; i++) {
        if (subs && subs[i])
            natsSubscription_Destroy(subs[i]);
    }
    free(subs);

    jsCtx_Destroy(js);
    natsConnection_Destroy(conn);
    nats_Close();

    return (s == NATS_OK ? 0 : 1);
}

