
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <nats/nats.h>

#define NUM_MESSAGES 10
#define WAIT_FOR_COMPLETION 5
#define REQUEST_REPLY_TIMEOUT_VALUE 2000  // in milliseconds


static const char* subjects[] = {
    "test.subject.1",
    "test.subject.2",
    NULL
};

static void messageHandler(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure) {
    printf("Received on '%s': %s\n", natsMsg_GetSubject(msg), natsMsg_GetData(msg));
    natsMsg_Destroy(msg);
}

void
test_async_response()
{
    printf("\n============= BEGIN: test_async =============\n\n");

    natsConnection *conn = NULL;
    natsSubscription *subs[2] = { NULL, NULL };
    natsStatus s;
    char msgBuf[256];
    int i, j;

    // Connect to NATS server
    printf("Connection to NATS URL: <%s>\n", NATS_DEFAULT_URL);
    s = natsConnection_ConnectTo(&conn, NATS_DEFAULT_URL);
    if (s != NATS_OK) {
        fprintf(stderr, "Error connecting to NATS: %s\n", natsStatus_GetText(s));
        return;
    }

    // Subscribe to subjects
    for (i = 0; subjects[i] != NULL; i++) {
        s = natsConnection_Subscribe(&subs[i], conn, subjects[i], messageHandler, NULL);
        if (s != NATS_OK) {
            fprintf(stderr, "Error subscribing to %s: %s\n", subjects[i], natsStatus_GetText(s));
            // Cleanup and return
            for (int k = 0; k < i; k++)
                natsSubscription_Destroy(subs[k]);
            natsConnection_Destroy(conn);
            return;
        }
    }

    // Publish messages
    for (i = 1; i <= NUM_MESSAGES; i++) {
        for (j = 0; subjects[j] != NULL; j++) {
            snprintf(msgBuf, sizeof(msgBuf), "Message #%d to subject '%s'", i, subjects[j]);
            s = natsConnection_Publish(conn, subjects[j], (const void*)msgBuf, (int)strlen(msgBuf));
            if (s != NATS_OK) {
                fprintf(stderr, "Error publishing to %s: %s\n", subjects[j], natsStatus_GetText(s));
            }
        }
    }

    // Flush to ensure all messages sent
    s = natsConnection_Flush(conn);
    if (s != NATS_OK) {
        fprintf(stderr, "Error flushing connection: %s\n", natsStatus_GetText(s));
    }

    // Wait for messages to arrive
    sleep(WAIT_FOR_COMPLETION);

    // Unsubscribe and cleanup
    for (i = 0; subjects[i] != NULL; i++) {
        natsSubscription_Unsubscribe(subs[i]);
        natsSubscription_Destroy(subs[i]);
    }

    natsConnection_Destroy(conn);
    nats_Close();

    printf("\n============= END: test_async =============\n\n");
} // end method test_async_response()

void
test_synchronous()
{
    printf("\n============= BEGIN: test_synchronous =============\n\n");

    natsConnection *conn = NULL;
    natsSubscription *sub = NULL;
    natsStatus s;
    char msgBuf[256];
    natsMsg *msg = NULL;
    int i;

    // Connect to NATS server
    printf("Connection to NATS URL: <%s>\n", NATS_DEFAULT_URL);
    s = natsConnection_ConnectTo(&conn, NATS_DEFAULT_URL);
    if (s != NATS_OK) {
        fprintf(stderr, "Error connecting to NATS: %s\n", natsStatus_GetText(s));
        return;
    }

    // Subscribe synchronously to first subject
    s = natsConnection_SubscribeSync(&sub, conn, subjects[0]);
    if (s != NATS_OK) {
        fprintf(stderr, "Error subscribing to %s: %s\n", subjects[0], natsStatus_GetText(s));
        natsConnection_Destroy(conn);
        return;
    }

    // Publish messages to the first subject
    for (i = 1; i <= NUM_MESSAGES; i++) {
        snprintf(msgBuf, sizeof(msgBuf), "Message #%d to subject '%s'", i, subjects[0]);
        s = natsConnection_Publish(conn, subjects[0], (const void*)msgBuf, (int)strlen(msgBuf));
        if (s != NATS_OK) {
            fprintf(stderr, "Error publishing to %s: %s\n", subjects[0], natsStatus_GetText(s));
        }
    }

    // Flush to ensure all messages sent
    s = natsConnection_Flush(conn);
    if (s != NATS_OK) {
        fprintf(stderr, "Error flushing connection: %s\n", natsStatus_GetText(s));
    }

    // Receive messages synchronously
    for (i = 1; i <= NUM_MESSAGES; i++) {
        s = natsSubscription_NextMsg(&msg, sub, 5000); // wait up to 5 seconds
        if (s == NATS_OK) {
            printf("Received message: %.*s\n", (int)natsMsg_GetDataLength(msg), natsMsg_GetData(msg));
            natsMsg_Destroy(msg);
        } else if (s == NATS_TIMEOUT) {
            printf("Timeout waiting for message #%d\n", i);
        } else {
            fprintf(stderr, "Error receiving message #%d: %s\n", i, natsStatus_GetText(s));
            break;
        }
    }

    // Cleanup
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(conn);
    nats_Close();

    printf("\n============= END: test_synchronous =============\n\n");
} // end method test_synchronous

// These variables areneeded for Request-Reply as this happens between 2 threads.
natsConnection *g_conn; // global connection pointer for replier thread
volatile int g_replier_should_exit = 0; // exit flag for replier thread
pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;

// Replier callback using manual reply pattern
void
request_reply_handler(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    // Print the incoming request
    pthread_mutex_lock(&print_mutex);
    printf("[Replier] Received request on '%s': %.*s\n",
           natsMsg_GetSubject(msg),
           (int)natsMsg_GetDataLength(msg),
           natsMsg_GetData(msg));
    pthread_mutex_unlock(&print_mutex);

    // Fetch the auto-generated reply subject
    const char *replySubj = natsMsg_GetReply(msg);
    if (replySubj != NULL) {
        const char *response = "ACK from replier";
        natsStatus r = natsConnection_Publish(
            nc,                    // same connection
            replySubj,             // inbox subject
            (const void*)response, // payload
            (int)strlen(response)
        );
        if (r != NATS_OK) {
            pthread_mutex_lock(&print_mutex);
            fprintf(stderr, "[Replier] Publish reply failed: %s\n", natsStatus_GetText(r));
            pthread_mutex_unlock(&print_mutex);
        }
    }

    natsMsg_Destroy(msg);
} // end method request_reply_handler

void
*replier_thread_func(void *arg)
{
    natsSubscription *sub = NULL;
    natsStatus         s;

    // Asynchronous subscribe with our manual-reply handler
    s = natsConnection_Subscribe(&sub,
                                 g_conn,
                                 subjects[0],
                                 request_reply_handler,
                                 NULL);
    if (s != NATS_OK) {
        pthread_mutex_lock(&print_mutex);
        fprintf(stderr, "[Replier] Subscribe error: %s\n", natsStatus_GetText(s));
        pthread_mutex_unlock(&print_mutex);
        return NULL;
    }
    // Ensure the subscription is registered
    natsConnection_Flush(g_conn);

    // Keep thread alive until signaled to exit
    while (! g_replier_should_exit) {
        sleep(1);
    }

    natsSubscription_Destroy(sub);
    return NULL;
} // end method 



void
test_request_reply(void)
{
    printf("\n============= BEGIN: Test REQUEST-REPLY =============\n\n");
    natsOptions *opts = NULL;
    natsStatus s;
    natsMsg *reply = NULL;
    pthread_t replier_thread;

    // Create options
    s = natsOptions_Create(&opts);
    if (s != NATS_OK) {
        pthread_mutex_lock(&print_mutex);
        fprintf(stderr, "Error creating options: %s\n", natsStatus_GetText(s));
        pthread_mutex_unlock(&print_mutex);
        return;
    }

    // Connect to NATS server
    printf("Connection to NATS URL: <%s>\n", NATS_DEFAULT_URL);
    s = natsConnection_Connect(&g_conn, opts);
    natsOptions_Destroy(opts);
    if (s != NATS_OK) {
        pthread_mutex_lock(&print_mutex);
        fprintf(stderr, "Error connecting to NATS server: %s\n", natsStatus_GetText(s));
        pthread_mutex_unlock(&print_mutex);
        return;
    }

    g_replier_should_exit = 0;

    // Start the replier thread
    if (pthread_create(&replier_thread, NULL, replier_thread_func, NULL) != 0) {
        pthread_mutex_lock(&print_mutex);
        fprintf(stderr, "Failed to create replier thread\n");
        pthread_mutex_unlock(&print_mutex);
        natsConnection_Destroy(g_conn);
        return;
    }

    sleep(1); // Give replier thread time to subscribe

    char msgbuf[256];

    for (int i = 1; i <= NUM_MESSAGES; i++) {
        snprintf(msgbuf, sizeof(msgbuf), "Request #%d from requester", i);

        pthread_mutex_lock(&print_mutex);
        printf("[Requester] Sending request: %s\n", msgbuf);
        pthread_mutex_unlock(&print_mutex);

        s = natsConnection_Request(&reply, g_conn, subjects[0], (const void *)msgbuf, (int)strlen(msgbuf), REQUEST_REPLY_TIMEOUT_VALUE);
        if (s == NATS_OK) {
            pthread_mutex_lock(&print_mutex);
            printf("[Requester] Got reply: %.*s\n", (int)natsMsg_GetDataLength(reply), natsMsg_GetData(reply));
            pthread_mutex_unlock(&print_mutex);

            natsMsg_Destroy(reply);
        }
        else if (s == NATS_TIMEOUT) {
            pthread_mutex_lock(&print_mutex);
            printf("[Requester] Request timed out for message #%d\n", i);
            pthread_mutex_unlock(&print_mutex);
        }
        else {
            pthread_mutex_lock(&print_mutex);
            printf("[Requester] Request error: %s\n", natsStatus_GetText(s));
            pthread_mutex_unlock(&print_mutex);
        }
    }

    // Signal replier thread to exit and wait for it
    g_replier_should_exit = 1;
    pthread_join(replier_thread, NULL);

    // Cleanup connection
    natsConnection_Destroy(g_conn);
    g_conn = NULL;
    nats_Close();

    printf("\n============= END: Test REQUEST-REPLY =============\n\n");
} // test_request_reply


int
main()
{
    printf("\n NATS Version <%s>\n", nats_GetVersion());
    test_async_response();

    sleep(2);
    test_synchronous();

    sleep(2);
    test_request_reply();

    printf("\n NATS Version <%s>\n", nats_GetVersion());

    return 0;
} // end method main

