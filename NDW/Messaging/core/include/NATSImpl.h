
#ifndef _NATS_IMPL_H
#define _NATS_IMPL_H

#include "ndw_types.h"

#include <stdio.h>

#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <cjson/cJSON.h>
#include <string.h>
#include <endian.h>
#include <pthread.h>

#include <nats/nats.h>
#include <nats/status.h>

#include "ndw_types.h"
#include "NDW_Utils.h"
#include "RegistryData.h"
#include "VendorImpl.h"
#include "AbstractMessaging.h"

/*
 * uthash User Guide: https://troydhanson.github.io/uthash/userguide.html
 */
#include "uthash.h"

/**
 * @file NATSImpl.h
 *
 * @brief This is NATS and JetStream vendor usage concrete implementation.
 * AbstractMessaging.h dictates which functions the vendor implementation needs to support for concreteness.
 * The Abstract messaging layer simulates inheritance through pointer to functions.
 * Check the header file, VendorImpl.h and the structure ndw_ImplAPIT_T to see the key functions any
 *  vendor implementation needs to support.
 *
 * @see AbstractMessaging.h
 * @see VendorImpl.h
 *
 * @author Andrena team member
 * @date 2025-07
 */

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

typedef struct ndw_NATS_Topic ndw_NATS_Topic_T;
typedef struct ndw_NATS_Connection ndw_NATS_Connection_T;


/**
 * @def NDW_NATS_TOPIC_MSGLIMIT_OPTION
 * @brief Configuration set in configuration file for Topic Maximum message limit option.
 */
#define NDW_NATS_TOPIC_MSGLIMIT_OPTION "MsgsLimit"

/**
 * @def NDW_NATS_TOPIC_BYTESLIMIT_OPTION
 * @brief Configuration set in configuration file for Topic Maximum bytes usage limit option.
 */
#define NDW_NATS_TOPIC_BYTESLIMIT_OPTION "BytesLimit"

/**
 * @struct ndw_NATS_JS_Attr_T
 * @brief NATS Jetstream (JS) specific configurations
 *  Too bad that it was done as an after thought and hence requires special treatment throughout the code!
 *  Configuration:
 *      A Stream has a unique Name. IT has a list of subjects, referred to a Stream Subject Names.
 *      Then we add consumers to the Stream.
 *      Consumers have a Durable Name and a Filter to subscribe on.
 *
 * How you subscribe and publish is kind of... messy design!
 *
 * Publication:
 *    When you publish we ONLY need the Stream Subject Name.
 *
 * Subscription:
 *   There are 2 types of Subscriptions:
 *   PUSH - Asynchronous notification
 *      To subscribe you need 3 things: Stream, Consumer Durable Name, and Filter.
 *   PULL - Polling
 *      To subscribe you need 2 things: Consumer Durable Name, and Filter.
 */
typedef struct ndw_NATS_JS_Attr
{
    bool is_initialized;          // Have we done initialization?
    bool is_config_error;         // If we have a configuration error during parsing of options.
    bool is_enabled;              // Durability applicable?
    bool is_pub_enabled;          // Is Publish enabled.
    bool is_sub_enabled;          // Is Subscribe enabled.
    bool is_push_mode;            // Push mode = Asynchronous notification; Pull mode = Polling.
    bool is_pull_mode;            // Push mode = Pull mode
    bool is_sub_browse_mode;      // Do not ack when receiving a message; queue browsing mode.

    // NOTE: When you publish you just need the 
    // NOTE: When you subscribe you need 3 things: stream name, durable_name and filter!

    const CHAR_T* stream_name;           // JS stream name.
    const CHAR_T* stream_subject_name;   // JS stream name.
    const CHAR_T* durable_name;          // Each consumer has a durable name in JS.
    const CHAR_T* filter;                // The key that the publisher sends on. Defaults to ndw_Topic_T sub_key.
    CHAR_T* debug_desc;

    ULONG_T sub_sequence_stream;         // Subject stream sequence position.
    ULONG_T sub_sequence_consumer;       // Subject stream sequence position for the consumer of messages.
    ULONG_T sub_num_delivered;           // Number of messages delivered to the subscribe.
    ULONG_T sub_num_pending;             // Number of messages yet to be delivered to the subscribe.

    ULONG_T durable_ack_sequence;        // Number of messages yet to be delivered to the subscribe.
    ULONG_T durable_ack_global_sequence; // Global ack sequence number of delivered messages.
    ULONG_T durable_total_delivered;     // Total number of durable numbers delivered so far.
    ULONG_T durable_total_pending;       // Total number of durable numbers yet to be delivered.

} ndw_NATS_JS_Attr_T; 

/*
 * JS Attributes we need.
 */

/**
 * @def NDW_NATS_JS_ATTR_JETSTREAM_NAME
 * @brief Tag in configuration to indicate it is a JetStream specific configuration.
 */
#define NDW_NATS_JS_ATTR_JETSTREAM_NAME         "Jetstream"

/**
 * @def NDW_NATS_JS_ATTR_STREAM_NAME
 * @brief Tag in configuration to indicate it is a JetStream stream name configuration.
 */
#define NDW_NATS_JS_ATTR_STREAM_NAME            "StreamName"

/**
 * @def NDW_NATS_JS_ATTR_STREAM_SUBJECT_NAME
 * @brief Tag in configuration to indicate it is a JetStream subject name configuration.
 */
#define NDW_NATS_JS_ATTR_STREAM_SUBJECT_NAME    "SubjectName"

/**
 * @def NDW_NATS_JS_ATTR_PUSH_SUBSCRIBE
 * @brief Tag in configuration to indicate it is a JetStream PUSH based subscription (as opposed to a PULL subscription).
 */
#define NDW_NATS_JS_ATTR_PUSH_SUBSCRIBE         "PushSubscribe"

/**
 * @def NDW_NATS_JS_ATTR_PULL_SUBSCRIBE
 * @brief Tag in configuration to indicate it is a JetStream PULL based subscription (as opposed to a PUSH subscription).
 */
#define NDW_NATS_JS_ATTR_PULL_SUBSCRIBE         "PullSubscribe"

/**
 * @def NDW_NATS_JS_ATTR_DURABLE_NAME
 * @brief Tag in configuration to indicate it is a JetStream subject durable name configuration.
 */
#define NDW_NATS_JS_ATTR_DURABLE_NAME           "DurableName"

/**
 * @def NDW_NATS_JS_ATTR_FILTER_NAME
 * @brief Tag in configuration to indicate it is a JetStream subject filter configuration.
 */
#define NDW_NATS_JS_ATTR_FILTER_NAME            "FilterName"

/**
 * @def NDW_NATS_JS_ATTR_BROWSE_MODE
 * @brief Tag in configuration to indicate it is a JetStream queue browser based subscription and hence
 *  the message should not be acked, else it will get popped off the queue.
 */
#define NDW_NATS_JS_ATTR_BROWSE_MODE            "Browse"

/*
 * NOTE of great importance!
 *
 * It is hard to remember various JetStream parameters to use when publishing messaeges and subscriber options to use.
 * Hence this level of documentation is warranted here.
 *
 * (1) JetStream Publisher - Only has to use the JetSteram Subject Name.
 *
 * (2) JetStream Subscribers - 2 types of subscribers:
 *     (a) PUSH (aysnchronous notification) subscribers:
 *          (i) Push subscribers must use Stream Namein jsOptions, and Durable Name and Consumer Name in function parameters.
 *     (b) PULL (synchronously polling) subscribers:
 *          (i) Pull subscribers must use Durable Name and Consumer Name in function parameters.
 */

typedef struct ndw_NATS_Connection ndw_NATS_Connection_T;

/**
 * @struct ndw_NATS_Topic_T
 * @brief NATS specific Topic. Holds NATS specific values and configuration for both publication and subscription.
 * It has a back pointer to ndw_Topic_T defined in AbstractMessaging.h header file.
 *
 * @see ndw_Topic_T structure defined in AbstractMessaging.h header file.
 */

typedef struct ndw_NATS_Topic
{
    bool durable_topic;                         // Is it a durable (persistent) Topic?
    LONG_T initiation_time;                     // Topic initiation time in UTC.
    LONG_T total_messages_published;            // Total messages published on this Topic since application started.
    LONG_T total_messages_received;             // Total messages received on this Topic since application started.
    LONG_T total_messages_get_requestmsg;       // Total messages response received for published on Topic since app started.

    ndw_NATS_Connection_T* nats_connection;     // Physical NATS Connection object Pointer.

    natsSubscription* nats_subscription;        // Physical NATS Subscription object Pointer.

    ndw_Topic_T* ndw_topic;                     // Back pointer to NDW_TOPIC_T defined in AbstractMessaging.h header file.

    INT_T subscription_PendingLimitsMsgs;       // Subscription pending limits for number of messages.
    INT_T subscription_PendingLimitsBytes;      // Subscription pending limits for number of bytes.

    ULONG_T subscription_PendingMsgs;           // Number of pending messages for this Subscription.
    ULONG_T subscription_PendingBytes;          // Number of pending bytes for this Subscription.
    ULONG_T subscription_DeliveredMsgs;         // Total number of delivered messages on this Subscription.
    ULONG_T subscription_DroppedMsgs;           // Total number of dropped messages for this Subscription.

    ndw_NATS_JS_Attr_T   nats_js_attr;          // Jet Stream Attributes: configuration, publication and subscription.

} ndw_NATS_Topic_T;


/**
 * @def NDW_NATS_CONNECTION_TCP_NODELAY_OPTION
 * @brief TCP_NODELAY socket option for NATS Connection.
 */
#define NDW_NATS_CONNECTION_TCP_NODELAY_OPTION "TCP_NODELAY"

/**
 * @def NDW_NATS_CONNECTION_NUMBER_OF_CALLBACK_THREADS
 * @brief Configuration for number of asynchronous notification threads NATS should use.
 */
#define NDW_NATS_CONNECTION_NUMBER_OF_CALLBACK_THREADS "CallbackThreads"

/**
 * @def NDW_NATS_CONNECTION_PUBLICATION_BACKOFF_BYTES
 * @brief Number of backoff bytes configuration name for NATS Connection.
 */
#define NDW_NATS_CONNECTION_PUBLICATION_BACKOFF_BYTES "PublicationBackoffBytes"

/**
 * @def NDW_NATS_CONNECTION_PUBLICATION_DEFAULT_BACKOFF_BYTES
 * @brief Default number of backoff bytes for the Publication.
 */
#define NDW_NATS_CONNECTION_PUBLICATION_DEFAULT_BACKOFF_BYTES (2 * 1024 * 1024)

/**
 * @def NDW_NATS_CONNECTION_PUBLICATION_BACKOFF_SLEEP_US
 * @brief NATS Connection backoff sleep configuration name..
 */
#define NDW_NATS_CONNECTION_PUBLICATION_BACKOFF_SLEEP_US "BackoffSleep_us"

/**
 * @def NDW_NATS_CONNECTION_PUBLICATION_DEFAULT_BACKOFF_SLEEP_US
 * @brief Default value for NATS Connection backoff sleep configuration.
 */
#define NDW_NATS_CONNECTION_PUBLICATION_DEFAULT_BACKOFF_SLEEP_US 100

/**
 * @def NDW_NATS_CONNECTION_PUBLICATION_BACKOFF_ATTEMPTS
 * @brief Configuration name for number of retries when a Publication fails.
 */
#define NDW_NATS_CONNECTION_PUBLICATION_BACKOFF_ATTEMPTS "BackoffAttempts"

/**
 * @def NDW_NATS_CONNECTION_PUBLICATION_DEFAULT_BACKOFF_ATTEMPTS
 * @brief Default value for number of retries when a Publication fails.
 */
#define NDW_NATS_CONNECTION_PUBLICATION_DEFAULT_BACKOFF_ATTEMPTS 5

/**
 * @def NDW_NATS_CONNECTION_FLUSH_TIMEOUT_MS_OPTION
 * @brief Configuration name for Connection Flush Timeout in milliseconds.
 */
#define NDW_NATS_CONNECTION_FLUSH_TIMEOUT_MS_OPTION "FlushTimeoutMs"

/**
 * @def NDW_NATS_CONNECTION_FLUSH_TIMEOUT_MS
 * @brief Default value of NATS Connection flush timeout in milliseconds.
 */
#define NDW_NATS_CONNECTION_FLUSH_TIMEOUT_MS 500

/**
 * @def NDW_NATS_CONNECTION_IO_BUFSIZE_KB
 * @brief Configuration name for NATS COnnetion IO Buffer Size in Kilobytes.
 */
#define NDW_NATS_CONNECTION_IO_BUFSIZE_KB "IOBufSizeKB"

/**
 * @def NDW_NATS_CONNECTION_DEFAULT_IO_BUFSIZE_KB
 * @brief  Default value of NATS Connection IO Buffer size.
 */
#define NDW_NATS_CONNECTION_DEFAULT_IO_BUFSIZE_KB 1024

/**
 * @def NDW_NATS_CONNECTION_TIMEOUT_MS
 * @brief Configuration: Connection timeout wait in milliseconds.
 * This timeout, expressed in milliseconds, is used to interrupt a (re)connect
 * attempt to a `NATS Server`. This timeout is used both for the low level TCP
 * connect call, and for timing out the response from the server to the client's
 * initial `PING` protocol.
 * Default set by NATS is 2000 milliseconds (2 seconds) 
 */
#define NDW_NATS_CONNECTION_TIMEOUT_MS "ConnectionTimeoutMS"

/**
 * @def NDW_NATS_CONNECTION_MAX_RECONNECTS
 * @brief Configuration: Maximum number of times to try to connection.
 * Default set by NATS is 60.
 */
#define NDW_NATS_CONNECTION_MAX_RECONNECTS "ConnectionMaxReconnects"

/**
 * @def NDW_NATS_CONNECTION_MAX_RECONNECT_WAIT_MS
 * @brief Configuration: Maximum number of times to try to connection.
 * Default set by NATS is 2000 milliseconds.
 * This means you wait for NDW_NATS_CONNECTION_MAX_RECONNECTS times NDW_NATS_CONNECTION_MAX_RECONNECT_WAIT_MS
 */
#define NDW_NATS_CONNECTION_MAX_RECONNECT_WAIT_MS "ConnectionReconnectWaitMS"

/**
 * @def NDW_NATS_CONNECTION_NOECHO
 * @brief Configuration: We think it should always be false.
 * This prevents a subscriberto listen messages published on this connection EVEN if it
 * tries to subscribe to it. So choose wisely!
 * Default set by NATS is false.
 */
#define NDW_NATS_CONNECTION_NOECHO "ConnectionNoEcho"


/**
 * @struct ndw_NATS_Connection_T
 * @brief Data structure that holds NATS connection state.

 * @see ndw_Connection_T in Registry.h header file. 
 */
typedef struct ndw_NATS_Connection
{
    LONG_T connect_time;                // Connection time in UTC.
    LONG_T disconnect_time;             // Disconnect time in UTC.
    ndw_Connection_T* ndw_connection;   // Back Pointer to ndw_Connection_T defined in RegistryData.h
    const CHAR_T* url;                  // NATS Brokder Network Connection URL. Has NATS broker port number encoded in it.
    natsConnection *conn;               // Physical NATS Connection object.
    natsStatus status;                  // Current status of NATS Connection.

    bool tcp_nodelay;                   // Set socket TCP_NODELAY option?
    LONG_T number_of_callback_threads;  // Number of threads that should be used for asynchronous message notifications.
    LONG_T flush_timeout_ms;            // Connection flush timeout value in milliseconds.
    LONG_T publication_backoff_bytes;   // Connection flush timeout value in milliseconds.
    LONG_T backoff_sleep_us;            // Connection backoff sleep in microseconds.
    LONG_T backoff_attempts;            // Number of backoff attempts.
    LONG_T total_backoff_attempts;      // Total number of backoff attempts made so far.
    LONG_T iobuf_size;                  // IO Buffer size to use for this NATS Connection.

    LONG_T connection_timeout;          // When initiating a connection specify timeout. Default is 2000 ms.
    LONG_T connection_max_reconnects;   // How many times to try to reconnect. Default is 60.
    LONG_T connection_reconnection_wait; // How long to wait between two reconnect attempts from the same server.
                                         // Default is 2000 milliseconds.
    bool connection_no_echo;            // We think it should always be false. This prevents a subscriber
                                        // to listen messages published on this connection EVEN if it
                                        // tries to subscribe to it. So choose wisely!

    jsCtx* js_context;                  // Unfortunately, Jetstream is treated different like an... add-on.
    INT_T js_enabled_count;             // Jetstream Topic enablementcount for this NATS connection.

} ndw_NATS_Connection_T;


/**
 * @brief A Pointer toa function that is invoked by Abstraction Layer for the
 *  vendor (NATS) to set function to Pointers for all API implementations.
 *
 * @param[in] impl Implementation Pointer (for simulating inheritance).
 * @param[out] vendor_id Unique vendor identifier assigned for NATS.
 *
 * @return 0 on succees, else < 0.
 */
extern INT_T ndw_NATS_InitDerivation(ndw_ImplAPI_T* impl, INT_T vendor_id);

/**
 * @brief Initiate a physical Connection to NATS broker.
 * @param[in] connection Abstraction Layer Logical Connection object.
 *
 * @return 0 on succees, else < 0.
 */
extern INT_T ndw_NATS_Connect(ndw_Connection_T* connection);

/**
 * @brief Disconnect a connection to NATS broker.
 * @param[in] connection Abstraction Layer Logical Connection object.
 *
 * @return 0 on succees, else < 0.
 */
extern INT_T ndw_NATS_Disconnect(ndw_Connection_T* connection);

/**
 * @brief Shutdown a connection to NATS broker.
 * @param[in] connection Abstraction Layer Logical Connection object.
 *
 * @return 0 on succees, else < 0.
 */
extern void ndw_NATS_ShutdownConnection(ndw_Connection_T* connection);

/**
 * @brief Inspect if a connection to NATS broker is still alive.
 * @param[in] connection Abstraction Layer Logical Connection object.
 *
 * @return true if connection is still alive, else false.
 */
extern bool ndw_NATS_IsConnected(ndw_Connection_T* connection);

/**
 * @brief Inspect if a connection to NATS broker is closed.
 * @param[in] connection Abstraction Layer Logical Connection object.
 *
 * @return true if connection is closed, else false.
 */
extern bool ndw_NATS_IsClosed(ndw_Connection_T* connection);

/**
 * @brief Inspect if a connection to NATS broker is draining.
 * @param[in] connection Abstraction Layer Logical Connection object.
 *
 * @return true if connection is draining, else false.
 */
extern bool ndw_NATS_IsDraining(ndw_Connection_T* connection);

/**
 * @brief Publish a message to NATS borker.
 *
 * @return 0 on succees, else < 0.
 */
extern INT_T ndw_NATS_PublishMsg();

/**
 * @brief Subscribe for asynchronous notification from NATS broker. 
 *
 * @param[in] topic Abstraction Layer Logical Topic object.
 *
 * @return 0 on succees, else < 0.
 */
extern INT_T ndw_NATS_SubscribeAsync(ndw_Topic_T* topic);

/**
 * @brief Unsubscribe from a previous Subscription to NATS broker.
 *
 * @param[in] topic Abstraction Layer Logical Topic object.
 *
 * @return 0 on succees, else < 0.
 */
extern INT_T ndw_NATS_Unsubscribe(ndw_Topic_T* topic);

/**
 * @brief Subscribe synchronously to NATS broker, in preparing of polling for messages.
 *
 * @param[in] topic Abstraction Layer Logical Topic object.
 *
 * @return 0 on succees, else < 0.
 */
extern INT_T ndw_NATS_SubscribeSynchronously(ndw_Topic_T* topic);

/**
 * @brief Get number of queued messages on a given Topic.
 *
 * @param[in] topic Abstraction Layer Logical Topic object.
 * @param[out] Returns total number of queued messages.
 *
 * @return 0 on succees, else < 0.
 */
extern INT_T ndw_NATS_GetQueuedMsgCount(ndw_Topic_T* topic, ULONG_T* count);

// Returns 0 if no messages, 1 if there is a message, else on errors returns < 0.
/**
 * @brief Synchronously poll NATS broker and get a message back.
 * XXX: Need to implement batching of messages for extraction in bulk.
 *
 * @param[in] topic Abstraction Layer Logical Topic object.
 * @param[out] msg Message Pointer returned on receiving a message.
 * @param[out] msg_length Message length obtained of the current messages.
 * @param[in] timeout_ms Timeout to return control to invoker when no messages have arrived.
 * @param[out] dropped_msgs Returns total number of dropped messages.
 *
 * @return 0 if no messages, 1 if there is a message, else < 0 on errors.
 */
extern INT_T ndw_NATS_SynchronousPollForMsg(ndw_Topic_T* topic, const CHAR_T** msg, INT_T* msg_length, LONG_T timeout_ms, LONG_T* dropped_messages, void** vendor_closure);


extern INT_T ndw_NATS_CommitQueuedMsg(ndw_Topic_T* topic, void* vendor_closure);

extern INT_T ndw_NATS_CleanupQueuedMsg(ndw_Topic_T* topic, void* vendor_closure);

/**
 * @brief Shutdown NATS messaging system from a client application perspective.
 *
 */
extern void ndw_NATS_Shutdown();

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _NATS_IMPL_H */

