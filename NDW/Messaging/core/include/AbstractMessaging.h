
#ifndef _ABSTRACTMESSAGING_H
#define _ABSTRACTMESSAGING_H

#define _GNU_SOURCE     // for RTLD_DEFAULT
#include "ndw_types.h"

#include <dlfcn.h>

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <stddef.h>
#include <endian.h>
#include <pthread.h>
#include <stdatomic.h>

#include <cjson/cJSON.h>

#include "ndw_types.h"
#include "NDW_Utils.h"
#include "RegistryData.h"
#include "MsgHeaders.h"

/**
 * @file AbstractMessaging.h
 *
 * @brief This is the primary header file needed by application developers to interact with a messaging system.
 * This API provides an abstraction layer on top of one or more vendor messaging system(s).
 * The primary API/functions of concern for developers are:
 *
 * ndw_Init()                    - Initialize the messaging system data strucures.
 * ndw_Connect                   - Make a network or memory connection to a vendor messaging system.
 * ndw_Disconnect                - Disconnect from a previous connection make to a vendor messaging system.
 * ndw_CreateOutMsgCxt           - Create a header and a message body to populate contents prior to sending a message.
 * ndw_Publish                   - Publish a message, with header and message body, to an underlying vendor messaging system.
 * ndw_SubscribeAsync            - Subscribe to the messaging system to receive messages asynchronously.
 * ndw_SubscribeSynchronous      - Subscribe Synchronously to receive messages from a Topic.
 * ndw_SynchronousPollForMsg     - Poll the messaging system for the next message for a Topic.
 * ndw_ResponseForRequestMsg     - Send Response for Request message
 * ndw_GetResponseForRequestMsg  - Get Response for Request sent in timeout interval
 * ndw_Shutdown()            - Tear down Connections and Subscriptions to vendor messaging system and shut down communication.
 *
 * @author Andrena team member
 * @date 2025-07
 */

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/*
 * Environment Variable Expectations:
 * 1) NDW_APP_CONFIG_FILE - This environment variable points to a (JSON) config file.
 * 2) NDW_APP_DOMAINS - List of Domains (as in the config file above) to use each separated by a comma.
 * 3) NDW_APP_ID - An integer Unique Application Identifier.
 * 4) NDW_VERBOSE - 0 means terseness of output, greater than zero would lead to higher levels of verbosity.
 * 5) NDW_CAPTURE_LATENCY - 0 means off, 1 means on and capture latency information.
 */
#define NDW_APP_CONFIG_FILE "NDW_APP_CONFIG_FILE"
#define NDW_APP_DOMAINS "NDW_APP_DOMAINS"
#define NDW_APP_ID "NDW_APP_ID"
#define NDW_VERBOSE "NDW_VERBOSE"
#define NDW_DEBUG_MSG_HEADERS "NDW_DEBUG_MSG_HEADERS"
#define NDW_CAPTURE_LATENCY "NDW_CAPTURE_LATENCY"


/**
 * @var extern INT_T ndw_verbose
 * @brief Defines level of verbosity for logging.
 *
 * @note Higher the number, more it logs. Zero is minimal logging.
 */
extern INT_T ndw_verbose; // Setting this greater than zero will trigger verbose output.

/**
 * @var extern CHAR_T ndw_debug_msg_header
 * @brief Outputs debugging information when message headers are processed.
 *
 * @note Set it to be greater than zero.
 */

extern INT_T ndw_debug_msg_header; // Setting this greater than zero will trigger verbose output.


/**
 * @struct ndw_ErrorDetails_T
 * @brief Represents a rectangle with width and height.
 *
 * This structure is used to capture detailed error information.
 * This is per thread data structure.
 *
 * @note Note MT-safe data structure to introspect.
 * Not all values might filled in.
 * For example pointers can be NULL. Primitive data types default to zero.
 */
typedef struct ndw_ErrorDetails
{
    char* file_name;            // Source file name where error was generated.
    INT_T line_number;          // Source line number in the file.
    char* function_name;        // Source function name that generated the error.

    INT_T last_errno;           // Last known errno.
    INT_T function_return_code; // Function return code that lead to the error. 
    char* error_description;    // Error description.

    INT_T vendor_error_code;    // Messaging vendor specific error code.
    char* vendor_error_description; // Messaging vendor specific error description.
    bool is_error; // Flag to indicate that it is indeed an error.
} ndw_ErrorDetails_T;

extern ndw_ErrorDetails_T* ndw_GetErrorDetails();

/**
 * @struct ndw_BadMessage_T
 * @brief Asynchronous notification of a Bad message occurence.
 *
 * This structure is used to catpure as much details on a bad message as possible.
 * Some Pointers might be NULL. Primitive data types default to zero.
 *
 * @note The notification comes in a background safe.
 * But this strucure is per thread and hence MT-safe.
 * XXX: To be implemented.
 */
typedef struct ndw_BadMessage
{
    LONG_T received_time;           // Error time received in UTC.
    INT_T header_id;                // Message header id.
    INT_T header_size;              // Size of the header.
    UCHAR_T* msg_addr;              // Pointer to message body.
    INT_T msg_size;                 // Message body size.
    ndw_Topic_T* topic;             // Topic on which the bad message arrived.
    ndw_Connection_T* connection;   // Connection on which the bad message arrived.
    ndw_Domain_T* domain;           // Domain associated with the bad message.
    INT_T error_code;               // Error code, if any.
    const char* error_msg;          // Error message, if any.
    
} ndw_BadMessage_T;


/**
 * @def NDW_ASYNC_CALLBACK_FUNCTION_NAME
 * @brief The application is expected to have this function with the specified name..
 * That function in the application code will be invoked when a message arrives asynchronously.
 *
 * This macro defines the name of the function that is expected to be defined by the application code.
 */
#define NDW_ASYNC_CALLBACK_FUNCTION_NAME "ndw_HandleAsyncMessage"

/**
 * @typedef void (*ndw_AsyncCallbackPtr_T)(ndw_Topic_T*, void* opaque);
 * @brief This is the callback signature used to invoke the application layer
 * by this Abstract messaging layer.
 *
 * @param[in] topic Topic data structure.
 * @param[in] opaque not used at this point; could be NULL.
 */
typedef INT_T (*ndw_AsyncCallbackPtr_T)(ndw_Topic_T* topic, void* opaque);

/**
 * @def NDW_BAD_MESSAGE_FUNCTION_NAME
 * @brief The application is expected to have this function with the specified name..
 * That function in the application code will be invoked when a bad message arrives asynchronously.
 *
 * This macro defines the name of the function that is expected to be defined by the application code.
 */
#define NDW_BAD_MESSAGE_FUNCTION_NAME "ndw_HandleBadMessage"

/**
 * @typedef void (*ndw_BadMessageCallbackPtr_T)(ndw_Topic_T*, void* opaque);
 * @brief This is the callback signature used to invoke the application layer
 * by this Abstract messaging layer when a bad message arrives.
 *
 * @param[in] bad_msg The bad message pointer.
 */
typedef INT_T (*ndw_BadMessageCallbackPtr_T)(ndw_BadMessage_T* bad_msg);
extern ndw_BadMessageCallbackPtr_T ndw_bad_message_callback_ptr;


/**
 * @brief Print a bad message contents.
 *
 * @param[in] bad_msg Pointer to bad message. Message printed to error stream.
 *
 * @return None.
 */

extern void ndw_print_BadMessage(ndw_BadMessage_T* bad_msg);

/**
 * @typedef void (*ndw_GOAsyncMsgHandler)(ndw_Topic_T* topic)
 * @brief This is the callback signature used to invoke the GO language application layer
 * by this Abstract messaging layer.
 *
 * @param[in] topic Topic data structure.
 */
typedef void (*ndw_GOAsyncMsgHandler)(ndw_Topic_T* topic);
extern void ndwGoMessageHandler(ndw_Topic_T* topic);
extern void ndw_SetGoMessageHandler(ndw_GOAsyncMsgHandler handler);

#if 0
extern ndw_GOAsyncMsgHandler ndw_go_msghandler;
static inline ndw_GOAsyncMsgHandler getGoHandler() { return &ndwGoMessageHandler; }
#endif


/*
 * API: Application API functions to the Abstract Messaging Layer.
 */

/**
 * @brief The very first function to invoke. Initializes data structures.
 *
 * @return If 0 success, else failure.
 *
 * @note This does not make any connections to vendor messaging system.
 */
extern INT_T ndw_Init();

/**
 * @brief Every new thread created must call this function as we need
 *  to intialize per thread data structures.
 *
 * @return If 0 success, else failure.
 */
extern INT_T ndw_ThreadInit();

/**
 * @brief Every new thread created must call this function before thread exit
 *  so as to free per thread data structures.
 *
 * @return If 0 success, else failure.
 */
extern INT_T ndw_ThreadExit();

// Shutdown all vendor communications and the messaging system.

/**
 * @brief Shutdown all vendor connections and shutdown the system.
 *
 * @return None.
 *
 * @note It will also unsubscribe subscriptions before tearing down connections to the vendor systems.
 */
extern void ndw_Shutdown();

/**
 * @brief App Id is an integer variable. It should be in the environment variable named NDW_APP_ID.
 *
 * @return Returns the app id stored.
 *
 * @note
 * App ID is set on the header field for every message sent out.
 */
INT_T ndw_GetAppId();

/**
 * @var extern INT_T ndw_capture_latency
 * @brief Global variable. If set to > 0 then will start capturing latency metrics.
 *
 * @note XXX: To be implemented.
 */
extern INT_T ndw_capture_latency;

/**
 * @brief Load App Domains as specified in Configuration File.
 * Loads information on Domains, Connections and Topics.
 * The environment variable NDW_APP_DOMAINS should contain a comma separated list of Domains to load.
 *
 * @return 0 if success, else < 0.
 *
 * @note
 * The environment variable NDW_APP_DOMAINS should contain a comma separated list of Domains to load.
 */
extern INT_T ndw_LoadAppDomains();

/**
 * @brief Given a Domain and a Connection, connect to the Vendor messaging system.
 *
 * @param[in] domain The Domain containing the Connection.
 * @param[in] connection The Connection name used to connect to the Vendor messaging system.
 *
 * @return 0 if success else < 0
 */
extern INT_T ndw_Connect(const char* domain, const char* connection);

/**
 * @brief Given a Domain and a Connection, disconnect to the Vendor messaging system.
 *
 * @param[in] domain The Domain containing the Connection.
 * @param[in] connection The Connection name used to disconnect to the Vendor messaging system.
 *
 * @return 0 if success else < 0
 */
extern INT_T ndw_Disconnect(const char* domain, const char* connection);

/**
 * @brief Disconnect all connections for the given Domain.
 * Will unsubscribe existing subscriptions before disconnecting.
 * Will wait for connection to drain, if the vendor implementation supports that concept.
 *
 * @param[in] domain Name of the domain.
 *
 * @return A list of connection for which disconnect failed. The list is NULL terminated.
 *
 * @note Will unsubscribe existing subscriptions before disconnecting.
 * User should free the list returned, but NOT the contents of each element in the list.
 */
extern ndw_Connection_T** ndw_DisconnectAll(const char* domain);

/**
 * @brief Given a Domain and a Connection, return indicator if we still connected Vendor messaging system.
 *
 * @param[in] domain The Domain containing the Connection.
 * @param[in] connection The Connection name used to query if the connection is still connected.
 *
 * @return true if still connected else false.
 */
extern bool ndw_IsConnected(const char* domain, const char* connection);

/**
 * @brief Given a Domain and a Connection, return indicator if we the connection to the Vendor messaging system closed.
 *
 * @param[in] domain The Domain containing the Connection.
 * @param[in] connection The Connection name used to query if the connection is closed.
 *
 * @return true if connection is closed, else false.
 */
extern bool ndw_IsClosed(const char* domain, const char* connection);

/**
 * @brief Given a Domain and a Connection, return indicator if we the connection is closing and draining.
 *
 * @param[in] domain The Domain containing the Connection.
 * @param[in] connection The Connection name used to query if the connection is closing and draining.
 *
 * @return true if connection is closing and draining, else false.
 *
 * @note Some network connections take time draining before closing.
 */
extern bool ndw_IsDraining(const char* domain, const char* connection);

/**
 * @brief For a given domain initiate connections to one or more vendor specific messaging system.
 *
 * @param[in] domain
 *
 * @return 0 if success, else < 0
 *
 * @note
 * Domain contains a list of Connections and each Connection contains a list of Topics.
 */
extern INT_T ndw_ConnectToDomain(const char* domain);

/**
 * @brief For a list of domains initiate connections to one or more vendor specific messaging system(s).
 * Loops through each domain and each connection for a domain and initiate connection to vendor messaging system(s).
 * to vendor specific messaging systems.
 *
 * @param[in] domains List of domains.
 *
 * @return 0 if success, else < 0
 *
 * @note
 * The domains parmeter is a list that is expected to have the last element as NULL.
 */
extern INT_T ndw_ConnectToDomains(const char** domains);

/**
 * @brief Given a domain, connect to all vendor messaging systems for each connection in that domain.
 * Loops through each connection for a domain and initiaties connection to each vendor messaging system.
 *
 * @param[in] domain The domain which has a list of connections.
 *
 * @return a list of failed connections. Returns NULL if no failures.
 *
 * @note
 * The return list contains a list of failed connections.
 * The caller would have to free the list, but NOT the contents of each element in the list.
 */
extern ndw_Connection_T** ndw_ConnectAll(const char* domain);

/**
 * @brief Before sending a message get back a ndw_OutMsgCxt_T to populate header and message body.
 *
 * @param[in] topic The topic objec to which to send the message.
 * @param[in] header_id The version of the header being used.
 * @param[in] msg_encoding_format Specify content format of message body (JSON, XML, binary, etc.)
 * @param[in] msg App message body that will be copied to this object. One can use the memory also allocated to build the content.
 * @param[in] msg_size Message body size. For strings factor in NULL byte.
 *
 * @return ndw_OutMsgCxt_T structure that holds header and message body.
 *
 * @note
 * The ndw_OutMsgCxt_T returned is per thread and MT-safe.
 */
ndw_OutMsgCxt_T* ndw_CreateOutMsgCxt(ndw_Topic_T* topic,
                    INT_T header_id, INT_T msg_encoding_format, UCHAR_T* msg, INT_T msg_size);

/**
 * @brief Publish a message. The header and message body should be in ndw_OutMsgCxt_T data structure.
 *
 * @return 0 if successful, else < 0.
 *
 * @note The reason there is no parameter to this function is because the message body and content
 * should be in ndw_OutMsgCxt_T, and this data structure is per thread.
 * @see ndw_OutMsgCxt_T data structure.
 */
extern INT_T ndw_PublishMsg();

/**
 * @brief Subscribe for Asynchronous message notification on a Topic.
 *
 * @param[in] topic_name The name of the topic.
 *
 * @return Returns 0 on success, else < 0.
 *
 * @note All aysnchronous messages land up in the function defined by the macro NDW_ASYNC_CALLBACK_FUNCTION_NAME.
 * @see See macro NDW_ASYNC_CALLBACK_FUNCTION_NAME for name of function where aysnchronous messages are delivered.
 */
extern INT_T ndw_SubscribeAsyncToTopicName(char* topic_name);

/**
 * @brief Subscribe for asynchronous notification of messages given an array of topic names.
 *
 * @param[in] topic_names An array of topic names to subscribe to.
 *
 * @return 0 on success, or < 0 indicating how many failed.
 *
 * @note The array should have an element at the end with a NULL pointer.
 * @see See macro NDW_ASYNC_CALLBACK_FUNCTION_NAME for name of function where aysnchronous messages are delivered.
 */
extern INT_T ndw_SubscribeAsyncToTopicNames(char** topic_names);

/**
 * @brief Summary.
 *
 * @param[in] topic Data structure of the Topic on which to unsubscribe for asynchronous notifications.
 *
 * @return 0 on success, or < 0 indicating how many failed.
 *
 * @note The array should have an element at the end with a NULL pointer.
 * @see See macro NDW_ASYNC_CALLBACK_FUNCTION_NAME for name of function where aysnchronous messages are delivered.
 */
extern INT_T ndw_SubscribeAsync(ndw_Topic_T* topic);

/**
 * @brief Summary.
 *
 * @param[in] topic Data structure of the Topic on which to unsubscribe for notifications.
 *
 * @return 0 on success, else < 0.
 */
extern INT_T ndw_Unsubscribe(ndw_Topic_T* topic);

/**
 * @brief Subscribe to a Topic for Synchronous (polling) of messages.
 *
 * @param[in] topic Name of the topic to subscribe for synchronous polling of mssages.
 *
 * @return 0 on success, else < 0.
 */
extern INT_T ndw_SubscribeSynchronouslyWithTopicName(const CHAR_T* topic_name);

/**
 * @brief Subscribe to a Topic for Synchronous (polling) of messages.
 *
 * @param[in] topic Topic data structure used to subscribe for synchronous polling of messages.
 *
 * @return 0 on success, else < 0.
 */
extern INT_T ndw_SubscribeSynchronous(ndw_Topic_T* topic);

/**
 * @brief Synchronously poll for messages with a time out.
 * XXX: Need to support polling for a batch of messages, even if it might not be suppported by underlying vendor implementation.
 *
 * @param[in] topic Topic data structure on which to synchronously poll for a message.
 * @param[in] timeout_ms Timeout in milliseconds to wait for messages to appear.
 * @param[out] dropped_messages Pointer to number of dropped messages that would be returned. 
 *
 * @return 0 on success, else < 0.
 *
 * @note
 */
extern INT_T ndw_SynchronousPollForMsg(ndw_Topic_T* topic, LONG_T timeout_ms, LONG_T* dropped_messages);

/**
 * @brief Publish Response Msg with NAT InBox as Subject for Request
 *
 * @param[in] topic Topic data structure on which to synchronously poll for a message.
 *
 * @return 0 on success, else < 0.
 *
 * @note
 */
extern INT_T ndw_Publish_ResponseForRequestMsg(ndw_Topic_T* topic);

/**
 * @brief Get Response Message from Request Message sent within timeout interval
 *
 * @param[in] topic Topic data structure on which to synchronously poll for a message.
 * @param[in] timeout_ms Timeout in milliseconds to wait for messages to appear.
 *
 * @return 0 on success, else < 0.
 *
 * @note
 */
extern INT_T ndw_GetResponseForRequestMsg(ndw_Topic_T* topic, LONG_T timeout_ms);

/**
 * @brief Commit the last message received and processed.
 * Does not matter if it is an asynchronous or a synchronous message.
 *
 * @param[in] topic Topic data strucure on which the last message arrrived.
 *
 * @return 0 on success, else < 0.
 */
extern INT_T ndw_CommitLastMsg(ndw_Topic_T* topic);

/**
 * @brief Get number of queued up messages on a topic. 
 *
 * @param[in] topic Topic data strucures on which to find out the number of queued messages.
 * @param[out] count Pointer to the field where the number of queued messages will be stored.
 *
 * @return 0 on success, else < 0.
 */
extern INT_T ndw_GetQueuedMsgCount(ndw_Topic_T* topic, ULONG_T* count);

// Print Statistics on Domains, Connections and Topics.

/**
 * @brief Print messaging statistics for all connections and for all topics for a list of domains.
 * @param[in] domains Array of domain names.
 * @return None.
 * @note the parameter array should be NULL terminated.
 *
 */
extern void ndw_PrintStatsForDomains(const char** domains);

/**
 * @brief Print messaging statistics for all Connections on all Topics for a given Domain name.
 * @param[in] domain Name of Domain for which to print statistics.
 * @return None.
 */
extern void ndw_PrintStatsForDomainByName(const char* domain);

/**
 * @brief Print messaging statistics for all connections of a particular domain
 * @param[in] domain Pointer to a Domain structure.
 * @return None.
 */
extern void ndw_PrintStatsForDomain(ndw_Domain_T* domain);

/**
 * @brief Print messaging statistics for a particular connection.
 * @param[in] domain Pointer to a Connection structure.
 * @return None.
 */
extern void ndw_PrintStatsForConnection(ndw_Connection_T* connection);

/**
 * @brief Print messaging statistics for a particular topic.
 * @param[in] domain Pointer to a Topic structure.
 * @return None.
 */
extern void ndw_PrintStatsForTopic(ndw_Topic_T* topic);

/*
 * Function Scope # 2: Vendor Implementations to invoke these following functions.
*/


/**
 * @brief Vendor implementations invoke this function for all asynchronous incoming messages.
 *
 * @param[in] topic The Topic structure for which the message came in.
 * @param[in] msg Pointer to the incoming message. Typically allocated by the vendor implementation.
 * @param[in] msg_size Sizeo of message. Includes both header and message body size.
 *
 * @return Returns 0 on succces, else < 0.
 *
 * @note Do not free the msg. That is reponsiblity of the abstraction and/or vendor implementation layer.
 */
extern INT_T ndw_HandleVendorAsyncMessage(ndw_Topic_T* topic, UCHAR_T* msg, INT_T msg_size, void *vendor_closure);

extern INT_T ndw_PollAsyncQueue(ndw_Topic_T* topic, LONG_T timeout_us);
extern INT_T ndw_CommitAsyncQueuedMessge(ndw_Topic_T* topic);

/**
 * @def NDW_LOGTOPICMSG
 * @brief This function log a diagnostic message for a Topic to output stream.
 * Very useful for debugging since it puts out Filename, Line number and function names along with details of the Topic.
 *
 * @param[in] __FILE__ This is generated by the compiler.
 * @param[in] __LINE__ This is generated by the compiler.
 * @param[in] CURRENT_FUNCTION This is generated by the compiler.
 * @param[in] stream The file stream to log the message to.
 * @param[in] topic The Topic data structure for which to log the Topic Details.
 *
 */
void ndw_LogTopicMsg(const char* filename, INT_T line_number, const char* function_name,
                        FILE* stream, const char* msg, ndw_Topic_T* t);

/**
 * @def NDW_LOGTOPICMSG
 * @brief This macro is ued to log diagnostic message for a Topic to output stream.
 * Very useful for debugging since it puts out Filename, Line number and function names along with details of the Topic.
 *
 * @param[in] __FILE__ This is generated by the compiler.
 * @param[in] __LINE__ This is generated by the compiler.
 * @param[in] CURRENT_FUNCTION This is generated by the compiler.
 * @param[in] ndw_out_file The output file stream to log the message to.
 * @param[in] topic The Topic data structure for which to log the Topic Details.
 *
 */
#define NDW_LOGTOPICMSG(msg, topic)     \
    ndw_LogTopicMsg(__FILE__, __LINE__, CURRENT_FUNCTION, ndw_out_file, msg, topic)

/**
 * @def NDW_LOGTOPICMSG
 * @brief This macro is ued to log diagnostic message for a Topic to error stream.
 * Very useful for debugging since it puts out Filename, Line number and function names along with details of the Topic.
 *
 * @param[in] __FILE__ This is generated by the compiler.
 * @param[in] __LINE__ This is generated by the compiler.
 * @param[in] CURRENT_FUNCTION This is generated by the compiler.
 * @param[in] ndw_err_file The file error stream to log the message to.
 * @param[in] topic The Topic data structure for which to log the Topic Details.
 *
 */
#define NDW_LOGTOPICERRMSG(msg, topic)  \
    ndw_LogTopicMsg(__FILE__, __LINE__, CURRENT_FUNCTION, ndw_err_file, msg, topic)

extern void nats_clearLastError(void);

#ifdef __cplusplus
}

#endif /* __cplusplus */

#endif /* _ABSTRACTMESSAGING_H */

