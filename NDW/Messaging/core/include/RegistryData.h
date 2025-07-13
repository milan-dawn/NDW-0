
#ifndef _REGISTRYDATA_H
#define _REGISTRYDATA_H

#include "ndw_types.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <cjson/cJSON.h>
#include <string.h>
#include <endian.h>
#include <pthread.h>
#include <time.h>

#include "ndw_types.h"
#include "NDW_Utils.h"
#include "QueueImpl.h"

/*
 * uthash User Guide: https://troydhanson.github.io/uthash/userguide.html
 */
#include "uthash.h"

/**
 * @file RegistryData.h
 * @brief This is the header file needed to load Registry data.
 *
 * There are 3 primary data structures:
 * ndw_Domain_T - Domain structure. This holds a list of Connections.
 * ndw_Connection_T - Connection data structure. Encapsulates an abstract connection. Holds a list of Topics.
 * ndw_Topic_T - Topic data structure. Encapsulates enough information to interact with a vendor pub-sub messaging system.
 *
 * @author Andrena team member
 * @date 2025-07
 */

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

#include "uthash.h"

typedef struct ndw_Connection ndw_Connection_T;
typedef struct ndw_Topic ndw_Topic_T;
typedef struct ndw_Domain ndw_Domain_T;
typedef struct ndw_DomainHandle ndw_DomainHandle_T;

/**
 * @struct ndw_Topic_T
 * @brief Abstraction to enable encapsulate of a vendor pub-sub messaging Topic. Used for both pub and sub operations.
 * Topics are often referred to as Subjects as well on some messaging system.
 *
 * @note 
 * This is the primary data structure that application developer uses to interact with the abstract messaging layer.
 * Use this for both pub and sub operations.
 */
typedef struct ndw_Topic
{
    bool disabled;                          // Is topic enabled or disabled?
    bool is_pub_enabled;                    // Is publish operation enabled on this topic?
    bool is_sub_enabled;                    // Is Subscribe operation enabled on this topic?
    bool durable_topic;                     // Is this topic for a durable subscription?
    CHAR_T* topic_description;              // Comments and description of this topic.
    INT_T topic_unique_id;                  // Unique identifier for this topic.
    CHAR_T* topic_unique_name;              // Unique name for this topic.

    CHAR_T* pub_key;                        // Publish Key to use: If not specified defeaults to topic_unique_name.
    CHAR_T* sub_key;                        // Subscription key to use: If not specified defeaults to topic_unique_name.

    bool q_async_enabled;                   // Is Ansynchronous message queue enabled?
    char* q_async_name;                     // Asynchronous queue name, if any.
    int q_async_size;                       // Asynchronous queue size, if configured.
    NDW_Q_T* q_async;                       // Queue where asynchronous data lands up
    void* q_async_closure;                  // Queue Closure which holds a Queued Item.

    UT_hash_handle hh_topic_id;             // hash handle for ID-based hash
    UT_hash_handle hh_topic_name;           // hash handle for name-based hash

    CHAR_T* debug_desc;                     // Holds all information to debug attributes of a Topic that can be logged.

    ndw_Connection_T* connection;           // Back reference to ndw_Connecion_T
    ndw_Domain_T* domain;                   // Back reference to ndw_Domain_T
    ndw_DomainHandle_T* domain_handle;      // Back-back reference to ndw_DomainHandle_T

    // XXX: Latency capture to be implemented.
    ndw_LatencyBuckets_T latency_bucket;    // Latency bucket information for published and subscribed messages.
    void* app_opaque;                       // Opaque data app can store per topic.
    void* ndw_opaque;                       // Opaque data the NDW needs to store.
    void* vendor_opaque;                    // Vendor specific implementation data structure

    CHAR_T* topic_options;                  // List of Topic options.
    NDW_NVPairs_T topic_options_nvpairs;    // Name value data structure to hold Topic options.

    CHAR_T* vendor_topic_options;           // List of Topic options revelant for the Vendor implementation.
    NDW_NVPairs_T vendor_topic_options_nvpairs; // Name value data structure to hold Topic options revelant to Vendor code.

    LONG_T sequence_number;                 // A sequencer number app can use to reset and increment.
    LONG_T total_published_msgs;            // Total published messages on this Topic while the application was running.
    LONG_T total_received_msgs;             // Total received messages on this Topic while the application was running.
    LONG_T total_bad_msgs_received;         // Total number of bad messages received on this Topic.

    LONG_T last_msg_received_time;          // Last received message timestamp in UTC.
    UCHAR_T* last_msg_header_received;      // Last received message's message header Pointer.
    UCHAR_T* last_msg_received;             // Last received message's message body Pointer.
    INT_T last_msg_received_size;           // Total number of bad messages received on this Topic.
    void* last_msg_closure;                 // For last message it is an opaque pointer.
    void* last_msg_vendor_closure;          // For last message it is a vendor opaque pointer.
    
    // IPv4 (15 bytes), IPv6 (39 bytes), IPv6 with scope zone (46 bytes). So let use 60.
    CHAR_T  last_received_ip_address[60];               // IP address for which the message arrived.
    ULONG_T last_received_durable_ack_sequence;         // For durable subjects the last ack sent by vendor messaging system.
    ULONG_T last_received_durable_ack_global_sequence;  // For durable subject the global ack sequencer number.
    ULONG_T last_received_durable_total_delivered;      // Total number of durable messages delivered to the application.
    ULONG_T last_received_durable_total_pending;        //  Total number of pending messages that should be delivered.
    LONG_T msgs_commit_count;                          // Number of commits of messages received.
    LONG_T msgs_ack_count;                             // Number of commits that resulted in acks.
    LONG_T msgs_failed_ack_count;                      // Number of commits that resulted in failed acks.

    bool synchronous_subscription;                      // Boolean indicator if this is a synchronous subject subscription.

} ndw_Topic_T;

/**
 * @struct ndw_Connection_T
 * @brief Abstraction for physical connection to a vendor messaging system.
 *
 * @note 
 * Use this object for connecting and disconnecting to a vendor messaging system.
 */
typedef struct ndw_Connection
{
    bool disabled;                          // Connection access is enabled or disabled?
    CHAR_T *connection_unique_name;         // Connection unique name.
    INT_T connection_unique_id;             // Connection unique identifier.
    CHAR_T *vendor_name;                    // Name of the messaging vendor.
    INT_T vendor_id;                        // Unique idenifier for the messaging vendor.
    INT_T vendor_logical_version;           // Logical version number of the vendor message product.
    CHAR_T *vendor_real_version;            // Real version number of the vendor message product.
    INT_T tenant_id;                        // Tentant identifier. Used for security reasons.
    CHAR_T *connection_url;                 // Vendor connection URL.
    CHAR_T *connection_comments;            // Comments for the vendor connection usage.

    CHAR_T* debug_desc;                     // Connection debugging information that can be put out to a log stream.

    ndw_Topic_T *topics_by_id;              // Topics are kept in a hashtable by keyed Topic unique identifer.
    ndw_Topic_T *topics_by_name;            // Topics are also kept in another hashtable keyed by Topic unique name.

    UT_hash_handle hh_connection_name;      // hash handle for topic unique name.
    UT_hash_handle hh_connection_id;        // hash handle for topic unique identifier.

    ndw_Domain_T* domain;                   // Back reference to ndw_Domain_T
    ndw_DomainHandle_T* domain_handle;      // Back-back reference to ndw_DomainHandle_T
    
    CHAR_T* vendor_connection_options;      // Vendor connection options.
    NDW_NVPairs_T vendor_connection_options_nvpairs; // Name value pairs hold vendoring connection options.

    void* app_opaque;                       // Opaque data app can store per connection.
    void* ndw_opaque;                       // Opaque data the NDW needs to store.
    void* vendor_opaque;                    // Vendor specific implementation data structure.

} ndw_Connection_T;

/**
 * @struct ndw_Domain_T
 * @brief Domain data struture.
 * A domain holds a list of connections for various vendor messaging system(s).
 * A connection in turn holds a list of topics for pub and sub messaging operations.
 */
typedef struct ndw_Domain
{
    CHAR_T *domain_name;                    // Domain unique name.
    INT_T domain_id;                        // Domain unique identifier.
    CHAR_T *domain_description;             // Domain comments or description.

    CHAR_T* debug_desc;

    ndw_Connection_T *connections_by_name; // Hashtable keyed by Connection unique name.
    ndw_Connection_T *connections_by_id;   // Hashtable keyed by Connection unique identifier.

    UT_hash_handle hh_domain_name;          // Hashtable for multiple domains keyed by domain name.
    UT_hash_handle hh_domain_id;            // Hashtable for multple domains keye by domain identifier.

    ndw_DomainHandle_T* domain_handle;      // Back reference to Domain structure. XXX: Review needed for usage.

    void* app_opaque;                       // Opaque data app can store per connection.
    void* ndw_opaque;                       // Opaque data the NDW needs to store.
    void* domain_impl;                      // Vendor specific implementation data structure
} ndw_Domain_T;

/**
 * @struct ndw_DomainHandle_T
 * @brief Used to keep a map of domains.
 */
typedef struct ndw_DomainHandle
{
    ndw_Domain_T *g_domains_by_name;        // List of domains by domain name.
    ndw_Domain_T *g_domains_by_id;          // List of domains by domain identifier.
    pthread_t creator_thread_id;            // Thread that created this data structure.
} ndw_DomainHandle_T;

/**
 * @var extern ndw_tls_domain_handle
 * @brief Thread local usage fordomain map.
 *
 * XXX: Review for usage.
 */
extern pthread_key_t    ndw_tls_domain_handle;

/**
 * @var extern ndw_g_domain_handle.
 * @brief Global handle for map of all domains.
 *
 * XXX Review for usage.
 */
extern ndw_DomainHandle_T* ndw_g_domain_handle;

/**
 * @brief Initialize the Registry that holds data structures for Domains, Connections and Topics.
 * A Domain has a list of Connections.
 * A Connection has a list of Topics for pub-sub operation.
 *
 * @return 0 on success, else < 0.
 *
 * @note Application code should NOT call this.
 */
extern INT_T ndw_InitializeRegistry();

/**
 * @brief Clean up all data structures of the Registry.
 *
 * @return 0 on success, else < 0.
 * @note Application code should NOT call this.
 */
extern INT_T ndw_CleanupRegistry(); // NOTE: Invoke this when done with messaging!

/**
 * @brief Get the global data structure that holds the hashtable for all Domains.
 *
 * XXX: Review usage of this.
 *
 * @return Returns the global data structure that holds the hashtable for all Domains.
 */
extern ndw_DomainHandle_T* ndw_GetDomainHandle();

/*
 * @brief Given a JSON file name path laod definitions of Domains, Connections and Topics and create data structures.
 *
 * @param[in] jsonfilenamepath JSON file path name. File holds configurations for Domains, Connections and Topics.
 * @param[in] domain_names Array of Domain names for which to load configurations and create data structures.
 *
 * @return 0 on success, else < 0.
 */
extern INT_T ndw_LoadDomains(const CHAR_T* jsonfilenamepath, CHAR_T** domain_names);

/**
 * @brief Given a Domain identifier return Domain data structure.
 *
 * @param[in] domain_id Domain identifier.
 *
 * @return Domain data structure Pointer (ndw_Domain_T).
 */
extern ndw_Domain_T* ndw_GetDomainById(INT_T domain_id);

/**
 * @brief Given a Domain name return Domain data structure Pointer.
 *
 * @param[in] domain_name Domain name.
 *
 * @return Domain data structure Pointer (ndw_Domain_T).
 */
extern ndw_Domain_T* ndw_GetDomainByName(const CHAR_T* domain_name);

/**
 * @brief Given a Domain name and a Connection name return Connection data structure Pointer.
 *
 * @param[in] domain_name Domain name.
 * @param[in] connection_name Connection name.
 *
 * @return Connection data structure Pointer (ndw_Connection_T).
 */
extern ndw_Connection_T* ndw_GetConnectionByName(const CHAR_T* domain_name, const CHAR_T* connection_name);

/**
 * @brief Given a Domain data structure Pointer and a Connection name return the Connection data structure Pointer.
 *
 * @param[in] domain Domain data structure Pointer.
 * @param[out] connection_name Unique Connection name.
 *
 * @return Connection data structure Pointer (ndw_Connection_T).
 */
extern ndw_Connection_T* ndw_GetConnectionByNameFromDomain(ndw_Domain_T* domain, const CHAR_T* connection_name);

/**
 * @brief Given a Domain data structure Pointer and a Connection idenitifier return the Connection data structure Pointer.
 *
 * @param[in] domain Domain data structure Pointer.
 * @param[out] connection_id Unique Connection idenitifier.
 *
 * @return Connection data structure Pointer (ndw_Connection_T).
 */
extern ndw_Connection_T* ndw_GetConnectionByIdFromDomain(ndw_Domain_T* domain, INT_T connection_id);

/**
 * @brief Given a Connection data structure Pointer and a Topic name return the Topic data structure Pointer.
 *
 * @param[in] connection Connection data structure Pointer.
 * @param[in] topic_name Unique Name of the Topic.
 *
 * @return Topic data structure Pointer (ndw_Topic_T).
 */
extern ndw_Topic_T* ndw_GetTopicByNameFromConnection(ndw_Connection_T* connection, const CHAR_T* topic_name);

/**
 * @brief Given a Connection data structure Pointer and a Topic identifier return the Topic data structure Pointer.
 *
 * @param[in] connection Connection data structure Pointer.
 * @param[in] topic_id Unique Topic identifier
 *
 * @return Topic data structure Pointer (ndw_Topic_T).
 */
extern ndw_Topic_T* ndw_GetTopicByIdFromConnection(ndw_Connection_T* connection, INT_T topic_id);



/**
 * @brief Given a Domain data structure Pointer return a list of Connection data structures.
 *
 * @param[in] domain Domain data structure Pointer.
 * @param[out] total_connections Inserts the total number of Connections for the given Domain.
 *
 * @return A list of Connections (ndw_Connection_T objects).
 *
 * @note
 * Caller has to free the returned list but NOT the content of each item in the list list.
 *
 */
extern ndw_Connection_T** ndw_GetAllConnectionsFromDomain(ndw_Domain_T* domain, INT_T* total_connections);

/**
 * @brief Given a Connection data structure Pointer return a list of Topic data structures.
 *
 * @param[in] domain Domain data structure Pointer.
 * @param[out] total_topics Inserts the total number of Topics for the given Connection.
 *
 * @return A list of Topics (ndw_Topic_T objects).
 *
 * @note
 * Caller has to free the returned list but NOT the content of each item in the list.
 */
extern ndw_Topic_T** ndw_GetAllTopicsFromConnection(ndw_Connection_T* connection, INT_T* total_topics);

/**
 * @brief Get list of all Domains loaded and configured.
 *
 * @param[out] total_domains Inserts the total number of domains that have been configured and loaded.
 *
 * @return a list of Domains (ndw_Domain_T data structure).
 *
 * @note Caller has to free the returned list but NOT the contenct of each item in the list.
 */
extern ndw_Domain_T** ndw_GetAllDomains(INT_T* total_domains);

/**
 * @brief Print out contents of a Topic in JSON format.
 * @param[in] Topic for which we need to print the contents in JSON format.
 * @param[in] indent Number of white spaces for each nested JSON node.
 *
 * @return None.
 */
extern void ndw_PrintTopicJson(ndw_Topic_T *topic, INT_T indent);

/**
 * @brief Print out contents of a Connection in JSON format.
 * @param[in] conn Connection for which we need to print the contents in JSON format.
 * @param[in] indent Number of white spaces for each nested JSON node.
 *
 * @return None.
 */
extern void ndw_PrintConnectionJson(ndw_Connection_T *conn, INT_T indent);

/**
 * @brief Print out contents of a Domain in JSON format.
 * @param[in] Domain for which we need to print the contents in JSON format.
 * @param[in] indent Number of white spaces for each nested JSON node.
 *
 * @return None.
 */
extern void ndw_PrintDomain_Json(ndw_Domain_T *domain, INT_T indent);

/**
 * @brief Validates the conent of the Registry and the data structures created.
 *
 * @return None.
 */
extern void ndw_DebugAndPrintDomainConfigurations();

/**
 * @brief Given the full path of a Topic in the heirarchy, return the Topic data structure.
 *
 * @param[in] path full path of Topic hierarchy. Example: Domain^Connection^Topic.
 *
 * @return Topic (ndw_Topic_T) data structure.
 */
extern ndw_Topic_T* ndw_GetTopicFromFullPath(const CHAR_T* path);

/**
 * @brief Give a full path name of a Topic split it into its constituents parts, namely, Domain, Connection and Topic.
 *
 * @param[in] topic_path. Full path of Topic. Example: Domain^Connection^Topic.
 * @param[out] Number of items in the returned list. Should be equal to 3.
 *
 * @return On success a list of 3 items. First item is name of Domain, second is name of Connection, and third is name of Topic.
 *
 * @note
 * After one is done with this array, invoke ndw_GetDomainConnectionTopicNames to free contents and the Pointer return.
 *
 * @see ndw_GetDomainConnectionTopicNames
 *
 */
extern CHAR_T** ndw_GetDomainConnectionTopicNames(const CHAR_T* topic_path, INT_T* length);

/**
 * @brief Assuming it is a list of 3 items, Domain name, Connection name and Topic name, free both contents and Pointer to list.
 *
 * @param[in] topic_path. Full path of Topic. Example: Domain^Connection^Topic.
 *
 */
extern void ndw_free_domain_connection_topic_names(CHAR_T** topic_path);

extern void ndw_FreeDomainConnectionTopicNames(CHAR_T** topic_path);

/*
*/
/**
 * @brief Summary Given a topic full path name, return its corresponding Connection and Domain Pointer references.
 *
 * @param[in] topic_path. Full path of Topic. Example: Domain^Connection^Topic.
 * @param[out] p_domain Stores the pointer to the Domain data structure if found.
 * @param[out] p_connection Stores the pointer to the Connection data structure if found.
 * @param[out] p_topic Stores the pointer to the Topic data structure if found.
 *
 * @return -1  if Domain lookup failed.
 * Returns -2 if Connection lookup failed.
 * Returns -3 if Topic lookup failed.
 * Else returns 0.
 *
 * @note Do NOT free any of the Pointers returned.
 */
extern INT_T ndw_GetDomainConnectionTopicFromPath(const CHAR_T* topic_path, ndw_Domain_T** p_domain, ndw_Connection_T** p_connection, ndw_Topic_T** p_topic);

/*
 * Returns 0 if both connection and domain pointers are set else < 0.
 */

/**
 * @brief * This method will return Connection and Domain pointers.
 *  If force_exit is set and Connection and Domain pointers are NOT found,
 *  it will treat it as a serious issue and exit forcefully!
 *
 * @param[in] method_name Name of the invoking method. 
 * @param[in] force_exit If force_exit is set and Connection and Domain pointers are NOT found,
 * @param[out] p_topic Stores the pointer to the Topic data structure if found.
 * @param[out] p_connection Stores the pointer to the Connection data structure if found.
 * @param[out] p_domain Stores the pointer to the Domain data structure if found.
 *
 * @return -1  if Domain lookup failed.
 * Returns -2 if Connection lookup failed.
 * Returns -3 if Topic lookup failed.
 * Else returns 0.
 */
extern INT_T ndw_GetConnectionDomain(const CHAR_T* method_name, bool force_exit,
                                    ndw_Topic_T* topic, ndw_Connection_T** connection, ndw_Domain_T** domain);



/**
 * @brief Cleans up all data structures for Domains, Connections and frees them.
 *
 * @return None.
 */
extern void ndw_CleanupAllDomainsConnectionsTopics();

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _REGISTRYDATA_H */

