
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "uthash.h"

#include "RegistryData.h"

ndw_DomainHandle_T* domain_handle = NULL; // global Domain Handle. Set once.

INT_T
ndw_InitializeRegistry()
{
    return 0;

} // end method ndw_InitializeRegistry

INT_T
ndw_CleanupRegistry()
{
    ndw_DomainHandle_T* dh = domain_handle;
    if (NULL != dh) {
        if (NULL != dh->g_domains_by_name) {
            ndw_CleanupAllDomainsConnectionsTopics();
        }
        dh->g_domains_by_id = NULL;
        dh->g_domains_by_name = NULL;
        free(dh);
    }

    domain_handle = NULL;

    return 0;

} // end method ndw_CleanupRegistry

ndw_DomainHandle_T*
ndw_GetDomainHandle()
{
    ndw_DomainHandle_T* dh = domain_handle;
    if (NULL == dh) {
        NDW_LOGERR( "*** WARNING: *** get_domain_handle from TLS is NULL!\n");
    }

    return dh;
} // end method ndw_GetDomainHandle

static ndw_DomainHandle_T*
ndw_GetDomainHandleInternal()
{
    ndw_DomainHandle_T* dh = ndw_GetDomainHandle();
    if (NULL == dh) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_GetDomainHandle from TLS is NULL!\n");
        exit(EXIT_FAILURE);
    }

    return dh;

} // end method ndw_GetDomainHandle

static CHAR_T*
ndw_GetJsonItem(cJSON* root, const CHAR_T* item, bool mandatory)
{
    if (NULL == root) {
        NDW_LOGERR("*** FATAL ERROR: NULL CJSON* root parameter!\n");
        exit(EXIT_FAILURE);
    }

    if ((NULL == item) || ('\0' == *item)) {
        CHAR_T* json_string = cJSON_Print(root);
        NDW_LOGERR("*** FATAL ERROR: NULL item parameter for cJSON object: <%s>!\n",
                    ((NULL == json_string) ? "?" : json_string));
        free(json_string);
        exit(EXIT_FAILURE);
    }

    CHAR_T* value = NULL;
    cJSON* description_item = cJSON_GetObjectItem(root, item);
    if (NDW_ISNULLCHARPTR(item) || (NULL == description_item) || NDW_ISNULLCHARPTR(description_item->valuestring)) {
        if (mandatory) {
            CHAR_T* json_string = cJSON_Print(root);
            NDW_LOGERR("*** FATAL ERROR: NULL item parameter for item<%s> for cJSON object: <%s>!\n",
                        item, ((NULL == json_string) ? "?" : json_string));
            free(json_string);
            exit(EXIT_FAILURE);
        }
    }
    else
    {
        value = strdup(description_item->valuestring);
    }

    return value;

} // end method ndw_GetJsonItem

static void
ndw_SetConnectionDebugDesc(cJSON* conn_obj, ndw_Connection_T* conn)
{
    free(conn->debug_desc);
    ndw_Domain_T* domain = conn->domain;
    const CHAR_T* disabled_connection = conn->disabled ? " <= *DISABLED*" : "";
    CHAR_T* id_as_string = cJSON_PrintUnformatted(cJSON_GetObjectItem(conn_obj, "ConnectionUniqueID"));
    conn->debug_desc = ndw_ConcatStrings("Connection<", id_as_string, ", ",
        conn->connection_unique_name, disabled_connection, ", ", conn->vendor_name, "> ", domain->debug_desc, NULL);
    free(id_as_string);
} // end method ndw_SetConnectionDebugDesc

INT_T
ndw_LoadDomains(const CHAR_T *jsonfilenamepath, CHAR_T **domain_names)
{
    if (NULL != domain_handle) {
        return -1;  // Already set. This can be called only once!
    }

    domain_handle = calloc(1, sizeof(ndw_DomainHandle_T));
    domain_handle->creator_thread_id = pthread_self();

    FILE *file = fopen(jsonfilenamepath, "rb");
    if (!file) {
        NDW_LOGERR( "Error opening file: %s\n", jsonfilenamepath);
        return -2;
    }

    fseek(file, 0, SEEK_END);
    LONG_T length = ftell(file);
    fseek(file, 0, SEEK_SET);

    CHAR_T *json_data = (CHAR_T *)malloc(length + 1);
    if (!json_data) {
        NDW_LOGERR( "Memory allocation failed\n");
        fclose(file);
        return -3;
    }

    fread(json_data, 1, length, file);
    json_data[length] = '\0';
    fclose(file);

    cJSON *root = cJSON_Parse(json_data);
    free(json_data);
    if (!root) {
        NDW_LOGERR( "Error parsing JSON\n");
        return -4;
    }

    cJSON *domains = cJSON_GetObjectItem(root, "Domains");
    if (!domains || !cJSON_IsArray(domains)) {
        NDW_LOGERR( "'Domains' array not found\n");
        cJSON_Delete(root);
        return -5;
    }

    for (INT_T i = 0; domain_names[i] != NULL; ++i)
    {
        const CHAR_T *target_domain_name = domain_names[i];

        cJSON *domain_obj = NULL;
        cJSON_ArrayForEach(domain_obj, domains)
        {
            cJSON *name_item = cJSON_GetObjectItem(domain_obj, "DomainName");
            if (name_item && strcmp(name_item->valuestring, target_domain_name) == 0)
            {
                ndw_Domain_T *domain = (ndw_Domain_T *)calloc(1, sizeof(ndw_Domain_T));
                domain->domain_handle = domain_handle;
                domain->domain_name = strdup(cJSON_GetObjectItem(domain_obj, "DomainName")->valuestring);
                domain->domain_id = cJSON_GetObjectItem(domain_obj, "DomainID")->valueint;
                domain->domain_description = strdup(cJSON_GetObjectItem(domain_obj, "DomainDescription")->valuestring);

                CHAR_T* id_as_string = cJSON_PrintUnformatted(cJSON_GetObjectItem(domain_obj, "DomainID"));
                domain->debug_desc = ndw_ConcatStrings("Domain<", id_as_string, ", ", domain->domain_name, ">", NULL);
                free(id_as_string);
                NDW_LOGX("Parsing Domain: %s\n", domain->debug_desc);

                HASH_ADD(hh_domain_id, domain_handle->g_domains_by_id, domain_id, sizeof(INT_T), domain);
                HASH_ADD_KEYPTR(hh_domain_name, domain_handle->g_domains_by_name, domain->domain_name, strlen(domain->domain_name), domain);


                cJSON *connections = cJSON_GetObjectItem(domain_obj, "Connections");
                if (connections && cJSON_IsArray(connections))
                {
                    cJSON *conn_obj = NULL;
                    cJSON_ArrayForEach(conn_obj, connections)
                    {
                        ndw_Connection_T *conn = (ndw_Connection_T *)calloc(1, sizeof(ndw_Connection_T));
                        conn->domain_handle = domain_handle;
                        conn->domain = domain;

                        cJSON* conn_disabled_obj = cJSON_GetObjectItem(conn_obj, "Disabled");
                        if ((NULL != conn_disabled_obj) && (0 == strcasecmp(conn_disabled_obj->valuestring, "true")))
                            conn->disabled = true;

                        conn->connection_unique_name = strdup(cJSON_GetObjectItem(conn_obj, "ConnectionUniqueName")->valuestring);
                        conn->connection_unique_id = cJSON_GetObjectItem(conn_obj, "ConnectionUniqueID")->valueint;
                        conn->vendor_name = strdup(cJSON_GetObjectItem(conn_obj, "VendorName")->valuestring);
                        conn->vendor_id = cJSON_GetObjectItem(conn_obj, "VendorId")->valueint;
                        conn->vendor_logical_version = cJSON_GetObjectItem(conn_obj, "VendorLogicVersion")->valueint;
                        conn->vendor_real_version = strdup(cJSON_GetObjectItem(conn_obj, "VendorRealVersion")->valuestring);
                        conn->tenant_id = cJSON_GetObjectItem(conn_obj, "TenantID")->valueint;
                        conn->connection_url = strdup(cJSON_GetObjectItem(conn_obj, "ConnectionURL")->valuestring);
                        conn->connection_comments = strdup(cJSON_GetObjectItem(conn_obj, "ConnectionComments")->valuestring);

                        ndw_SetConnectionDebugDesc(conn_obj, conn);

                        NDW_LOGX("Parsing Connection: %s\n", conn->debug_desc);

                        conn->vendor_connection_options = ndw_GetJsonItem(conn_obj, "ConnectionOptions", false);
                        if ((NULL != conn->vendor_connection_options) && ('\0' != *(conn->vendor_connection_options))) {
                            ndw_ParseNVPairs(conn->vendor_connection_options, &(conn->vendor_connection_options_nvpairs));
                            ndw_PrintNVPairs("Vendor Connection Options", &(conn->vendor_connection_options_nvpairs));
                        }

                        HASH_ADD(hh_connection_id, domain->connections_by_id, connection_unique_id, sizeof(INT_T), conn);
                        HASH_ADD_KEYPTR(hh_connection_name, domain->connections_by_name, conn->connection_unique_name, strlen(conn->connection_unique_name), conn);

                        INT_T num_topics = 0;
                        INT_T num_disabled_topics = 0;
                        cJSON *topics = cJSON_GetObjectItem(conn_obj, "Topics");
                        if (topics && cJSON_IsArray(topics))
                        {
                            cJSON *topic_obj = NULL;
                            cJSON_ArrayForEach(topic_obj, topics)
                            {
                                ++num_topics;
                                ndw_Topic_T *topic = (ndw_Topic_T *)calloc(1, sizeof(ndw_Topic_T));
                                topic->domain_handle = domain_handle;
                                topic->domain = domain;
                                topic->connection = conn;

                                if (conn->disabled) {
                                    topic->disabled = true; // If connection is disabled the Topic is also disabled.
                                    ++num_disabled_topics;
                                }
                                else {
                                    cJSON* topic_disabled_obj = cJSON_GetObjectItem(topic_obj, "Disabled");
                                    if ((NULL != topic_disabled_obj) &&
                                        (0 == strcasecmp(topic_disabled_obj->valuestring, "true")))
                                    {
                                        topic->disabled = true;
                                        ++num_disabled_topics;
                                    }
                                }

                                topic->topic_description = strdup(cJSON_GetObjectItem(topic_obj, "TopicDescription")->valuestring);
                                topic->topic_unique_id = cJSON_GetObjectItem(topic_obj, "TopicUniqueID")->valueint;
                                topic->topic_unique_name = strdup(cJSON_GetObjectItem(topic_obj, "TopicUniqueName")->valuestring);

                                cJSON* json_pub_object = cJSON_GetObjectItem(topic_obj, "PubKey");
                                if ((NULL == json_pub_object) || NDW_ISNULLCHARPTR(json_pub_object->valuestring)) {
                                    topic->pub_key = strdup("");
                                    topic->is_pub_enabled = false;
                                } else {
                                    topic->pub_key = strdup(json_pub_object->valuestring);
                                    topic->is_pub_enabled = true;
                                }

                                cJSON* json_sub_object = cJSON_GetObjectItem(topic_obj, "SubKey");
                                if ((NULL == json_sub_object) || NDW_ISNULLCHARPTR(json_sub_object->valuestring)) {
                                    topic->sub_key = strdup("");
                                    topic->is_sub_enabled = false;
                                } else {
                                    topic->sub_key = strdup(json_sub_object->valuestring);
                                    topic->is_sub_enabled = true;
                                }

                                const CHAR_T* disabled_topic = topic->disabled ? " <= *DISABLED*" : "";
                                CHAR_T* id_as_string = cJSON_PrintUnformatted(cJSON_GetObjectItem(topic_obj, "TopicUniqueID"));

                                cJSON* json_async_queue_name = cJSON_GetObjectItem(topic_obj, "MsgQueueName");
                                if (NULL != json_async_queue_name) {
                                    char* q_name = json_async_queue_name->valuestring;
                                    if (! NDW_ISNULLCHARPTR(q_name)) {
                                        cJSON* json_async_queue_size = cJSON_GetObjectItem(topic_obj, "MsgQueueSize");
                                        if (NULL == json_async_queue_size) {
                                            NDW_LOGERR("*** FATAL ERROR: Need to specific Topic MsgQueueSize JSON tag for Topic<%s>\n",
                                                    topic->topic_unique_name);
                                            exit(EXIT_FAILURE);
                                        }

                                        if ((topic->q_async_size = json_async_queue_size->valueint) <= 0) {
                                            NDW_LOGERR("*** FATAL ERROR: Invalid MsgQueueSize<%d> JSON tag for Topic<%s>\n",
                                                    topic->q_async_size, topic->topic_unique_name);
                                            exit(EXIT_FAILURE);
                                        }

                                        topic->q_async_name = strdup(q_name);

                                        topic->q_async = ndw_CreateInboundDataQueue(topic->q_async_name, topic->q_async_size);
                                        if (NULL == topic->q_async) {
                                            NDW_LOGERR("*** FATAL ERROR: Failed to create Asynchronous Queue based on Queue Name<%s> for Topic<%s>\n",
                                                        topic->q_async_name, topic->topic_unique_name);
                                            exit(EXIT_FAILURE);
                                        }
                                        topic->q_async_enabled = true;
                                    }
                                }

                                topic->debug_desc = ndw_ConcatStrings("Topic<", id_as_string, ", ", topic->topic_unique_name,
                                                    disabled_topic, ", (pk: \"", topic->pub_key,
                                                    "\") (sk: \"", topic->sub_key, "\")> ",
                                                    "QAsync<", ((NULL == topic->q_async) ? "n" : "Y"), "> ",
                                                    conn->debug_desc, NULL);
                                free(id_as_string);
                                NDW_LOGX("Parsing Topic: %s\n", topic->debug_desc);

                                topic->topic_options = ndw_GetJsonItem(topic_obj, "TopicOptions", false);
                                if ((NULL != topic->topic_options) && ('\0' != *(topic->topic_options))) {
                                    ndw_ParseNVPairs(topic->topic_options, &(topic->topic_options_nvpairs));
                                    ndw_PrintNVPairs("Topic Options", &(topic->topic_options_nvpairs));
                                }

                                topic->vendor_topic_options = ndw_GetJsonItem(topic_obj, "VendorTopicOptions", false);
                                if ((NULL != topic->vendor_topic_options) && ('\0' != *(topic->vendor_topic_options))) {
                                    ndw_ParseNVPairs(topic->vendor_topic_options, &(topic->vendor_topic_options_nvpairs));
                                    ndw_PrintNVPairs("VENDOR Topic Options", &(topic->vendor_topic_options_nvpairs));
                                }

                                HASH_ADD(hh_topic_id, conn->topics_by_id, topic_unique_id, sizeof(INT_T), topic);
                                HASH_ADD_KEYPTR(hh_topic_name, conn->topics_by_name, topic->topic_unique_name, strlen(topic->topic_unique_name), topic);
                            }
                        } // end for each Topic within each Connection in JSON

                        // If there are no topics for the connection or all topics are disabled, then
                        // the connection should be disabled.
                        if ((0 == num_topics) || (num_disabled_topics == num_topics)) {
                            conn->disabled = true;
                            ndw_SetConnectionDebugDesc(conn_obj, conn);
                        }

                    } // end for each Connection in JSON

                } // end for Connection List indicator in JSON

                break; // Found and parsed the target domain

            } // end for a matching Domain
        } // end for each Domain in JSON
    } // end for i

    cJSON_Delete(root);

    return 0;
} // end ndw_LoadDomains

ndw_Domain_T*
ndw_GetDomainById(INT_T domain_id)
{
    ndw_DomainHandle_T* dh = ndw_GetDomainHandleInternal();
    ndw_Domain_T* domain = NULL;
    HASH_FIND(hh_domain_id, dh->g_domains_by_id, &domain_id, sizeof(INT_T), domain);
    return domain;
} // ndw_GetDomainById

ndw_Domain_T*
ndw_GetDomainByName(const CHAR_T* domain_name)
{
    if ((NULL == domain_name) || ('\0' == *domain_name))
        return NULL;

    ndw_Domain_T* domain = NULL;
    ndw_DomainHandle_T* dh = ndw_GetDomainHandleInternal();
    HASH_FIND(hh_domain_name, dh->g_domains_by_name, domain_name, strlen(domain_name), domain);
    return domain;
}

ndw_Connection_T*
ndw_GetConnectionByName(const CHAR_T* domain_name, const CHAR_T* connection_name)
{
    ndw_Domain_T* domain = ndw_GetDomainByName(domain_name);
    if (NULL == domain)
        return NULL;

    if ((NULL == connection_name) || ('\0' == *connection_name))
        return NULL;

    ndw_Connection_T* connection = NULL;
    HASH_FIND(hh_connection_name, domain->connections_by_name, connection_name, strlen(connection_name), connection);
    return connection;
} // end method ndw_GetConnectionByName


ndw_Connection_T*
ndw_GetConnectionByNameFromDomain(ndw_Domain_T* domain, const CHAR_T* connection_name)
{
    if (!domain || !connection_name) {
        return NULL;
    }

    ndw_Connection_T* connection = NULL;
    HASH_FIND(hh_connection_name, domain->connections_by_name, connection_name, strlen(connection_name), connection);
    return connection;
}

ndw_Connection_T*
ndw_GetConnectionByIdFromDomain(ndw_Domain_T* domain, INT_T connection_id)
{
    if (!domain) {
        return NULL;
    }

    ndw_Connection_T* connection = NULL;
    HASH_FIND(hh_connection_id, domain->connections_by_id, &connection_id, sizeof(INT_T), connection);
    return connection;
}

ndw_Topic_T*
ndw_GetTopicByNameFromConnection(ndw_Connection_T* connection, const CHAR_T* topic_name)
{
    if (!connection || !topic_name) {
        return NULL;
    }

    ndw_Topic_T* topic = NULL;
    HASH_FIND(hh_topic_name, connection->topics_by_name, topic_name, strlen(topic_name), topic);
    return topic;
}

ndw_Topic_T*
ndw_GetTopicByIdFromConnection(ndw_Connection_T* connection, INT_T topic_id)
{
    if (!connection) {
        return NULL;
    }

    ndw_Topic_T* topic = NULL;
    HASH_FIND(hh_topic_id, connection->topics_by_id, &topic_id, sizeof(INT_T), topic);
    return topic;
}

ndw_Topic_T**
ndw_GetAllTopicsFromConnection(ndw_Connection_T* connection, INT_T* total_topics)
{
    if (NULL == connection) return NULL;
    if (NULL == total_topics) return NULL;

    INT_T num_topics = 0;
    ndw_Topic_T *curr, *tmp;

    // Count the number of topics
    HASH_ITER(hh_topic_name, connection->topics_by_name, curr, tmp) {
        num_topics++;
    }

    if (num_topics == 0) return NULL;

    // Allocate array with space for NULL terminator
    ndw_Topic_T** topics_array = (ndw_Topic_T**)malloc(sizeof(ndw_Topic_T*) * (num_topics + 1));
    if (!topics_array) return NULL;

    // Fill the array
    INT_T index = 0;
    HASH_ITER(hh_topic_name, connection->topics_by_name, curr, tmp) {
        topics_array[index++] = curr;
    }
    topics_array[index] = NULL; // NULL terminator

    *total_topics = index;

    return topics_array;
} // end method ndw_GetAllTopicsFromConnection


ndw_Connection_T**
ndw_GetAllConnectionsFromDomain(ndw_Domain_T* domain, INT_T* total_connections)
{
    if (NULL == domain) return NULL;
    if (NULL == total_connections) return NULL;

    *total_connections = 0;
    INT_T num_connections = 0;
    ndw_Connection_T *curr, *tmp;

    // Count number of connections
    HASH_ITER(hh_connection_name, domain->connections_by_name, curr, tmp) {
        num_connections++;
    }

    if (num_connections == 0) return NULL;

    // Allocate array with space for NULL terminator
    ndw_Connection_T** connections_array = (ndw_Connection_T**)malloc(sizeof(ndw_Connection_T*) * (num_connections + 1));
    if (!connections_array) return NULL;

    // Populate array
    INT_T index = 0;
    HASH_ITER(hh_connection_name, domain->connections_by_name, curr, tmp) {
        connections_array[index++] = curr;
    }
    connections_array[index] = NULL; // NULL-terminate

    *total_connections = index;

    return connections_array;
} // end method ndw_GetAllConnectionsFromDomain


ndw_Domain_T**
ndw_GetAllDomains(INT_T* total_domains)
{
    if (NULL == total_domains)
        return NULL;

    *total_domains = 0;

    ndw_DomainHandle_T* dh = ndw_GetDomainHandleInternal();
    INT_T count = 0;
    ndw_Domain_T *current, *tmp;

    // Count the number of domains
    HASH_ITER(hh_domain_name, dh->g_domains_by_name, current, tmp) {
        count++;
    }

    if (count == 0) return NULL;

    // Allocate array of ndw_Domain_T POINTERS (+1 for NULL terminator)
    ndw_Domain_T **domain_list = malloc((count + 1) * sizeof(ndw_Domain_T*));
    if (!domain_list) return NULL;

    // Populate array
    INT_T i = 0;
    HASH_ITER(hh_domain_name, dh->g_domains_by_name, current, tmp) {
        domain_list[i++] = current;
    }
    domain_list[i] = NULL;

    *total_domains = i;

    return domain_list;
} // end method ndw_GetAllDomains

void
ndw_PrintTopicJson(ndw_Topic_T *topic, INT_T indent)
{
    if (!topic)
        return;

    NDW_LOG("%*s{\n", indent, "");
    NDW_LOG("%*s\"Topic DISABLED?\": %s,\n", indent + 2, "", topic->disabled ? "TRUE" : "false");
    NDW_LOG("%*s\"TopicUniqueID\": %d,\n", indent + 2, "", topic->topic_unique_id);
    NDW_LOG("%*s\"TopicUniqueName\": \"%s\",\n", indent + 2, "", topic->topic_unique_name);
    NDW_LOG("%*s\"TopicDescription\": \"%s\"\n", indent + 2, "", topic->topic_description);
    NDW_LOG("%*s\"NOTE: Connection ID\": %d,\n", indent + 2, "", topic->connection->connection_unique_id);
    NDW_LOG("%*s\"NOTE: Connection Name\": %s,\n", indent + 2, "", topic->connection->connection_unique_name);
    NDW_LOG("%*s\"NOTE: PubKey \": %s,\n", indent + 2, "", topic->pub_key);
    NDW_LOG("%*s\"NOTE: SubKey \": %s,\n", indent + 2, "", topic->sub_key);
    NDW_LOG("%*s\"NOTE: debug_desc \": %s,\n", indent + 2, "", topic->debug_desc);
    NDW_LOG("%*s\"NOTE: Connection(0x%lX) Domain(0x%lX) DomainHandle(0x%lX)\n", indent + 2, "",
        (ULONG_T) topic->connection, 
        (ULONG_T) topic->domain, 
        (ULONG_T) topic->domain_handle);
    NDW_LOG("%*s}", indent, "");
} // ndw_PrintTopicJson

void
ndw_PrintConnectionJson(ndw_Connection_T *conn, INT_T indent)
{
    if (!conn) return;
    NDW_LOG("%*s{\n", indent, "");
    NDW_LOG("%*s\"ConnectionUniqueID\": %d,\n", indent + 2, "", conn->connection_unique_id);
    NDW_LOG("%*s\"ConnectionUniqueName\": \"%s\",\n", indent + 2, "", conn->connection_unique_name);
    NDW_LOG("%*s\"VendorName\": \"%s\",\n", indent + 2, "", conn->vendor_name);
    NDW_LOG("%*s\"VendorId\": %d,\n", indent + 2, "", conn->vendor_id);
    NDW_LOG("%*s\"VendorLogicVersion\": %d,\n", indent + 2, "", conn->vendor_logical_version);
    NDW_LOG("%*s\"VendorRealVersion\": \"%s\",\n", indent + 2, "", conn->vendor_real_version);
    NDW_LOG("%*s\"TenantID\": %d,\n", indent + 2, "", conn->tenant_id);
    NDW_LOG("%*s\"ConnectionURL\": \"%s\",\n", indent + 2, "", conn->connection_url);
    NDW_LOG("%*s\"ConnectionComments\": \"%s\",\n", indent + 2, "", conn->connection_comments);
    NDW_LOG("%*s\"debug_desc\": \"%s\",\n", indent + 2, "", conn->debug_desc);
    NDW_LOG("%*s\"NOTE: Connection(0x%lX) Domain(0x%lX) DomainHandle(0x%lX)\n", indent + 2, "",
        (ULONG_T) conn,
        (ULONG_T) conn->domain,
        (ULONG_T) conn->domain_handle);

    NDW_LOG("%*s\"Topics\": [\n", indent + 2, "");
    ndw_Topic_T *topic, *tmp_topic;
    INT_T first = 1;
    HASH_ITER(hh_topic_id, conn->topics_by_id, topic, tmp_topic) {
        if (!first) NDW_LOG(",\n");
        ndw_PrintTopicJson(topic, indent + 4);
        first = 0;
    }
    NDW_LOG("\n%*s]\n", indent + 2, "");
    NDW_LOG("%*s}", indent, "");
} // end method ndw_PrintConnectionJson

void
ndw_PrintDomainJson(ndw_Domain_T *domain, INT_T indent)
{
    if (!domain) return;
    NDW_LOG("%*s{\n", indent, "");
    NDW_LOG("%*s\"DomainID\": %d,\n", indent + 2, "", domain->domain_id);
    NDW_LOG("%*s\"DomainName\": \"%s\",\n", indent + 2, "", domain->domain_name);
    NDW_LOG("%*s\"DomainDescription\": \"%s\",\n", indent + 2, "", domain->domain_description);
    NDW_LOG("%*s\"NOTE: Domain(0x%lX) DomainHandle(0x%lX)\n", indent + 2, "",
        (ULONG_T) domain,
        (ULONG_T) domain->domain_handle);

    NDW_LOG("%*s\"Connections\": [\n", indent + 2, "");
    ndw_Connection_T *conn, *tmp_conn;
    INT_T first = 1;
    HASH_ITER(hh_connection_id, domain->connections_by_id, conn, tmp_conn) {
        if (!first) NDW_LOG(",\n");
        ndw_PrintConnectionJson(conn, indent + 4);
        first = 0;
    }
    NDW_LOG("\n%*s]\n", indent + 2, "");
    NDW_LOG("%*s}", indent, "");
    NDW_LOG("\n\n");
} // end method ndw_PrintDomainJson

CHAR_T*
ndw_ConcatDomainConnectionTopicByName(const CHAR_T* domain_name, const CHAR_T* connection_name, const CHAR_T* topic_name) 
{
    // Check if any input string is NULL
    if (domain_name == NULL || connection_name == NULL || topic_name == NULL) {
        return NULL;
    }

    // Calculate the total length of the strings
    size_t total_length = strlen(domain_name) + strlen(connection_name) + strlen(topic_name) + 3; // +3 for '^', '^' and '\0'

    // Allocate memory for the new string
    CHAR_T* new_str = malloc(total_length * sizeof(CHAR_T));
    if (new_str == NULL) {
        // Memory allocation failed, return NULL
        return NULL;
    }

    // Concatenate the strings with '^' in between
    sprintf(new_str, "%s^%s^%s", domain_name, connection_name, topic_name);

    return new_str;
} // end method ndw_ConcatDomainConnectionTopicByName

void
ndw_FreeDomainConnectionTopicNames(CHAR_T** topic_path)
{
    if (topic_path != NULL) {
        for (CHAR_T** ptr = topic_path; *ptr != NULL; ptr++) {
            free(*ptr);
        }
        free(topic_path);
    }
} // end method FreeDomainConnectionTopicNames

ndw_Topic_T*
ndw_GetTopicFromFullPath(const CHAR_T* path)
{
    INT_T length = 0;
    CHAR_T** dct_list = ndw_GetDomainConnectionTopicNames(path, &length);
    if (3 != length) {
        NDW_LOGERR("ERROR: Invalid Path <%s>. Use DomainName^ConnectionNam^TopicName as a full path\n", path);
        ndw_FreeDomainConnectionTopicNames(dct_list);
        return NULL;
    }

    ndw_Domain_T* d = ndw_GetDomainByName(dct_list[0]);
    if (NULL == d) {
        NDW_LOGERR("Invalid Path <%s>. Cannot find Domain <%s>\n", path, dct_list[0]);
        ndw_FreeDomainConnectionTopicNames(dct_list);
        return NULL;
    }

    ndw_Connection_T* c = ndw_GetConnectionByNameFromDomain(d, dct_list[1]);
    if (NULL == c) {
        NDW_LOGERR("Invalid Path <%s>. Cannot find Connection <%s> from Domain <%s>\n",
                        path, dct_list[1], dct_list[0]);
        ndw_FreeDomainConnectionTopicNames(dct_list);
        return NULL;
    }

    ndw_Topic_T* t = ndw_GetTopicByNameFromConnection(c, dct_list[2]);
    if (NULL == t) {
        NDW_LOGERR("Invalid Path <%s>. Cannot find Topic <%s> from Connection <%s> for Domain <%s>\n",
                        path, dct_list[2], dct_list[1], dct_list[0]);
        ndw_FreeDomainConnectionTopicNames(dct_list);
        return NULL;
    }

    ndw_FreeDomainConnectionTopicNames(dct_list);
    return t;
} // end method ndw_GetTopicFromFullPath

CHAR_T**
ndw_GetDomainConnectionTopicNames(const CHAR_T* topic_path, INT_T* length)
{
    *length = 0;
    // Check if input string is NULL
    if (topic_path == NULL || length == NULL) {
        return NULL;
    }

    // Allocate memory for the four strings (3 strings + NULL)
    CHAR_T** result = malloc(4 * sizeof(CHAR_T*));
    if (result == NULL) {
        // Memory allocation failed, return NULL
        return NULL;
    }

    // Initialize all elements to NULL
    for (INT_T i = 0; i < 4; i++) {
        result[i] = NULL;
    }

    const CHAR_T* start = topic_path;
    INT_T index = 0;
    const CHAR_T* ptr = topic_path;
    while (*ptr != '\0' && index < 3) {
        if (*ptr == '^' || *(ptr + 1) == '\0') {
            INT_T str_length = ptr - start + (*(ptr + 1) == '\0' && *ptr != '^');
            if (str_length <= 0) {
                ndw_FreeDomainConnectionTopicNames(result);
                return NULL;
            }
            result[index] = malloc((str_length + 1) * sizeof(CHAR_T));
            if (result[index] == NULL) {
                ndw_FreeDomainConnectionTopicNames(result);
                return NULL;
            }
            strncpy(result[index], start, str_length);
            result[index][str_length] = '\0';
            start = ptr + 1;
            index++;
        }
        ptr++;
    }

    // Check if there are exactly 3 strings
    if (index != 3) {
        ndw_FreeDomainConnectionTopicNames(result);
        return NULL;
    }

    result[3] = NULL; // Set the last element to NULL
    *length = 3; // Set the length
    return result;
} // end method ndw_GetDomainConnectionTopicNames

INT_T
ndw_GetDomainConnectionTopicFromPath(const CHAR_T* topic_path, ndw_Domain_T** p_domain, ndw_Connection_T** p_connection, ndw_Topic_T** p_topic)
{
    if ((NULL == topic_path) || ('\0' == *topic_path))
        return -1;

    INT_T path_length = 0;
    CHAR_T** domain_connection_topic_names = ndw_GetDomainConnectionTopicNames(topic_path, &path_length);
    if (NULL == domain_connection_topic_names) {
        NDW_LOGERR( "*** ERROR: domain_connection_topic_names(%s) returned NULL!\n", topic_path);
        return -1;
    }

    if (3 != path_length) {
        NDW_LOGERR( "*** ERROR: domain_connection_topic_names(%s) returned invalid path_length = %d!\n", topic_path, path_length);
        return -1;
    }

    ndw_Domain_T* d = ndw_GetDomainByName(domain_connection_topic_names[0]);
    if (NULL == d) {
        ndw_FreeDomainConnectionTopicNames(domain_connection_topic_names);
        return -1;
    }

    *p_domain = d;

    ndw_Connection_T* c = ndw_GetConnectionByNameFromDomain(d, domain_connection_topic_names[1]);
    if (NULL == c) {
        ndw_FreeDomainConnectionTopicNames(domain_connection_topic_names);
        return -2;
    }

    *p_connection = c;

    ndw_Topic_T* t = ndw_GetTopicByNameFromConnection(c, domain_connection_topic_names[2]);
    if (NULL == t) {
        ndw_FreeDomainConnectionTopicNames(domain_connection_topic_names);
        return -3;
    }

    *p_topic = t;

    ndw_FreeDomainConnectionTopicNames(domain_connection_topic_names);
    return 0;
} // end method ndw_GetDomainConnectionTopicFromPath


void
ndw_DebugAndPrintDomainConfigurations()
{
    NDW_LOG("============= BEGIN: DEBUG CONFIGURATION ==============\n\n");

    INT_T total_domains = 0;
    ndw_Domain_T** domains = ndw_GetAllDomains(&total_domains);
    if (NULL == domains) {
        NDW_LOGERR( "*** ERROR: FAILED to get list of Domains!");
        return;
    }

    if (0 == total_domains) {
        NDW_LOGERR( "*** ERROR: FAILED to get List of Domains!");
        free(domains);
        return;
    }
    
    INT_T i = 0;
    for (ndw_Domain_T* domain = domains[i]; NULL != domain; domain = domains[++i])
    {
        NDW_LOG("-> New Domain: Name<%s> ID<%d>\n", domain->domain_name, domain->domain_id);

        ndw_Domain_T* domain_check = ndw_GetDomainById(domain->domain_id);
        if ((NULL == domain_check) ||
            (domain != domain_check) || (domain->domain_id != domain_check->domain_id) ||
            (0 != strcmp(domain->domain_name, domain_check->domain_name))) {
            NDW_LOGERR( "*** ERROR: ndw_GetDomainById(%d) failed!\n", domain->domain_id);
            return;
        }

        domain_check = ndw_GetDomainByName(domain->domain_name);
        if ((NULL == domain_check) ||
            (domain != domain_check) || (domain->domain_id != domain_check->domain_id) ||
            (0 != strcmp(domain->domain_name, domain_check->domain_name))) {
            NDW_LOGERR( "*** ERROR: ndw_GetDomainByName(%s) failed!\n", domain->domain_name);
            return;
        }

        INT_T total_connections = 0;
        ndw_Connection_T** connections = ndw_GetAllConnectionsFromDomain(domain, &total_connections);

        if (NULL == connections)
            continue;

        if (0 == total_connections) {
            free(connections);
            connections = NULL;
            continue;
        }

        INT_T j = 0;
        for (ndw_Connection_T* conn = connections[j]; NULL != conn; conn = connections[++j])
        {
            NDW_LOG("   --> Connection Name<%s%s> ID<%d>\n", conn->connection_unique_name,
                            conn->disabled ? " <= *DISABLED*" : "", conn->connection_unique_id);

            if (NULL == conn->domain) {
                NDW_LOGERR( "*** ERROR: Domain NOT set on connection ID<%d>", conn->connection_unique_id);
                return;
            }

            if (domain->domain_id != conn->domain->domain_id) {
                NDW_LOGERR( "*** ERROR: Domain Check on ID FAILED <%d> != <%d>\n",
                        domain->domain_id, conn->domain->domain_id);
                return;
            }

            ndw_Connection_T* conn_check = ndw_GetConnectionByIdFromDomain(domain, conn->connection_unique_id);
            if ((NULL == conn_check) ||
                (conn != conn_check) || (conn->connection_unique_id != conn_check->connection_unique_id) ||
                (0 != strcmp(conn->connection_unique_name, conn_check->connection_unique_name)))
            {
                NDW_LOGERR( "*** ERROR: ndw_GetConnectByIdFromDomain(%d) FAILED\n", conn->connection_unique_id);
                return;
            }

            conn_check = ndw_GetConnectionByNameFromDomain(domain, conn->connection_unique_name);
            if ((NULL == conn_check) ||
                (conn != conn_check) || (conn->connection_unique_id != conn_check->connection_unique_id) ||
                (0 != strcmp(conn->connection_unique_name, conn_check->connection_unique_name)))
            {
                NDW_LOGERR( "*** ERROR: ndw_GetConnectByNameFromDomain(%s) FAILED\n", conn->connection_unique_name);
                return;
            }

            INT_T total_topics = 0;
            ndw_Topic_T** topics = ndw_GetAllTopicsFromConnection(conn, &total_topics);

            if (NULL == topics)
                continue;

            if (0 == total_topics) {
                free(topics);
                topics = NULL;
                continue;
            }

            INT_T k = 0;
            for (ndw_Topic_T* topic = topics[k]; NULL != topic; topic = topics[++k])
            {
                NDW_LOG("      --> Topic Name<%s%s> ID<%d>\n", topic->topic_unique_name,
                    topic->disabled ? " <= *DISABLED*" : "", topic->topic_unique_id);

                if (NULL == topic->connection) {
                    NDW_LOGERR( "*** ERROR: Connection NOT set on Topic ID<%d>\n", topic->topic_unique_id);
                    return;
                }

                if (conn->connection_unique_id != topic->connection->connection_unique_id) {
                    NDW_LOGERR( "*** ERROR: Connecton Check on ID FAILED <%d> != <%d>\n",
                            conn->connection_unique_id, topic->connection->connection_unique_id);
                    return;
                }

                ndw_Topic_T* topic_check = ndw_GetTopicByIdFromConnection(conn,topic->topic_unique_id);
                if ((NULL == topic_check) ||
                    (topic != topic_check) || (topic->topic_unique_id != topic_check->topic_unique_id) ||
                    (0 != strcmp(topic->topic_unique_name, topic_check->topic_unique_name))) {
                    NDW_LOGERR( "*** ERROR: getTopicByIdFromConnection(%d) FAILED!\n", topic->topic_unique_id);
                    return;
                }

                topic_check = ndw_GetTopicByNameFromConnection(conn,topic->topic_unique_name);
                if ((NULL == topic_check) ||
                    (topic != topic_check) || (topic->topic_unique_id != topic_check->topic_unique_id) ||
                    (0 != strcmp(topic->topic_unique_name, topic_check->topic_unique_name))) {
                    NDW_LOGERR( "*** ERROR: ndw_GetTopicByNameFromConnection(%s) FAILED!\n", topic->topic_unique_name);
                    return;
                }

                CHAR_T* topic_path = ndw_ConcatDomainConnectionTopicByName(
                    domain->domain_name, conn->connection_unique_name, topic->topic_unique_name);
                if (NULL == topic_path) {
                    NDW_LOGERR( "*** ERROR: Topic Path is NULL for Domain^Connection^Topic Names <%s^%s^%s>\n",
                    domain->domain_name, conn->connection_unique_name, topic->topic_unique_name);
                    return;
                }
                NDW_LOG("        Domain^Connection^Topic Name <%s>\n", topic_path);

                INT_T path_length = 0;
                CHAR_T** domain_connection_topic_names = ndw_GetDomainConnectionTopicNames(topic_path, &path_length);
                if (NULL == domain_connection_topic_names) {
                       NDW_LOGERR( "*** ERROR: domain_connection_topic_names(%s) returned NULL!\n", topic_path);
                       return;
                }

                if (3 != path_length) {
                       NDW_LOGERR( "*** ERROR: domain_connection_topic_names(%s) returned invalid path_length = %d!\n", topic_path, path_length);
                       return;
                }

                INT_T ptr_index = 0;
                CHAR_T* ptr = domain_connection_topic_names[ptr_index];
                while (NULL != ptr) {
                        NDW_LOG("        domain_connection_topic_names[%d] = <%s>\n", ptr_index, ptr);
                        ptr = domain_connection_topic_names[++ptr_index];
                }

                ndw_Domain_T* d = ndw_GetDomainByName(domain_connection_topic_names[0]);
                if (NULL == d) {
                    NDW_LOGERR( "*** ERROR: FAILED to get Domain from name <%s>\n", domain_connection_topic_names[0]);
                    return;
                }

                ndw_Connection_T* c = ndw_GetConnectionByNameFromDomain(d, domain_connection_topic_names[1]);
                if (NULL == c) {
                    NDW_LOGERR( "*** ERROR: FAILED to get Connection <%s> with Domain name <%s>\n",
                        domain_connection_topic_names[1],
                        domain_connection_topic_names[0]);
                    return;
                }

                ndw_Topic_T* t = ndw_GetTopicByNameFromConnection(c, domain_connection_topic_names[2]);
                if (NULL == t) {
                    NDW_LOGERR( "*** ERROR: FAILED to get Topic <%s> with Connection <%s> with Domain name <%s>\n",
                        domain_connection_topic_names[2],
                        domain_connection_topic_names[1],
                        domain_connection_topic_names[0]);
                    return;
                }

                if (t != topic) {
                    NDW_LOGERR( "*** ERROR: Invalid Topic Returned with Name<%s> while expected Name<%s> for Topic <%s> with Connection <%s> with Domain name <%s>\n",
                        t->topic_unique_name, topic->topic_unique_name,
                        domain_connection_topic_names[2],
                        domain_connection_topic_names[1],
                        domain_connection_topic_names[0]);
                    return;
                }

                free(topic_path);
                ndw_FreeDomainConnectionTopicNames(domain_connection_topic_names);

            } // end Topics

            free(topics);

        } // end Connections
        
        free(connections);
    } // end Domains


    NDW_LOG("============= END: DEBUG CONFIGURATION ==============\n\n");

    NDW_LOG("============= BEGIN: Print in JSON Format ==============\n\n");
    i = 0;
    for (ndw_Domain_T* domain = domains[i]; NULL != domain; domain = domains[++i])
    {
            ndw_PrintDomainJson(domain, 0);
            NDW_LOG("---\n");
    }
    NDW_LOG("============= END: Print in JSON Format ==============\n\n");

    free(domains);

} // end method ndw_DebugAndPrintDomainConfigurations


void
ndw_free_topics(ndw_Topic_T **topics_by_id, ndw_Topic_T **topics_by_name)
{
    ndw_Topic_T *current_topic, *tmp;

    // Free by ID hash (primary freeing loop)
    HASH_ITER(hh_topic_id, *topics_by_id, current_topic, tmp) {
        HASH_DELETE(hh_topic_id, *topics_by_id, current_topic);
        HASH_DELETE(hh_topic_name, *topics_by_name, current_topic); // Remove from name hash too
        free(current_topic->topic_description);
        free(current_topic->topic_unique_name);
        free(current_topic->pub_key);
        free(current_topic->sub_key);
        free(current_topic->debug_desc);
        ndw_FreeNVPairs(&(current_topic->topic_options_nvpairs));
        free(current_topic->topic_options);
        ndw_FreeNVPairs(&(current_topic->vendor_topic_options_nvpairs));
        free(current_topic->vendor_topic_options);
        free(current_topic->q_async_name);
        if (NULL != current_topic->q_async) {
            free(current_topic->q_async);
            current_topic->q_async = NULL;
        }
        free(current_topic); // Only once
    }
} // end method ndw_free_topics

void
ndw_free_connections(ndw_Connection_T **connections_by_id, ndw_Connection_T **connections_by_name)
{
    ndw_Connection_T *current_conn, *tmp;

    HASH_ITER(hh_connection_id, *connections_by_id, current_conn, tmp) {
        HASH_DELETE(hh_connection_id, *connections_by_id, current_conn);
        HASH_DELETE(hh_connection_name, *connections_by_name, current_conn); // Remove from name hash too

        // Free nested topics
        ndw_free_topics(&current_conn->topics_by_id, &current_conn->topics_by_name);

        free(current_conn->connection_unique_name);
        free(current_conn->vendor_name);
        free(current_conn->vendor_real_version);
        free(current_conn->connection_url);
        free(current_conn->connection_comments);
        ndw_FreeNVPairs(&(current_conn->vendor_connection_options_nvpairs));
        free(current_conn->vendor_connection_options);
        free(current_conn->debug_desc);
        free(current_conn); // Only once
    }
} // end method ndw_free_connections

void
ndw_free_domains(ndw_Domain_T **domains_by_id, ndw_Domain_T **domains_by_name)
{
    ndw_Domain_T *current_domain, *tmp;

    HASH_ITER(hh_domain_id, *domains_by_id, current_domain, tmp) {
        HASH_DELETE(hh_domain_id, *domains_by_id, current_domain);
        HASH_DELETE(hh_domain_name, *domains_by_name, current_domain); // Remove from name hash too

        // Free nested connections
        ndw_free_connections(&current_domain->connections_by_id, &current_domain->connections_by_name);

        free(current_domain->domain_name);
        free(current_domain->domain_description);
        free(current_domain->debug_desc);
        free(current_domain); // Only once
    }
} // end method ndw_free_domains

void
ndw_CleanupAllDomainsConnectionsTopics()
{
    ndw_DomainHandle_T* dh = ndw_GetDomainHandleInternal();
    ndw_free_domains(&dh->g_domains_by_id, &dh->g_domains_by_name);
    dh->g_domains_by_id = 0;
    dh->g_domains_by_name = 0;
} // end method ndw_CleanupAllDomainsConnectionsTopics

/*
 * This method will return Connection and Domain POINTERS.
 * If force_exit is set and Connection and Domain POINTERS are NOT found,
 *  it will treat it as a serious issue and exit forcefully!
 * Returns 0 if both connection and domain POINTERS are set else < 0
 */
INT_T
ndw_GetConnectionDomain(const CHAR_T* method_name, bool force_exit,
                            ndw_Topic_T* t, ndw_Connection_T** connection, ndw_Domain_T** domain)  
{
    const CHAR_T* prefix = ((NULL == method_name) || ('\0' == *method_name)) ?
                                "ndw_GetConnectionDomain(): " : method_name;

    if (NULL == t) {
        NDW_LOGERR( "%s *** FATAL ERROR: ndw_Topic_T* parameter is NULL!\n", prefix);
        exit(EXIT_FAILURE); // Does not make sense to respect force_exit.
    }

    if (NULL == connection) {
        NDW_LOGERR( "*** FATAL ERROR: %s input parameter connection is NULL! For %s\n", prefix, t->debug_desc);
        exit(EXIT_FAILURE); // Does not make sense to respect force_exit.
    }

    ndw_Connection_T* c = t->connection;
    if (NULL == c) {
        NDW_LOGERR( "%s ndw_Connection_T* is NULL for %s\n", prefix, t->debug_desc);

        if (force_exit)
            exit(EXIT_FAILURE);
        else
            return -1;
    }

    *connection = c;

    if (NULL == domain) {
        NDW_LOGERR( "*** FATAL ERROR: %s input parameter domain is NULL! For %s\n", prefix, t->debug_desc);
        exit(EXIT_FAILURE); // Does not make sense to respect force_exit.
    }

    ndw_Domain_T* d = c->domain;
    if (NULL == d) {
        NDW_LOGERR( "%s ndw_Domain_T* set to NULL! For %s\n", prefix, t->debug_desc);

        if (force_exit)
            exit(EXIT_FAILURE);
        else
            return -2;
    }

    *domain = d;
    return 0;
} // end method ndw_GetConnectionDomain

