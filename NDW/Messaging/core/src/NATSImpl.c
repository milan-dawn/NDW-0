
#include "NATSImpl.h"

extern INT_T ndw_verbose; // Defined in NDW_Init.h, but we do not include that file.

// Never good to have multiple threads create multiple connections simultaneously.
pthread_mutex_t ndw_NATS_ConnectionLock = PTHREAD_MUTEX_INITIALIZER;
// And same for JetStream
pthread_mutex_t ndw_NATS_JS_ConnectionLock = PTHREAD_MUTEX_INITIALIZER;

// JetStream methods (externs as they were written at the bottom of the source code file.)
extern INT_T ndw_NATS_ProcessConfiguration(ndw_Topic_T*);
extern INT_T ndw_NATS_JSConnect(ndw_NATS_Connection_T*);
extern INT_T ndw_NATS_JSPublish(ndw_Topic_T*);
extern INT_T ndw_NATS_JSSubscribe(ndw_Topic_T*, bool push_mode);
extern bool ndw_NATS_IsJSPubSub(ndw_Topic_T*);
extern INT_T ndw_NATS_JSPollForMsg(ndw_Topic_T* topic, const CHAR_T** msg, INT_T* msg_length, LONG_T timeout_ms, void** vendor_closure);
extern INT_T ndw_NATS_CommitLastMsg(ndw_Topic_T* topic, void* vendor_closure);

extern INT_T ndw_NATS_Publish_ResponseForRequestMsg(ndw_Topic_T* topic);
extern INT_T ndw_NATS_GetResponseForRequestMsg(ndw_Topic_T* topic, const CHAR_T** msg, INT_T* msg_length,
                                LONG_T timeout_ms, void** vendor_closure);

typedef struct ndw_NATS_Closure
{
    LONG_T counter;                         // Incremental counter.
    ndw_Topic_T* topic;                     // Topic.
    ndw_NATS_Topic_T* nats_topic;           // NATS Topic
    ndw_NATS_Connection_T* nats_connection; // NATS connection.
    bool is_js;                             // Is it associated with JetStream operation?
    natsMsg* nats_msg;                      // NATS message received.
    const CHAR_T* user_msg;                 // User Message inside nats_msg.
    INT_T msg_size;                         // Message size.
} ndw_NATS_Closure_T;

void ndw_NATS_tls_closure_Destructor(void *ptr)
{
    free(ptr);
}

typedef struct ndw_NATS_TopicCxt
{
    ndw_Topic_T* topic;
    ndw_NATS_Topic_T* nats_topic;

    ndw_Connection_T* connection;
    ndw_NATS_Connection_T* nats_connection;
} ndw_NATS_TopicCxt_T;

void ndw_NATS_tls_populatecxt_Destructor(void *ptr)
{
    free(ptr);
}

pthread_key_t ndw_NATS_tls_closure; // Last message closure
pthread_once_t ndw_NATS_tls_closure_once = PTHREAD_ONCE_INIT;
static void ndw_NATS_tls_closure_Init()
{
    if (0 != pthread_key_create(&ndw_NATS_tls_closure, ndw_NATS_tls_closure_Destructor)) {
        NDW_LOGERR("*** FATAL ERROR: Failed to create ndw_NATS_tls_closure!\n");
        ndw_exit(EXIT_FAILURE);
    }
}

pthread_key_t ndw_NATS_tls_populatecxt; // Per thread memory for NATS_TopicCxt_T.
pthread_once_t ndw_NATS_tls_populatecxt_once = PTHREAD_ONCE_INIT;
static void ndw_NATS_tls_populatecxt_Init()
{
    if (0 != pthread_key_create(&ndw_NATS_tls_populatecxt, ndw_NATS_tls_populatecxt_Destructor)) {
        NDW_LOGERR("*** FATAL ERROR: Failed to create ndw_NATS_tls_populatecxt!\n");
        ndw_exit(EXIT_FAILURE);
    }
}

static ndw_NATS_TopicCxt_T*
ndw_NATS_PopulateCxt(const CHAR_T* filename, INT_T line_number, const CHAR_T* function_name, ndw_Topic_T* topic)
{
    if (NULL == topic) {
        ndw_print(filename, line_number, function_name, ndw_err_file, "*** FATAL ERROR *** ndw_Topic_T* is NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_TopicCxt_T* cxt = (ndw_NATS_TopicCxt_T*) pthread_getspecific(ndw_NATS_tls_populatecxt);

    if (NULL == cxt) {
        ndw_print(filename, line_number, function_name, ndw_err_file,
             "*** FATAL ERROR *** ndw_NATS_TopicCxt_T* is NULL for %s\n", cxt->topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    memset(cxt, 0, sizeof(ndw_NATS_TopicCxt_T));

    cxt->topic = topic;

    cxt->nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;
    if (NULL == cxt->nats_topic) {
        ndw_print(filename, line_number, function_name, ndw_err_file,
             "*** FATAL ERROR *** nats_topic from vendor_opaque is NULL for %s\n", cxt->topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }
 
    cxt->connection = topic->connection;
    if (NULL == cxt->connection) {
        ndw_print(filename, line_number, function_name, ndw_err_file,
             "*** FATAL ERROR *** ndw_Connection_T* is NULL for %s\n", cxt->topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    cxt->nats_connection = (ndw_NATS_Connection_T*) cxt->connection->vendor_opaque;
    if (NULL == cxt->nats_connection) {
        ndw_print(filename, line_number, function_name, ndw_err_file,
             "*** FATAL ERROR *** ndw_NATS_Connection_T* is NULL for %s\n", cxt->topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }
    
    return cxt;
} // end method ndw_NATS_PopulatCxt

#define NDW_NATS_POPULATECXT(topic) \
    ndw_NATS_PopulateCxt(__FILE__, __LINE__, CURRENT_FUNCTION, topic)

static ndw_NATS_Closure_T*
ndw_NATS_get_EmptyClosure(const char* file_name, int line_number, const char* function_name, ndw_Topic_T* topic)
{
    if (NULL == topic) {
        NDW_LOGERR("[%s, %d, %s]: ***FATAL ERROR: closure does NOT match with what is stored in TLS\n",
            file_name, line_number, function_name);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_Closure_T* c = (ndw_NATS_Closure_T*) pthread_getspecific(ndw_NATS_tls_closure);
    if (NULL == c) {
        c = (ndw_NATS_Closure_T*) calloc(1, sizeof(ndw_NATS_Closure_T));
        pthread_setspecific(ndw_NATS_tls_closure, c);
    }

    if (NULL != c->topic) {
        NDW_LOGERR("[%s, %d, %s]: ***FATAL ERROR: ndw_Topic_T Pointer is NOT NULL for %s\n",
            file_name, line_number, function_name, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL != c->nats_topic) {
        NDW_LOGERR("[%s, %d, %s]: ***FATAL ERROR: ndw_NATS_Topic_T Pointer is NOT NULL for %s\n",
            file_name, line_number, function_name, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL != c->nats_connection) {
        NDW_LOGERR("[%s, %d, %s]: ***FATAL ERROR: ndw_NATS_Connection_T Pointer is NOT NULL for %s\n",
            file_name, line_number, function_name, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL != c->nats_msg) {
        NDW_LOGERR("[%s, %d, %s]: ***FATAL ERROR: nats_msg Pointer is NOT NULL for %s\n",
            file_name, line_number, function_name, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL != c->user_msg) {
        NDW_LOGERR("[%s, %d, %s]: ***FATAL ERROR: user_msg Pointer is NOT NULL for %s\n",
            file_name, line_number, function_name, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (0 != c->msg_size) {
        NDW_LOGERR("[%s, %d, %s]: ***FATAL ERROR: msg_size<%d> is NOT zero for %s\n",
            file_name, line_number, function_name, c->msg_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Connection_T* connection = topic->connection;
    if (NULL == connection) {
        NDW_LOGERR("[%s, %d, %s]: ***FATAL ERROR: ndw_Connection_T  Pointer is NULL for %s\n",
            file_name, line_number, function_name, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;
    if (NULL == nats_topic) {
        NDW_LOGERR("[%s, %d, %s]: ***FATAL ERROR: ndw_NATS_Topic_T Pointer is NULL for %s\n",
            file_name, line_number, function_name, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_Connection_T* nats_connection = (ndw_NATS_Connection_T*) connection->vendor_opaque;

    c->topic = topic;
    c->nats_topic = nats_topic;
    c->nats_connection = nats_connection;
    c->nats_msg = NULL;
    c->user_msg = NULL;
    c->msg_size = 0;

    return c;
} // end method ndw_NATS_get_EmptyClosure

#define NDW_NATS_GET_EMPTY_CLOSURE(topic) \
    ndw_NATS_get_EmptyClosure(__FILE__, __LINE__, CURRENT_FUNCTION, topic)

static void
ndw_NATS_Clear_Closure(const char* file_name, int line_number, const char* function_name, ndw_Topic_T* topic)
{
    if (NULL == topic) {
        NDW_LOGERR("[%s, %d, %s]: ***FATAL ERROR: closure does NOT match with what is stored in TLS\n",
            file_name, line_number, function_name);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_Closure_T* c = (ndw_NATS_Closure_T*) pthread_getspecific(ndw_NATS_tls_closure);
    if (NULL == c) {
        return;
    }

    LONG_T counter = c->counter;
    memset(c, 0, sizeof(ndw_NATS_Closure_T));
    c->counter = counter;
}

#define NDW_CLEAR_CLOSURE(topic) \
    ndw_NATS_Clear_Closure(__FILE__, __LINE__, CURRENT_FUNCTION, topic)


static const CHAR_T*
ndw_GetNATSConnectionStatus(natsConnection *conn)
{
    natsConnStatus cs = natsConnection_Status(conn);
    switch (cs) {
        case NATS_CONN_STATUS_DISCONNECTED: return "*DISCONNECTED"; break;
        case NATS_CONN_STATUS_CONNECTING: return "CONNECING"; break;
        case NATS_CONN_STATUS_CONNECTED: return "CONNECTED"; break;
        case NATS_CONN_STATUS_CLOSED: return "*CLOSED"; break;
        case NATS_CONN_STATUS_RECONNECTING: return "*RECONNECTING"; break;
        case NATS_CONN_STATUS_DRAINING_SUBS: return "*DRAINING_SUBS"; break;
        case NATS_CONN_STATUS_DRAINING_PUBS: return "*DRAINING_PUBS"; break;
        default:
            return "UNKNOWN!";
    }

} // end method ndw_GetNATSConnectionStatus

static void
ndw_PrintNATSConnectionCallback(const CHAR_T* prefix, natsConnection* nc, void* closure)
{
    const CHAR_T* print_prefix = NDW_ISNULLCHARPTR(prefix) ? "?" : prefix;
    if ((NULL == nc) || (NULL == closure)) {
        NDW_LOGERR("%s\n", print_prefix);
        return;
    }

    const CHAR_T* name_for_closure = (const CHAR_T*) closure;
    if (NDW_ISNULLCHARPTR(name_for_closure))
        NDW_LOGERR("*** ALERT ==> %s\n", prefix);
    else
        NDW_LOGERR("*** ALERT ==> <%s> <== %s\n", prefix, name_for_closure);
} // end method ndw_PrintNATSConnectionCallback

static void
ndw_NATS_ConnectionClosedPermanently(natsConnection *nc, void* closure)
{
    ndw_PrintNATSConnectionCallback("Connection Permanently Closed! ", nc, closure);
} // end method ndw_NATS_ConnectionClosedPermanently

static void
ndw_NATS_ConnectionDisconnected(natsConnection *nc, void* closure)
{
    ndw_PrintNATSConnectionCallback("Connection got Disconnected! ", nc, closure);
} // end method ndw_NATS_ConnectionDisconnected

static void
ndw_NATS_ConnectionReconnected(natsConnection *nc, void* closure)
{
    ndw_PrintNATSConnectionCallback("Connection got Reconnected! ", nc, closure);
} // end method ndw_NATS_ConnectionDisconnected

static void
ndw_NATS_AsyncEventError(natsConnection *nc, natsSubscription* subscription,
                        natsStatus err, void* closure)
{
    if ((NULL == nc) || (NULL == closure))
        return;

    ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) subscription;
    if (NULL == nats_topic) {
        NDW_LOGERR("*** ALERT ==>AsyncEventError with NULL closure for subscription! With natsStatus<%d>\n", err);
        return;
    }

    ndw_Topic_T* topic = nats_topic->ndw_topic;
    if (NULL == topic) {
        NDW_LOGERR("*** ALERT ==> AsyncEventError with NULL ndw_Topic_T*! natsStatus<%d>\n", err);
        return;
    }

    ndw_Connection_T* connection = topic->connection;
    if (NULL == connection) {
        NDW_LOGERR("*** ALERT ==> AsyncEventError! natsStatus<%d> NOTE: ndw_Connection_T* is NULL! For %s\n",
                    err, topic->debug_desc);
        return;
    }

    ndw_Domain_T* domain = connection->domain;
    if (NULL == domain) {
        NDW_LOGERR("*** ALERT ==> AsyncEventError with natsStatus<%d>. NOTE: ndw_Domain_T* is NULL! For %s\n",
                    err, topic->debug_desc);
        return;
    }

    NDW_LOGERR("*** ALERT ==> AsyncEventError! natsStatus<%d> for %s\n", err, topic->debug_desc);
} // end method ndw_NATS_AsyncEventError


static void
ndw_NATS_Internal_AsyncMessageHandler(natsConnection *nc, natsSubscription *sub,
                                        natsMsg *msg, void *closure)
{
    const CHAR_T* user_msg = natsMsg_GetData(msg);
    INT_T msg_size = natsMsg_GetDataLength(msg);

    if (NULL == closure) {
        NDW_LOGERR( "*** FATAL ERROR: closure is NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_Topic_T* nats_t = (ndw_NATS_Topic_T*) closure;
    ndw_Topic_T* t = (NULL == nats_t) ? NULL : nats_t->ndw_topic;

    if (ndw_verbose > 4) {
        const CHAR_T* subject = (NULL == sub) ? "Unknown!" : natsSubscription_GetSubject(sub);
        CHAR_T* ip_address = NULL;
        natsConnection_GetClientIP(nc, &ip_address);
        NDW_LOGX("[Connection IP Address: <%s>] received message on subject <%s> msg_length: %d\n",
                        (NULL == ip_address) ? "?" : ip_address, subject, msg_size);
        free(ip_address);
    }

    ndw_NATS_Closure_T* vendor_closure = NDW_NATS_GET_EMPTY_CLOSURE(t);

    vendor_closure->is_js = false;
    vendor_closure->nats_topic = nats_t;
    vendor_closure->nats_connection = nats_t->nats_connection;
    vendor_closure->nats_msg = msg;
    vendor_closure->user_msg = user_msg;
    vendor_closure->msg_size = msg_size;
    vendor_closure->counter += 1;

     ndw_NATS_JS_Attr_T* js_attr = &(nats_t->nats_js_attr);

    if (js_attr->is_initialized && js_attr->is_enabled && js_attr->is_sub_enabled) {
        vendor_closure->is_js = true;
    } 

    ndw_HandleVendorAsyncMessage(t, (UCHAR_T*) user_msg, msg_size, vendor_closure);

    if (t->q_async_enabled) {
        return; // Do NOT destroy natsMsg
    }

} // ndw_NATS_Internal_AsyncMessageHandler


bool
ndw_is_really_NATS_connection(ndw_Connection_T* connection)
{
    if (NULL == connection) {
        return false;
    }

    if (NDW_IMPL_NATS_IO_ID != connection->vendor_id) {
        NDW_LOGERR( "*** ERROR: Being invoked with invalid vendor_id<%d> expected<%d>", connection->vendor_id, NDW_IMPL_NATS_IO_ID);
        return false;
    }

    if ((NULL == connection->vendor_name) || ('\0' == *(connection->vendor_name)) ||
        (0 != strcmp(connection->vendor_name, NDW_IMPL_NATS_IO_NAME))) {
        NDW_LOGERR( "*** ERROR: Being invoked with invalid vendor_name with vendor_id<%d> Expected<%d>", connection->vendor_id, NDW_IMPL_NATS_IO_ID);
        return false;
    }

    if (NDW_IMPL_NATS_LOGICAL_VERSION != connection->vendor_logical_version) {
        NDW_LOGERR( "*** ERROR: Being invoked with invalid vendor_version<%d> for vendor_id<%d> Expected<%d>",
            connection->vendor_id, connection->vendor_logical_version, NDW_IMPL_NATS_LOGICAL_VERSION);
        return false;
    }

    if ((NULL == connection->connection_url) || ('\0' == *(connection->connection_url))) {
        NDW_LOGERR( "*** ERROR: Connection URL not set vendor_id<%d, %d, %s>\n",
            connection->vendor_id, connection->vendor_logical_version, connection->vendor_name);
        return false;
        
    }

    return true;

} // ndw_is_really_NATS_connection

static LONG_T
ndw_NATS_ParseConnectionOptions(ndw_Connection_T* connection, const CHAR_T* option_name, bool* value_exists)
{
    if (NULL == connection) {
        NDW_LOGERR("*** FATAL ERROR: NULL POINTER ndw_Connection_T*\n");
        ndw_exit(EXIT_FAILURE);
    }


    if (NDW_ISNULLCHARPTR(option_name)) {
        NDW_LOGERR("*** FATAL ERROR: NULL option_name for %s\n", connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == value_exists) {
        NDW_LOGERR("*** FATAL ERROR: NULL value_exists* for %s\n", connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    *value_exists = false;

    ndw_NATS_Connection_T* conn = (ndw_NATS_Connection_T*) connection->vendor_opaque;
    if (NULL == conn) {
        NDW_LOGERR("*** FATAL ERROR: NULL ndw_NATS_Connection_T* for %s\n", connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    LONG_T value = -1;

    // Parse publication back of bytes to throttle publishing.
    const CHAR_T* value_ptr = ndw_GetNVPairValue(option_name, &(connection->vendor_connection_options_nvpairs));
    if (! NDW_ISNULLCHARPTR(value_ptr)) {
            if (ndw_atol(value_ptr, &value))
                *value_exists = true;
    }

    return value;
} // end method ndw_NATS_ParseConnectionOptions

INT_T
ndw_NATS_ConnectWithLock(ndw_Connection_T* connection)
{
    if (NULL == connection) {
        NDW_LOGERR( "*** ERROR: Invalid connection POINTER!\n");
        return -1;
    }

    if (connection->disabled)
        return 0;

    if (! ndw_is_really_NATS_connection(connection)) {
        NDW_LOGERR("*** ERROR: NOT a NATS connection but being asked to connect!");
        return -2;
    }

    ndw_Domain_T* domain = connection->domain;
    if (NULL == domain) {
        NDW_LOGERR("*** FATAL ERROR: ndw_domain_T* POINTER not set in connection for %s\n", connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    const CHAR_T* url = connection->connection_url;

    ndw_NATS_Connection_T* conn = (ndw_NATS_Connection_T*) connection->vendor_opaque;
    if (NULL == conn) {
        NDW_LOGERR("*** FATAL ERROR: ndw_NATS_Connection_T* NOT set in connection->vendor_opaque for %s\n", connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL != conn->conn) {
        bool b = ndw_NATS_IsConnected(connection);
        if (b) {
            return 0; // Already connected.
        }

        b = natsConnection_IsReconnecting(conn->conn);
        if (b) {
            NDW_LOGERR("*** WARNING: IsReconnecting on %s\n", connection->debug_desc);
            return 0;
        }

        b = natsConnection_IsDraining(conn->conn);
        if (b) {
            NDW_LOGERR("*** WARNING: Connection is closing and Draining on %s ! Try again... !\n", connection->debug_desc);
            return -3;
        }
    }

    if (ndw_verbose) {
        NDW_LOG("Tring to Connect to URL <%s> for %s\n", url, connection->debug_desc);
    }


    natsOptions *nats_options;
    natsOptions_Create(&nats_options);

    const CHAR_T* name_for_closure = ndw_Concat_Strings(
        domain->domain_name, connection->connection_unique_name, "^");

    if (NATS_OK != natsOptions_SetName(nats_options, name_for_closure)) {
        NDW_LOGERR("*** FATAL ERROR: natsOptions_SetName failed for %s\n", connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    free((CHAR_T*) name_for_closure);

    if (NATS_OK != natsOptions_SetClosedCB(nats_options, ndw_NATS_ConnectionClosedPermanently, NULL)) {
        NDW_LOGERR("*** FATAL ERROR: FAILED to set callback for natsOptions_SetClosedCB\n");
        ndw_exit(EXIT_FAILURE);
    }

    if (NATS_OK != natsOptions_SetDisconnectedCB(nats_options, ndw_NATS_ConnectionDisconnected, NULL))
    {
        NDW_LOGERR("*** FATAL ERROR: FAILED to set callback for natsOptions_SetDisconnectedCB\n");
        ndw_exit(EXIT_FAILURE);
    }

    if (NATS_OK != natsOptions_SetReconnectedCB(nats_options, ndw_NATS_ConnectionReconnected, NULL))
    {
        NDW_LOGERR("*** FATAL ERROR: FAILED to set callback for natsOptions_SetReconnectedCB\n");
        ndw_exit(EXIT_FAILURE);
    }

    if (NATS_OK != natsOptions_SetURL(nats_options, url)) {
        NDW_LOGERR("*** FATAL ERROR: natsOptions_setURL failed for url<%s>\n", url);
        ndw_exit(EXIT_FAILURE);
    }

    if (NATS_OK != natsOptions_SetIOBufSize(nats_options, (INT_T) conn->iobuf_size)) {
        NDW_LOGERR("*** FATAL ERROR: natsOptions_SetIOBufSize failed for value<%ld>\n", conn->iobuf_size);
        ndw_exit(EXIT_FAILURE);
    }

    if (conn->connection_timeout > 0) {
        if (NATS_OK != natsOptions_SetTimeout(nats_options, conn->connection_timeout)) {
            NDW_LOGERR("*** FATAL ERROR: natsOptions_SetTimeout failed for value<%ld>\n",
                        conn->connection_timeout);
            ndw_exit(EXIT_FAILURE);
        }
        else {
            NDW_LOGX("NOTE: natsOptions_SetTimeout with value<%ld>\n", conn->connection_timeout);
        }
    }

    if (! NDW_ISNULLCHARPTR(connection->connection_unique_name)) {
        if (NATS_OK != natsOptions_SetName(nats_options, connection->connection_unique_name)) {
            NDW_LOGERR("*** FATAL ERROR: natsOptions_setName failed for value<%s>\n",
                        connection->connection_unique_name);
            ndw_exit(EXIT_FAILURE);
        }
        else {
            NDW_LOGX("NOTE: natsOptions_SetName with value<%s>\n", connection->connection_unique_name);
        }
    }

    if (conn->connection_max_reconnects > 0) {
        if (NATS_OK != natsOptions_SetMaxReconnect(nats_options, conn->connection_max_reconnects)) {
            NDW_LOGERR("*** FATAL ERROR: natsOptions_SetMaxReconnect failed for value<%ld>\n",
                        conn->connection_max_reconnects);
            ndw_exit(EXIT_FAILURE);
        }
        else {
            NDW_LOGX("NOTE: natsOptions_SetMaxReconnect with value<%ld>\n", conn->connection_max_reconnects);
        }
    }

    if (conn->connection_reconnection_wait > 0) {
        if (NATS_OK != natsOptions_SetReconnectWait(nats_options, conn->connection_reconnection_wait)) {
            NDW_LOGERR("*** FATAL ERROR: natsOptions_SetReconnectWait failed for value<%ld>\n",
                        conn->connection_reconnection_wait);
            ndw_exit(EXIT_FAILURE);
        }
        else {
            NDW_LOGX("NOTE: natsOptions_SetReconnectWait with value<%ld>\n", conn->connection_reconnection_wait);
        }
    }

    if (conn->connection_no_echo) {
        if (NATS_OK != natsOptions_SetNoEcho(nats_options, true)) {
            NDW_LOGERR("*** FATAL ERROR: natsOptions_SetNoEcho failed for value<%ld>\n",
                        conn->connection_reconnection_wait);
            ndw_exit(EXIT_FAILURE);
        }
        else {
            NDW_LOGX("NOTE: WARNING: natsOptions_SetNoEcho as TRUE!\n");
        }
    }

    if (ndw_verbose > 1) {
        NDW_LOGX("---> NATS: Initiating NATS Connection: %s\n", connection->debug_desc);
    }

    //natsSockCtx* socket_ctx = &(conn->conn->sockCtx);
    conn->status = natsConnection_Connect(&conn->conn, nats_options);

    if (NATS_OK != conn->status) {
        NDW_LOGERR("*** WARNING: FAILED to Connect with NATS with URL<%s> Error Status %s for %s\n",
                    url, natsStatus_GetText(conn->status), connection->debug_desc);

        natsConnection_Destroy(conn->conn); // Assumption is that NATS is delete that memory!
        conn->conn = NULL;
        natsOptions_Destroy(nats_options);
        return -4;
    }
    else {
        // Also initialize JetStream if configured.
        if (conn->js_enabled_count > 0) {
            INT_T js_ret_code = ndw_NATS_JSConnect(conn);
            if (0 != js_ret_code) {
                NDW_LOGERR("*** ERROR: FAILED to Connect with JetStream with NATS using URL<%s> for %s\n",
                            url, connection->debug_desc);
            }
        }

        natsOptions_Destroy(nats_options);
        return 0;
    }
} // end method ndw_NATS_ConnectWithLock

INT_T
ndw_NATS_Connect(ndw_Connection_T* connection)
{
    pthread_mutex_lock(&ndw_NATS_ConnectionLock);
    INT_T ret_code = ndw_NATS_ConnectWithLock(connection);
    pthread_mutex_unlock(&ndw_NATS_ConnectionLock);
    return ret_code;
} // end method ndw_NATS_Connect

INT_T
ndw_NATS_DisconnectWithLock(ndw_Connection_T* connection)
{
    if (NULL == connection) {
        NDW_LOGERR( "*** ERROR: ndw_NATS_Disconnect(): Invalid connection POINTER!\n");
        return -1;
    }

    if (! ndw_is_really_NATS_connection(connection))
        return -2;

    const CHAR_T* url = connection->connection_url;

    ndw_NATS_Connection_T* conn = (ndw_NATS_Connection_T*) connection->vendor_opaque;
    if (NULL == conn) {
        NDW_LOGERR("*** ERROR: No natsConnection data structure. Was this ever connected for %s?\n", connection->debug_desc);
        return -3;
    }

    if (NULL != conn->conn)
    {
        bool b = natsConnection_IsDraining(conn->conn);
        if (b) {
            INT_T timeout_millis = 1000 * 1000;
            usleep(timeout_millis);
            b = natsConnection_IsDraining(conn->conn);
            if (b) {
                NDW_LOGERR("** WARNING: Connection URL<%s> is closing and Draining! Forcefully disconnecting!! For %s\n",
                   url, connection->debug_desc);
            }
        }

        b = natsConnection_IsReconnecting(conn->conn);
        if (b) {
                NDW_LOGERR( "** WARNING: Connection URL<%s> is Reconnecting! Forcefully disconnecting!! For %s\n",
                    url, connection->debug_desc);
        }

        INT_T total_topics = 0;
        ndw_Topic_T** topics = ndw_GetAllTopicsFromConnection(connection, &total_topics);
        if ((NULL != topics) && (total_topics > 0)) {
           for (INT_T topic_count = 0; topic_count < total_topics; topic_count++) {
                ndw_Topic_T* topic = topics[topic_count];
                if (NULL == topic)
                    break;

                ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;
                if (NULL == nats_topic)
                    continue;
                ndw_NATS_Unsubscribe(topic);
           } 
        }

        free(topics);

        // Flush out connection.
        if (conn->flush_timeout_ms > 0) {
            natsStatus flush_code = natsConnection_FlushTimeout(conn->conn, (LONG_T) conn->flush_timeout_ms);
            if (NATS_OK == flush_code) {
                NDW_LOG("natsConnection_FlushTimeout(<%ld> milliseconds) Okay for %s!\n",
                    conn->flush_timeout_ms, connection->debug_desc);
            } else {
                NDW_LOGERR("*** ERROR: natsConnection_FlushTimeout(<%ld> milliseconds) FAILED with natsStatus<%d> for %s!\n",
                    conn->flush_timeout_ms, flush_code, connection->debug_desc);
            }
        }

        if (ndw_verbose > 1) {
            NDW_LOGX("---> NATS: Disconnecting from %s\n", connection->debug_desc);
        }

        natsConnection_Destroy(conn->conn); // Assumption is that NATS is delete that memory!
        conn->conn = NULL;
        conn->disconnect_time = ndw_GetCurrentUTCNanoseconds();

        if (ndw_verbose) {
            NDW_LOGX("==> ndw_NATS_DisConnect(): URL<%s> <== Disconnected for %s!\n",
                    connection->connection_url, connection->debug_desc);
        }
        
        ndw_Connection_T* ndw_connection = conn->ndw_connection;
        if (ndw_connection != connection) {
            NDW_LOGERR( "===> *** ERROR *** PLEASE CHECK: ndw_Connection_T* NOT equal to vendor opaque structure ndw_NATS_Connection_T* structure! for %s\n", connection->debug_desc);
        }
    }

    if (NULL != conn->js_context) {
        jsCtx_Destroy(conn->js_context);
        conn->js_context = NULL;
    }

    conn->conn = NULL;

    return 0;
} // end method ndw_NATS_DisconnectWithLock

INT_T
ndw_NATS_Disconnect(ndw_Connection_T* connection)
{
    pthread_mutex_lock(&ndw_NATS_ConnectionLock);
    INT_T ret_code = ndw_NATS_DisconnectWithLock(connection);
    pthread_mutex_unlock(&ndw_NATS_ConnectionLock);
    return ret_code;
} // end method ndw_NATS_Disconnect


bool
ndw_NATS_IsConnected(ndw_Connection_T* connection)
{
    if (NULL == connection) {
        NDW_LOGERR( "*** ERROR: ndw_NATS_IsConnected(): Invalid connection POINTER!\n");
        return false;
    }

    if (connection->disabled)
        return false;

    const CHAR_T* url = connection->connection_url;
    if ((NULL == url) || ('\0' == *url)) {
        NDW_LOGERR( "*** ERROR: Invalid connection URL! For %s\n", connection->debug_desc);
        return false;
    }

    ndw_NATS_Connection_T* nats_connection = (ndw_NATS_Connection_T*) connection->vendor_opaque;
    if (NULL == nats_connection) {
        return false;
    }

    if (NULL == nats_connection->conn) {
        return false;
    }

    bool b = natsConnection_IsClosed(nats_connection->conn);

    return (!b);
} // end method ndw_NATS_IsConnected

bool
ndw_NATS_IsClosed(ndw_Connection_T* connection)
{
    if (NULL == connection) {
        NDW_LOGERR( "*** ERROR: ndw_NATS_IsClosed(): Invalid connection POINTER!\n");
        return false;
    }

    if (connection->disabled)
        return true;

    const CHAR_T* url = connection->connection_url;
    if ((NULL == url) || ('\0' == *url)) {
        NDW_LOGERR( "*** ERROR: Invalid connection URL for %s!\n", connection->debug_desc);
        return false;
    }

    ndw_NATS_Connection_T* nats_connection = (ndw_NATS_Connection_T*) connection->vendor_opaque;
    if (NULL == nats_connection) {
        return false;
    }

    if (NULL == nats_connection->conn) {
        return false;
    }

    bool b = natsConnection_IsClosed(nats_connection->conn);

    return b;
} // end method ndw_NATS_IsClosed

bool
ndw_NATS_IsDraining(ndw_Connection_T* connection)
{
    if (NULL == connection) {
        NDW_LOGERR( "*** ERROR: ndw_NATS_IsDraining(): Invalid connection POINTER!\n");
        return false;
    }

    if (connection->disabled)
        return false;

    const CHAR_T* url = connection->connection_url;
    if ((NULL == url) || ('\0' == *url)) {
        NDW_LOGERR( "*** ERROR: Invalid connection URL for %s!\n", connection->debug_desc);
        return false;
    }

    ndw_NATS_Connection_T* nats_connection = (ndw_NATS_Connection_T*) connection->vendor_opaque;
    if (NULL == nats_connection) {
        return false;
    }

    if (NULL == nats_connection->conn) {
        return false;
    }

    bool b = natsConnection_IsDraining(nats_connection->conn);

    return b;
} // end method 


INT_T
ndw_NATS_ThreadInit(ndw_ImplAPI_T* impl, INT_T vendor_id)
{
    (void) impl;
    (void) vendor_id;

    pthread_once(&ndw_NATS_tls_populatecxt_once, ndw_NATS_tls_populatecxt_Init);
    pthread_setspecific(ndw_NATS_tls_populatecxt, calloc(1, sizeof(ndw_NATS_TopicCxt_T)));

    pthread_once(&ndw_NATS_tls_closure_once, ndw_NATS_tls_closure_Init);
    pthread_setspecific(ndw_NATS_tls_closure, calloc(1, sizeof(ndw_NATS_Closure_T)));

    return 0;
} // end method ndw_NATS_ThreadInit

INT_T
ndw_NATS_ThreadExit()
{
    pthread_t this_thread = pthread_self();
    if (ndw_verbose > 2) {
        NDW_LOGX("TLS: Deleting... \n");
    }

    if (NDW_IS_NDW_INIT_THREAD(this_thread)) {
        ndw_NATS_TopicCxt_T* cxt = (ndw_NATS_TopicCxt_T*) pthread_getspecific(ndw_NATS_tls_populatecxt);
        if (NULL != cxt) {
            free(cxt);
        }
        else {
            if (ndw_verbose > 2) {
                NDW_LOGX("TLS: *** WARNING: ndw_NATS_TopicCxt_T is already NULL!\n");
            }
        }
    }
    ndw_safe_PTHREAD_KEY_DELETE("ndw_NATS_tls_populatecxt", ndw_NATS_tls_populatecxt);

    if (NDW_IS_NDW_INIT_THREAD(this_thread)) {
        ndw_NATS_Closure_T* vendor_closure = (ndw_NATS_Closure_T*) pthread_getspecific(ndw_NATS_tls_closure);
        if (NULL != vendor_closure) {
            free(vendor_closure);
        }
        else {
            if (ndw_verbose > 2) {
                NDW_LOGX("TLS: *** WARNING: ndw_NATS_Closure_T is already NULL!\n");
            }
        }
    }
    ndw_safe_PTHREAD_KEY_DELETE("ndw_NATS_tls_populatecxt", ndw_NATS_tls_closure);

    return 0;
} // end method ndw_NATS_ThreadInit

INT_T
ndw_NATS_Init(ndw_ImplAPI_T* impl, INT_T vendor_id)
{
    if (ndw_verbose) {
        NDW_LOGX("NATS Client Library Version <%s>\n", nats_GetVersion());
        NDW_LOGX("ndw_NATS_Init() vendor_id <%d>...\n", vendor_id);
    }

    if (NULL == impl) {
        NDW_LOGERR( "*** FATAL ERROR: NULL ndw_ImplAPI_T POINTER\n");
        ndw_exit(EXIT_FAILURE);
    }

    if (NDW_IMPL_NATS_IO_ID != impl->vendor_id) {
        NDW_LOGERR( "*** FATAL ERROR: Invalid ask to Init derivation as id<%d> and Expected <%d>\n", impl->vendor_id, NDW_IMPL_NATS_IO_ID);
        ndw_exit(EXIT_FAILURE);
    }

    if ((NULL == impl->vendor_name) || ('\0' == *(impl->vendor_name))) {
        NDW_LOGERR( "*** FATAL ERROR: Invalid ask to Init derivation for id<%d> but vendor_name is NULL!\n", impl->vendor_id);
        ndw_exit(EXIT_FAILURE);
    }

    if (0 != strcmp(impl->vendor_name, NDW_IMPL_NATS_IO_NAME)) {
        NDW_LOGERR( "*** FATAL ERROR: Invalid ask to Init derivation for id<%d> as name<%s> while Expected name<%s>\n",
                impl->vendor_id, impl->vendor_name, NDW_IMPL_NATS_IO_NAME);
        ndw_exit(EXIT_FAILURE);
    }

#if 0
    /// XXX: Since ThreadInit is called before NATS Init.
    ndw_NATS_ThreadInit(impl, vendor_id);
#endif

    impl->ProcessConfiguration = ndw_NATS_ProcessConfiguration;
    impl->Connect = ndw_NATS_Connect;
    impl->Disconnect = ndw_NATS_Disconnect;
    impl->ShutdownConnection = ndw_NATS_ShutdownConnection;
    impl->IsConnected = ndw_NATS_IsConnected;
    impl->IsClosed = ndw_NATS_IsClosed;
    impl->IsDraining = ndw_NATS_IsDraining;
    impl->PublishMsg = ndw_NATS_PublishMsg;
    impl->SubscribeAsync = ndw_NATS_SubscribeAsync;
    impl->Unsubscribe = ndw_NATS_Unsubscribe;
    impl->GetQueuedMsgCount = ndw_NATS_GetQueuedMsgCount;
    impl->SubscribeSynchronously = ndw_NATS_SubscribeSynchronously;
    impl->SynchronousPollForMsg = ndw_NATS_SynchronousPollForMsg;
    impl->Publish_ResponseForRequestMsg = ndw_NATS_Publish_ResponseForRequestMsg;
    impl->GetResponseForRequestMsg = ndw_NATS_GetResponseForRequestMsg;
    impl->CommitLastMsg = ndw_NATS_CommitLastMsg;
    impl->CommitQueuedMsg = ndw_NATS_CommitQueuedMsg;
    impl->CleanupQueuedMsg = ndw_NATS_CleanupQueuedMsg;

    if (ndw_verbose) {
        NDW_LOGX("===> NATS.io Init Derivation complete for id<%d> name<%s> logical_version<%d>\n",
                impl->vendor_id, impl->vendor_name, impl->vendor_logical_version);

    }

    // NOTE: If all connections are disabled then you have not initialized NATS.
    // So when you close it there is a pthread object leak per valgrind!
    nats_Open(-1);

    return 0;
} // end method ndw_NATS_Init

void
ndw_NATS_ShutdownConnection(ndw_Connection_T* connection)
{
    if ((NULL == connection) || (! ndw_is_really_NATS_connection(connection)))
        return;

    ndw_NATS_Connection_T* nats_connection = (ndw_NATS_Connection_T*) connection->vendor_opaque;
    if (NULL == nats_connection)
        return;

    if (NULL != nats_connection->conn)
        ndw_NATS_Disconnect(connection);

    INT_T total_topics = 0;
    ndw_Topic_T** topics = ndw_GetAllTopicsFromConnection(connection, &total_topics);

    if ((NULL == topics) || (total_topics <= 0)) {
        nats_connection->ndw_connection = NULL;
        free(nats_connection);
        return;
    }

    for (INT_T topic_count = 0; topic_count < total_topics; topic_count++) {
        ndw_Topic_T* topic = topics[topic_count];
        if (NULL == topic)
            break;

        ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;
        if (NULL == nats_topic)
            continue;

        topic->vendor_opaque = NULL;
        free(nats_topic->nats_js_attr.debug_desc);
        nats_topic->nats_js_attr.debug_desc = NULL;
        free(nats_topic);
    }

    free(topics);

    connection->vendor_opaque = NULL;
    free(nats_connection);
} // ndw_NATS_ShutdownConnection

void
ndw_NATS_Shutdown()
{
    NDW_LOGX("===> ndw_NATS_Shutdown() ...\n");

    NDW_LOGX("First invoking nats_CloseAndWait...\n");
    nats_CloseAndWait(5000);

    NDW_LOGX("Now invoking nats_Close...\n");
    nats_Close();

#if 0
    //
    // NOTE: This is kept here for awareness!
    // Do NOT do this as the shutdown code might need access to a pthread_key_t
    // To free per thread data structure we MUST rely on a seperate call after shutdown to do the cleanup!
    //
    ndw_NATS_ThreadExit();
#endif
} // end method ndw_NATS_Shutdown

void ndw_NATS_derived_init(void) __attribute__((constructor));
void
ndw_NATS_derived_init()
{
    ndw_ImplAPI_T* impl = get_Implementation_API(NDW_IMPL_NATS_IO_ID); 
    if (NULL != impl)
    {
        impl->Init = ndw_NATS_Init;
        impl->Shutdown = ndw_NATS_Shutdown;

        impl->ThreadInit = ndw_NATS_ThreadInit;
        impl->ThreadExit = ndw_NATS_ThreadExit;

        impl->vendor_id = NDW_IMPL_NATS_IO_ID;
        impl->vendor_name = NDW_IMPL_NATS_IO_NAME;
        impl->vendor_logical_version = NDW_IMPL_NATS_LOGICAL_VERSION;
    }
} // end method ndw_NATS_derived_init

static void
ndw_Build_JSAttributes_String(ndw_Topic_T* topic)
{
    ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;
    if (NULL == nats_topic) {
        NDW_LOGERR("*** FATAL ERROR Cannot print ndw_JS_Attr_T as ndw_NATS_Topic_T* is NULL for %s\n",
            topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_JS_Attr_T* attr = &(nats_topic->nats_js_attr);
    attr->debug_desc = ndw_ConcatStrings(" <-- JetStream Attributes: Initialized<",
        attr->is_initialized ? "TRUE" : "false",
         "> Enabled<", attr->is_enabled ? "TRUE" : "false",
         "> ConfigErr<", attr->is_config_error ? "TRUE" : "false",
         "> Publish<", attr->is_pub_enabled ? "TRUE" : "false",
         "> Subscribe<", attr->is_sub_enabled ? "TRUE" : "false",
         "> PUSH<", attr->is_push_mode ? "TRUE" : "false",
         "> pull<", attr->is_pull_mode ? "TRUE" : "false",
         "> StreamName<", NDW_ISNULLCHARPTR(attr->stream_name) ? "???" : attr->stream_name,
         "> StreamSubjectName<", NDW_ISNULLCHARPTR(attr->stream_subject_name) ? "???" : attr->stream_subject_name,
         "> DurableName<", NDW_ISNULLCHARPTR(attr->durable_name) ? "???" : attr->durable_name,
         "> Filter<\"", NDW_ISNULLCHARPTR(attr->filter) ? "???" : attr->filter,
         "\">", NULL);

    if (attr->is_initialized && (! attr->is_config_error) && (attr->is_enabled)) {
        CHAR_T* prev_debug_desc = topic->debug_desc;
        topic->debug_desc = ndw_ConcatStrings(prev_debug_desc, attr->debug_desc, NULL);
        free(prev_debug_desc);
    }

} // end method ndw_Build_JSAttributes_String


static void
ndw_NATS_PrintJSAttr(ndw_Topic_T* topic)
{
    if (ndw_verbose <= 0) {
        return;
    }

    if (NULL == topic) {
        NDW_LOGERR("*** ERROR Cannot print ndw_JS_Attr_T as input parameter topic is NULL!\n");
        return;
    }

    ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;
    if (NULL == nats_topic) {
        NDW_LOGERR("*** ERROR Cannot print ndw_JS_Attr_T as ndw_NATS_Topic_T* is NULL for %s\n", topic->debug_desc);
        return;
    }

    ndw_NATS_JS_Attr_T* js_attr = &(nats_topic->nats_js_attr);

    NDW_LOG("ndw_NATS_JS_Attr_T for %s Attributes: is_initialized<%s> is_config_err<%s> is_enabled<%s> "
            "is_pub_enabled<%s> is_sub_enabled<%s> is_push_mode<%s> is_pull_mode<%s>\n"
            "stream_name<%s> stream_subject_name<%s> durable_name<%s> filter_name<%s>\n",
                topic->debug_desc,
                js_attr->is_initialized ? "TRUE!" : "false",
                js_attr->is_config_error ? "*** TRUE ***" : "false",
                js_attr->is_enabled ? "TRUE!" : "false",
                js_attr->is_pub_enabled ? "TRUE!" : "false",
                js_attr->is_sub_enabled ? "TRUE!" : "false",
                js_attr->is_push_mode ? "TRUE!" : "false",
                js_attr->is_pull_mode ? "TRUE!" : "false",
                NDW_ISNULLCHARPTR(js_attr->stream_name) ? "?" : js_attr->stream_name,
                NDW_ISNULLCHARPTR(js_attr->stream_subject_name) ? "?" : js_attr->stream_subject_name,
                NDW_ISNULLCHARPTR(js_attr->durable_name) ? "?" : js_attr->durable_name,
                NDW_ISNULLCHARPTR(js_attr->filter) ? "?" : js_attr->filter);
    
} // end method ndw_NATS_PrintJSAttr

static INT_T
ndw_NATS_PublicationBackoff(ndw_NATS_Connection_T* conn)
{
    if (NULL == conn) {
        NDW_LOGERR("*** ERROR FATAL: ndw_NATS_Connection* conn parameter is NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Connection_T* connection = conn->ndw_connection;
    if (NULL == connection) {
        NDW_LOGERR("*** ERROR FATAL: ndw_Connection* POINTER is NULL inside of ndw_NATS_Connection_T*\n");
        ndw_exit(EXIT_FAILURE);
    }

    NDW_LOGX("Trying backoff on %s flush_timeout_ms<%ld> publication_backoff_bytes<%ld> "
                "backoff_sleep_us<%ld> backoff_attempts<%ld> ...... for %s\n",
                conn->ndw_connection->debug_desc, conn->flush_timeout_ms, conn->publication_backoff_bytes,
                conn->backoff_sleep_us, conn->backoff_attempts, connection->debug_desc);

    INT_T buffered = 0;
    LONG_T attempts = 0;
    natsStatus flush_code;
    INT_T sleep_us = (INT_T) conn->backoff_sleep_us;
    while (attempts < conn->backoff_attempts)
    {
        buffered = natsConnection_Buffered(conn->conn);

        if (buffered < ((INT_T) conn->publication_backoff_bytes)) {
            NDW_LOGX("Attempt[%ld] ==> buffered<%d> backoff at<%ld> diff<%ld> for %s\n",
                attempts, buffered, conn->publication_backoff_bytes, (conn->publication_backoff_bytes - buffered), connection->debug_desc);
            break;
        }

        usleep((sleep_us <= 0) ? 100 : sleep_us);

        if (conn->flush_timeout_ms > 0)
        {
            flush_code = natsConnection_FlushTimeout(conn->conn, (LONG_T) conn->flush_timeout_ms);
            if (NATS_OK == flush_code) {
                NDW_LOGX("Attempt[%ld] => natsConnection_FlushTimeoutMs(<%ld> milliseconds) Okay! For %s\n",
                            attempts, conn->flush_timeout_ms, connection->debug_desc);
            } else {
                NDW_LOGERR("**** ERROR: Attempt[%ld] natsConnection_FlushTimeoutMs(<%ld> milliseconds) FAILED with natsStatus<%d>!\n For %s",
                    attempts, conn->flush_timeout_ms, flush_code, connection->debug_desc);
            }
        }

        ++attempts;
        conn->total_backoff_attempts += 1;
    } // end while attempts ...

    NDW_LOGX("Completed Attempts<%ld> backoff with flush_timeout_ms<%ld> publication_backoff_bytes<%ld> "
                "backoff_sleep_us<%ld> backoff_attempts<%ld> for %s......\n", attempts,
                conn->flush_timeout_ms, conn->publication_backoff_bytes,
                conn->backoff_sleep_us, conn->backoff_attempts, connection->debug_desc);

    return 0;
} // end method ndw_NATS_PublicationBackoff

INT_T
ndw_NATS_Publish_Internal(const char* subject)
{
    ndw_OutMsgCxt_T* cxt_msg = ndw_GetOutMsgCxt();
    if (NULL == cxt_msg) {
        NDW_LOGERR("*** FATAL ERROR: ndw_GetOutMsg() returned NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Topic_T* t = cxt_msg->topic;
    ndw_NATS_TopicCxt_T* cxt_topic = NDW_NATS_POPULATECXT(t);

    if (t->disabled || t->connection->disabled)
        return 0;

    ndw_Connection_T* c = cxt_topic->connection;

    if (c->disabled)
        return 0;

    UCHAR_T* start_address = (UCHAR_T*) cxt_msg->header_address;
    INT_T header_size = cxt_msg->header_size;
    //UCHAR_T* message_address = cxt_msg->message_address;
    INT_T msg_size = cxt_msg->message_size;
    INT_T total_size = header_size + msg_size;

    if (! ndw_NATS_IsConnected(c)) {
        NDW_LOGTOPICERRMSG("Connection was NOT established (before)!", t);
        return -3;
    }

    ndw_NATS_Connection_T* nats_connection = (ndw_NATS_Connection_T*) c->vendor_opaque;
    if (NULL == nats_connection) {
        NDW_LOGERR("*** WARNING: ndw_NATS_Connection_T* not yet allocated for %s\n", t->debug_desc);
        return -4;
    }

    if (! t->is_pub_enabled) {
        NDW_LOGERR("*** WARNING: Topic is NOT enabled for Publication! %s\n", t->debug_desc);
        return -5;
    }

    if (NDW_ISNULLCHARPTR(subject)) {
        NDW_LOGERR("*** FATAL ERROR: pub_key is NULL in Topic! %s\n", t->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (cxt_msg->loopback_test) {
        // Loopback testing.
        return ndw_HandleVendorAsyncMessage(t, (UCHAR_T*) start_address, total_size, NULL);
    }


    ndw_NATS_Topic_T* nats_topic = cxt_topic->nats_topic;
    ndw_NATS_JS_Attr_T* js_attr = &(nats_topic->nats_js_attr);

    if (js_attr->is_initialized && js_attr->is_enabled) {
        INT_T ret_code = ndw_NATS_JSPublish(t);   // Handle JetStream specific publication.
        if (0 != ret_code) {
            NDW_LOGERR("*** ERROR: Failed to publish on JetStream for %s\n", t->debug_desc);
        }

        return ret_code;
    }

    INT_T max_tries = 2;
    INT_T ret_code = -9;
    for (INT_T i = 0; i < max_tries; i++)
    {
        natsStatus status = natsConnection_Publish( nats_connection->conn, subject,
                                                (const void*) start_address, (INT_T) total_size);
        if (NATS_OK == status) {
            nats_topic->total_messages_published += 1;
            ret_code = 0;
            break; // Successful Publish!
        }

        const CHAR_T* connection_status = ndw_GetNATSConnectionStatus(nats_connection->conn);
        NDW_LOGERR( "*** WARNING: Publish FAILED with status<%d> and ConnectionStatus<%s> for TopicSequenceNumber<%ld> for %s\n",
                     status, connection_status, t->sequence_number, t->debug_desc);

        ndw_NATS_PublicationBackoff(nats_connection);
    }

    return ret_code;
} // end method ndw_NATS_PublishMsg()

INT_T
ndw_NATS_PublishMsg()
{
    ndw_OutMsgCxt_T* cxt_msg = ndw_GetOutMsgCxt();
    if (NULL == cxt_msg) {
        NDW_LOGERR("*** FATAL ERROR: ndw_GetOutMsg() returned NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Topic_T* t = cxt_msg->topic;

    if (NULL == t) {
        NDW_LOGERR("*** FATAL ERROR: new_Topic_T* is NULL in ndw_OutMsgCxt_T*\n");
        ndw_exit(EXIT_FAILURE);
    }

    return ndw_NATS_Publish_Internal(t->pub_key);
} // end method ndw_NATS_PublishMsg()

static INT_T
ndw_NATS_SetSubscriptionOptions(ndw_Topic_T* topic)
{
    ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;
    if (NULL == nats_topic) {
        return 0;
    }

    if (topic->disabled || topic->connection->disabled)
        return 0;

    //ndw_Connection_T* conn = topic->connection;
    //ndw_Domain_T* domain = conn->domain;

    const CHAR_T* pendingMsgsLimit = ndw_GetNVPairValue(NDW_NATS_TOPIC_MSGLIMIT_OPTION, &(topic->topic_options_nvpairs));
    INT_T n_PendingMsgsLimit = 0;
    if (NULL != pendingMsgsLimit) {
        if (ndw_atoi(pendingMsgsLimit, &n_PendingMsgsLimit) && (n_PendingMsgsLimit > 0)) {
            const CHAR_T* pendingBytesLimit = ndw_GetNVPairValue(NDW_NATS_TOPIC_BYTESLIMIT_OPTION, &(topic->topic_options_nvpairs));
            INT_T n_PendingBytesLimit = 0;
            if (ndw_atoi(pendingBytesLimit, &n_PendingBytesLimit) && (n_PendingBytesLimit > 0)) {
                nats_topic->subscription_PendingLimitsMsgs = n_PendingMsgsLimit;
                nats_topic->subscription_PendingLimitsBytes = n_PendingBytesLimit;

                INT_T nMsgsLimits = 0;
                INT_T nBytesLimits = 0;
                natsStatus n_status = natsSubscription_GetPendingLimits(
                        nats_topic->nats_subscription, &nMsgsLimits, &nBytesLimits);

                if (NATS_OK != n_status) {
                    NDW_LOGERR("*** ERROR: natsSubscription_GetPendingLimits return code <%d> for %s\n",
                                n_status, topic->debug_desc);
                }
                else {
                    NDW_LOGERR("*** Existing PendingMsgsLimit <%d> and PendingBytesLimit <%d> for %s\n",
                                nMsgsLimits, nBytesLimits, topic->debug_desc);

                    n_status = natsSubscription_SetPendingLimits(nats_topic->nats_subscription,
                                    nats_topic->subscription_PendingLimitsMsgs,
                                    nats_topic->subscription_PendingLimitsBytes);
                    if (NATS_OK != n_status) {
                        NDW_LOGERR("*** ERROR: natsSubscription_SetPendingLimits return code <%d> "
                            " when trying to set MsgsLimit to <%d> and Bytes Limit to <%d> for %s\n",
                            n_status, nats_topic->subscription_PendingLimitsMsgs, nats_topic->subscription_PendingLimitsBytes,
                            topic->debug_desc);
                    }
                    else {

                        NDW_LOGERR("NOTE: natsSubscription_SetPendingLimits okay. "
                            "New Settings MsgsLimit to <%d> and Bytes Limit to <%d> for %s\n",
                            nats_topic->subscription_PendingLimitsMsgs, nats_topic->subscription_PendingLimitsBytes,
                            topic->debug_desc);

                            nMsgsLimits = 0;
                            nBytesLimits = 0;
                            n_status = natsSubscription_GetPendingLimits(
                                    nats_topic->nats_subscription, &nMsgsLimits, &nBytesLimits);

                            NDW_LOGX("Double checking Existing PendingMsgsLimit<%d> and "
                             "PendingBytesLimit<%d> natsSubscription_GetPendingLimits() call return code<%d> for %s\n",
                                nMsgsLimits, nBytesLimits, n_status, topic->debug_desc);
                    }

                } // end of GetPendingLimits was successful

            } // if Pending Bytes Limit Present
        } // if Pending Messages Limit Present
    } // end of subscription Messages and Bytes Limit Options

    return 0;
} // end method ndw_NATS_SetSubscriptionOptions

INT_T
ndw_NATS_SubscribeAsync(ndw_Topic_T* topic)
{
    if (NULL == topic) {
        NDW_LOGERR( "Parameter ndw_Topic_T* is NULL\n");
        return -1;
    }

    if (topic->disabled || topic->connection->disabled)
        return 0;

    ndw_NATS_TopicCxt_T* cxt_topic = NDW_NATS_POPULATECXT(topic);
    ndw_NATS_Connection_T* nats_connection = cxt_topic->nats_connection;
    if (NULL == cxt_topic->nats_connection->conn) {
        NDW_LOGERR( "*** ERROR Not yet Connected to %s\n", topic->debug_desc);
        return -2;
    }

    ndw_NATS_Topic_T* nats_topic = cxt_topic->nats_topic;
    ndw_NATS_JS_Attr_T* js_attr = &(nats_topic->nats_js_attr);
    if (js_attr->is_initialized && js_attr->is_enabled) {
        INT_T ret_code = ndw_NATS_JSSubscribe(topic, true); // PUSH (asynchronous) mode.
        if (0 != ret_code) {
            NDW_LOGERR("*** ERROR: JetStream PUSH mode Subscription failed for %s\n", topic->debug_desc);
        }

        return ret_code;
    }

    if (ndw_verbose > 2)
        NDW_LOGTOPICMSG("Subscribing to Topic", topic);

    if (NULL != nats_topic->nats_subscription) {
        NDW_LOGTOPICERRMSG("*** WARNING: Already Subscribed for Topic!", topic);
        return -3;
    }

    natsStatus status = natsConnection_Subscribe(
                                        &nats_topic->nats_subscription,
                                        nats_connection->conn,
                                        topic->topic_unique_name,
                                        ndw_NATS_Internal_AsyncMessageHandler,
                                        nats_topic);

    if (NATS_OK != status) {
        NDW_LOGERR("*** ERROR: NATS Subscribe failed with status <%d> for %s\n", status, topic->debug_desc);
        natsSubscription_Destroy(nats_topic->nats_subscription);
        nats_topic->nats_subscription = NULL;
        return  -4;
    }
    else {
        if (ndw_verbose > 2) {
            NDW_LOGTOPICMSG("* Successfully Subscribed!\n", topic);
        }

        natsOptions *nats_options;
        natsOptions_Create(&nats_options);
        if (NATS_OK != natsOptions_SetErrorHandler(nats_options, ndw_NATS_AsyncEventError, nats_topic))
        {
            NDW_LOGERR("*** FATAL ERROR: FAILED to set (Async) callback for natsOptions_SetErrorHandler\n");
            ndw_exit(EXIT_FAILURE);
        }

        natsOptions_Destroy(nats_options);

        ndw_NATS_SetSubscriptionOptions(topic);
        return 0;
    }
} // end method ndw_NATS_SubscribeAsync

INT_T
ndw_NATS_Unsubscribe(ndw_Topic_T* topic)
{
    if (NULL == topic)
        return 0;

    if (topic->disabled || topic->connection->disabled)
        return 0;

    ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;
    if (NULL == nats_topic)
        return 0;

    if (topic->durable_topic) {
        // Nothing to do at this Point. We can use natsSubscription_Unsubscribe
    }

    if (NULL != nats_topic->nats_subscription) {
        NDW_LOGTOPICMSG("NOTE: Unsubscribing from Topic:", topic);
        natsSubscription_Unsubscribe(nats_topic->nats_subscription);
        natsSubscription_Destroy(nats_topic->nats_subscription);
        nats_topic->nats_subscription = NULL;
        topic->synchronous_subscription = false;
        return 0;
    }
    else {
        NDW_LOGTOPICMSG("NOTE: Unsubscribing from Topic: nats_topc->nats_subscription not set!", topic);
        return -1;
    }
} // end method ndw_NATS_Unsubscribe

INT_T
ndw_NATS_GetQueuedMsgCount(ndw_Topic_T* topic, ULONG_T* count)
{
    if ((NULL == topic) || (NULL == count))
        return -1;

    *count = 0;

    if (topic->disabled || topic->connection->disabled)
        return 0;

    ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;
    if (NULL == nats_topic)
        return -2;

    if (NULL == nats_topic->nats_subscription)
        return -3;

    natsStatus status = natsSubscription_QueuedMsgs(nats_topic->nats_subscription, count);
    if (NATS_OK == status) {
        nats_topic->subscription_PendingMsgs = *count;
        return 0;
    }
    else {
        NDW_LOGERR("*** ERROR: natsSubscription_QueuedMsgs() failed with natsStatus<%d> for %s\n", status, topic->debug_desc);
        return -4;
    }
} // end method ndw_NATS_GetQueuedMsgCount

INT_T
ndw_NATS_SubscribeSynchronously(ndw_Topic_T* topic)
{
    if (NULL == topic) {
        NDW_LOGERR( "Parameter ndw_Topic_T* is NULL\n");
        return -1;
    }

    if (topic->disabled || topic->connection->disabled)
        return 0;

    ndw_NATS_TopicCxt_T* cxt_topic = NDW_NATS_POPULATECXT(topic);

    ndw_NATS_Connection_T* nats_connection = cxt_topic->nats_connection;
    if (NULL == nats_connection->conn) {
        NDW_LOGERR( "*** ERROR Not yet Connected to Connection to %s\n", topic->debug_desc);
        return -2;
    }

    ndw_NATS_Topic_T* nats_topic = cxt_topic->nats_topic;
    ndw_NATS_JS_Attr_T* js_attr = &(nats_topic->nats_js_attr);
    if (js_attr->is_initialized && js_attr->is_enabled) {
        INT_T ret_code = ndw_NATS_JSSubscribe(topic, js_attr->is_push_mode); // PUSH (asynchronous) mode or POLL mode (poll).
        if (0 != ret_code) {
            NDW_LOGERR("*** ERROR: JetStream PUSH mode Subscription failed for %s\n", topic->debug_desc);
        }

        return ret_code;
    }

    if (NULL != nats_topic->nats_subscription) {
        NDW_LOGTOPICERRMSG("*** WARNING: Already Subscribed for Topic!", topic);
        return -3;
    }

    if (topic->synchronous_subscription) {
        NDW_LOGTOPICERRMSG("*** WARNING: Already Subscribed for Topic and flag is ON!",topic);
        return -4;
    }

    if (ndw_verbose > 2)
        NDW_LOGTOPICMSG("SubscribeSync to Topic", topic);

    natsStatus status = natsConnection_SubscribeSync(&nats_topic->nats_subscription,
                                                    nats_connection->conn,
                                                    topic->topic_unique_name);

    if (NATS_OK != status) {
        NDW_LOGERR("FAILED to SubscribeSync! NATS SubscriptionSync failure status<%d> for %s\n", status, topic->debug_desc);
        natsSubscription_Destroy(nats_topic->nats_subscription);
        nats_topic->nats_subscription = NULL;
        return  -5;
    }
    else
    {
       if (ndw_verbose > 2) {
            NDW_LOGTOPICMSG("Successfully Subscribed to Topic:", topic);
        }

        topic->synchronous_subscription = true;

        ndw_NATS_SetSubscriptionOptions(topic);
    }

    return 0;
} // end method ndw_NATS_SubscribeSyncSynchronously

INT_T
ndw_NATS_SynchronousPollForMsg(ndw_Topic_T* topic, const CHAR_T** msg, INT_T* msg_length,
                                LONG_T timeout_ms, LONG_T* dropped_messages, void** vendor_closure)
{
    if (NULL != dropped_messages)
        *dropped_messages = 0;

    if (timeout_ms < 0)
        timeout_ms = 1;

    if (NULL == msg) {
        NDW_LOGERR("Invalid message POINTER!\n");
        return -1;
    }

    *msg = NULL;

    if (NULL == msg_length) {
        NDW_LOGERR("Invalid msg_length POINTER!\n");
        return -2;
    }

    if (NULL == vendor_closure) {
        NDW_LOGERR("*** FATAL ERROR: vendor_closure Pointer to Pointer is NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    *vendor_closure = NULL;
    *msg_length = 0;

    ndw_NATS_TopicCxt_T* cxt = NDW_NATS_POPULATECXT(topic);

    if (topic->disabled || topic->connection->disabled)
        return 0;

    if (! topic->is_sub_enabled) {
        NDW_LOGERR("*** WARNING: Topic is NOT enabled for Subscription! %s\n", topic->debug_desc);
        return -3;
    }

    if ((NULL == cxt->nats_connection) || (NULL == cxt->nats_connection->conn)) {
        NDW_LOGERR("*** WARNING NOT yet Connected to Connection! For %s\n", topic->debug_desc);
        return -4;
    }

    if (NULL == cxt->nats_topic) {
        NDW_LOGTOPICERRMSG("*** WARNING: Check if you have invoked Synchronous Subcription on topic", topic);
        return -5;
    }

    ndw_NATS_Topic_T* nats_topic = cxt->nats_topic;
    ndw_NATS_JS_Attr_T* js_attr = &(nats_topic->nats_js_attr);

    if (js_attr->is_initialized && js_attr->is_enabled) {
        INT_T ret_code = ndw_NATS_JSPollForMsg(topic, msg, msg_length, timeout_ms, vendor_closure);
        return ret_code;
    }

    natsSubscription* subscription = cxt->nats_topic->nats_subscription;
    if (NULL == subscription) {
        NDW_LOGTOPICERRMSG("*** WARNING: Check if you have Synchronous Subcription has been invoked on topic", topic);
        return -6;
    }

    natsMsg *nats_msg = NULL;
    natsStatus status = natsSubscription_NextMsg(&nats_msg, subscription, timeout_ms);

    LONG_T dropped = 0;
    natsSubscription_GetDropped(subscription, &dropped);
    *dropped_messages = (LONG_T) dropped;
    cxt->nats_topic->subscription_DroppedMsgs = (LONG_T) dropped;

    if (NATS_OK == status) {

        const CHAR_T* user_msg = natsMsg_GetData(nats_msg);
        INT_T nats_msg_size = natsMsg_GetDataLength(nats_msg);

        ndw_NATS_Closure_T* closure = NDW_NATS_GET_EMPTY_CLOSURE(topic);
        closure->nats_msg = nats_msg;
        closure->user_msg = user_msg;
        closure->msg_size = nats_msg_size;

        *msg = user_msg;
        *msg_length = nats_msg_size;

        *vendor_closure = closure;

        return 1;
    }
    else if (NATS_TIMEOUT != status) {
        NDW_LOGERR("Poll on Synchronous Subscription failed with NATS error code<%s> for %s\n",
                        natsStatus_GetText(status), topic->debug_desc);
        return -7; // Will be an error if anything else other than timeout.
    }
    else {
         return 0; // No message.
    }
} // end method ndw_NATS_SynchronousPollForMsg

INT_T
ndw_NATS_Publish_ResponseForRequestMsg(ndw_Topic_T* topic)
{
    ndw_NATS_Closure_T* vendor_closure = topic->last_msg_vendor_closure;
    if (NULL == vendor_closure) {
        NDW_LOGERR("*** ERROR: There is no vendor_closure Pointer in for Request-Response "
                   "feature to work off of the last message for %s\n", topic->debug_desc);
        return -1; // Do not Exit!
    }

    const natsMsg* nats_msg = vendor_closure->nats_msg;
    if (NULL == nats_msg) {
        NDW_LOGERR("*** ERROR: vendor_closure has NULL natsMsg Pointer for %s\n", topic->debug_desc);
        return -2; // Do not Exit!
    }
    const char* reply_subject = natsMsg_GetReply(nats_msg);
    if (NULL == reply_subject) {
        NDW_LOGERR("*** ERROR: vendor_closure has natsMsg but no Response Subject per "
                   "natsMsg_GetRepy(nats_msg) for %s\n", topic->debug_desc);
        return -3; // Do not Exit!
    }

    //return ndw_NATS_Publish_Internal(subject);  this blow up, as this callback thread do not tls specific outMsgCtx

    ndw_NATS_Connection_T* nats_connection = vendor_closure->nats_connection;

    if (NULL == nats_connection) {
        NDW_LOGERR("*** ERROR: vendor_closure has NULL connection Pointer for %s\n", topic->debug_desc);
        return -4; // Do not Exit!
    }

    const CHAR_T* user_msg = natsMsg_GetData(nats_msg);
    INT_T msg_size = natsMsg_GetDataLength(nats_msg);

    natsStatus status = natsConnection_Publish(nats_connection->conn,
				reply_subject, (const void*) user_msg, msg_size);
    if (status != NATS_OK) {
        NDW_LOGERR("NATS message publish failed natsStatus<%d> %s\n", status, topic->debug_desc);
        return -5; // Do not Exit!
    } else {
        status = natsConnection_Flush(nats_connection->conn);
        if (status != NATS_OK) {
            NDW_LOGERR("NATS connnection flush failed natsStatus<%d> %s\n", status, topic->debug_desc);
        }
    }

    return 0;

} // end ndw_NATS_Publish_ResponseForRequestMsg(void)

INT_T
ndw_NATS_GetResponseForRequestMsg(ndw_Topic_T* topic, const CHAR_T** msg, INT_T* msg_length,
                                LONG_T timeout_ms, void** vendor_closure)
{
    ndw_OutMsgCxt_T* cxt_msg = ndw_GetOutMsgCxt();
    if (NULL == cxt_msg) {
        NDW_LOGERR("*** FATAL ERROR: ndw_GetOutMsg() returned NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Topic_T* t = cxt_msg->topic;
    ndw_NATS_TopicCxt_T* cxt_topic = NDW_NATS_POPULATECXT(t);

    if (t->disabled || t->connection->disabled) {
        NDW_LOGERR("*** ERROR: topic or topic connection disabled!\n");
        return 0;
    }

    ndw_Connection_T* c = cxt_topic->connection;

    if (c->disabled) {
        NDW_LOGERR("*** ERROR: ctx topic connection disabled!\n");
        return 0;
    }

    CHAR_T* start_address = (CHAR_T*) cxt_msg->header_address;
    INT_T header_size = cxt_msg->header_size;
    INT_T msg_size = cxt_msg->message_size;
    INT_T total_size = header_size + msg_size;

    if (! ndw_NATS_IsConnected(c)) {
        NDW_LOGTOPICERRMSG("Connection was NOT established (before)!", t);
        return -3;
    }

    ndw_NATS_Connection_T* nats_connection = (ndw_NATS_Connection_T*) c->vendor_opaque;
    if (NULL == nats_connection) {
        NDW_LOGERR("*** WARNING: ndw_NATS_Connection_T* not yet allocated for %s\n", t->debug_desc);
        return -4;
    }

    if (! t->is_pub_enabled) {
        NDW_LOGERR("*** WARNING: Topic is NOT enabled for Publication! %s\n", t->debug_desc);
        return -5;
    }

    if (NDW_ISNULLCHARPTR(t->pub_key)) {
        NDW_LOGERR("*** FATAL ERROR: pub_key is NULL in Topic! %s\n", t->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_Topic_T* nats_topic = cxt_topic->nats_topic;

    *vendor_closure = NULL;
    *msg_length = 0;

    natsStatus status;
    natsMsg *request_msg = NULL;
    status = natsMsg_Create(&request_msg, t->pub_key, NULL, start_address, total_size);
    if (status != NATS_OK) {
        NDW_LOGERR("*** FATAL ERROR: nats message create failed! %s\n", t->debug_desc);
        return -6;
    }

    natsMsg *reply_msg = NULL;
    status = natsConnection_RequestMsg(&reply_msg, nats_connection->conn, request_msg, timeout_ms);

    natsMsg_Destroy(request_msg);
    request_msg = NULL;

    if (NATS_OK == status && (reply_msg != NULL)) {
        const CHAR_T* user_msg = natsMsg_GetData(reply_msg);
        INT_T reply_msg_size = natsMsg_GetDataLength(reply_msg);

        ndw_NATS_Closure_T* closure = NDW_NATS_GET_EMPTY_CLOSURE(topic);

        closure->nats_msg = reply_msg;
        closure->user_msg = user_msg;
        closure->msg_size = reply_msg_size;

        *msg = user_msg;
        *msg_length = reply_msg_size;
        *vendor_closure = closure;
        nats_topic->total_messages_get_requestmsg += 1;

        return 1;
    } else if (NATS_TIMEOUT != status) {
        NDW_LOGERR("Get Response from Requested Msg failed with NATS error code<%s> for %s\n",
                        natsStatus_GetText(status), topic->debug_desc);
        nats_clearLastError();
        return -7;
    }

    return 0;
} // end method ndw_NATS_GetResponseForRequestMsg

/*
 * NOTE: Talk about complexity in handling subscription using JetStream!!
 */
bool
ndw_NATS_IsJSPubSub(ndw_Topic_T* topic)
{
    ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;
    ndw_NATS_JS_Attr_T* js_attr = &(nats_topic->nats_js_attr);
    if (js_attr->is_initialized) {
        return js_attr->is_enabled;
    }

    // Initialize it once during pub or sub invocation.
    js_attr->is_initialized = true;
    js_attr->is_enabled = false;

    NDW_NVPairs_T* vendor_nvpairs = &(topic->vendor_topic_options_nvpairs);
    const CHAR_T* js_enabled = ndw_GetNVPairValue(NDW_NATS_JS_ATTR_JETSTREAM_NAME, vendor_nvpairs);
    if (NDW_ISNULLCHARPTR(js_enabled) || (0 != strcasecmp("true", js_enabled))) {
        return false;
    }

    js_attr->is_enabled = true;

    const CHAR_T* stream_name = ndw_GetNVPairValue(NDW_NATS_JS_ATTR_STREAM_NAME, vendor_nvpairs);
    if (NDW_ISNULLCHARPTR(stream_name)) {
        NDW_LOGERR("JetStream Attribute<%s> is missing from Configuration for %s\n",
                        NDW_NATS_JS_ATTR_STREAM_NAME, topic->debug_desc);
        // Although if we just Publishing we do not need the Stream Name.
        // But it is good to be disciplined and have the Stream Name!
        js_attr->is_config_error = true;
        return false;
    }

    js_attr->stream_name = stream_name;

    // For Publish we need a Stream Subject Name.
    // And if one is not specified we use the pub_key in the ndw_Topic_T data structure.
    const CHAR_T* stream_subject_name = ndw_GetNVPairValue(NDW_NATS_JS_ATTR_STREAM_SUBJECT_NAME, vendor_nvpairs);
    js_attr->stream_subject_name = NDW_ISNULLCHARPTR(stream_subject_name) ?
                    topic->pub_key : stream_subject_name;
    if (NDW_ISNULLCHARPTR(js_attr->stream_subject_name)) {
        NDW_LOGERR("*** CONFIG ERROR: JetStream Subject Name cannot be NULL! "
                    "Is ndw_Topic_T pub_key also NULL? Check Attributes for %s\n", topic->debug_desc);
        js_attr->is_config_error = true;
        return false;
    }

    // Done with Publish parameters.
    // Move onto Subscribe, which can be Push or Pull mode.
    // Yikes: We need 5 paramaters. Talk about complexity!
    // NOTE: Push mode needs Stream Name, Durable Name and Filter.
    // NOTE: Pull model needs Durable Name and Filter.

    const CHAR_T* stream_PushSubscribe = ndw_GetNVPairValue(NDW_NATS_JS_ATTR_PUSH_SUBSCRIBE, vendor_nvpairs);
    bool is_push = (! NDW_ISNULLCHARPTR(stream_PushSubscribe)) &&
                    (0 == strcasecmp("true", stream_PushSubscribe));

    const CHAR_T* stream_PullSubscribe = ndw_GetNVPairValue(NDW_NATS_JS_ATTR_PULL_SUBSCRIBE, vendor_nvpairs);
    bool is_pull = (! NDW_ISNULLCHARPTR(stream_PullSubscribe)) &&
                    (0 == strcasecmp("true", stream_PullSubscribe));

    if (is_pull && is_push) {
        NDW_LOGERR("Both Push and Pull cannot be configured to be true for JetStream subscriber! "
                "Parameters are: <\"%s\"> and <\"%s\"> for %s\n",
                NDW_NATS_JS_ATTR_PUSH_SUBSCRIBE, NDW_NATS_JS_ATTR_PULL_SUBSCRIBE, topic->debug_desc);   

        js_attr->is_config_error = true;
        return js_attr->is_enabled;
    }

    if (!is_pull) {
        is_push = true; // By default we assume it is PUSH since PULL is NOT specified.
    }

    // At this Point config says we MUST have a subscription.
    // Check that both Durable Name and Filter is specified.

    const CHAR_T* stream_DurableName = ndw_GetNVPairValue(NDW_NATS_JS_ATTR_DURABLE_NAME, vendor_nvpairs);
    js_attr->durable_name = stream_DurableName;

    const CHAR_T* stream_filter = ndw_GetNVPairValue(NDW_NATS_JS_ATTR_FILTER_NAME, vendor_nvpairs);
    if (NDW_ISNULLCHARPTR(stream_filter)) {
        js_attr->filter = topic->sub_key;
    }

    bool is_filter = true; // As we default to ndw_Topic_T sub_key.
    bool is_durable = (! NDW_ISNULLCHARPTR(stream_DurableName));

    js_attr->is_sub_enabled = (is_durable && is_filter);

    if ( (is_durable && (! is_filter)) || ((! is_durable) && is_filter) ) {
        NDW_LOGERR("*** CONFIG ERROR: JetStream subscription requires both Durable Parameter<\"%s\"> "
                    "and FilterName Parameter<\"%s\">, and <%s> is missing! %s\n",
                NDW_NATS_JS_ATTR_DURABLE_NAME, NDW_NATS_JS_ATTR_FILTER_NAME,
                is_durable ? "FilterName" : "DurableName", topic->debug_desc);

        js_attr->is_pub_enabled = false;
        js_attr->is_sub_enabled = false;
        js_attr->is_config_error = true;
        return js_attr->is_enabled;
    }

    if (js_attr->is_sub_enabled) {
        const CHAR_T* queue_browse = ndw_GetNVPairValue(NDW_NATS_JS_ATTR_BROWSE_MODE, vendor_nvpairs);
        js_attr->is_sub_browse_mode = (! NDW_ISNULLCHARPTR(queue_browse)) &&
                                    (0 == strcasecmp("true", queue_browse));
    }

    // Yikes: That was a lot of twisted logic just to set vendor attributes!

    js_attr->is_config_error = false;
    js_attr->is_pub_enabled = js_attr->is_enabled;
    js_attr->is_push_mode = is_push;
    js_attr->is_pull_mode = is_pull;
    js_attr->is_initialized = true;
    topic->durable_topic = true;
    nats_topic->durable_topic = true;

    ndw_NATS_PrintJSAttr(topic);

    return true;
} // end method ndw_NATS_ISJSPubSub

INT_T
ndw_NATS_JSInitWithLock(ndw_NATS_Connection_T* nats_connection)
{
    if (nats_connection->js_enabled_count <= 0) {
        NDW_LOGERR("*** FATAL ERROR: Asked to initiate JetStream connection but it is NOT enabled in "
                    "ndw_NATS_Connection_T for %s\n", nats_connection->ndw_connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL != nats_connection->js_context) {
        // NDW_LOGX("Already connected to JetStream for %s\n", topic->debug_desc);
        return 0;
    }

    natsStatus s = natsConnection_JetStream(&(nats_connection->js_context), nats_connection->conn, NULL);
    if (NATS_OK != s) {
        NDW_LOGERR("Failed to connection JetStream. Status code<%d, %s>. For %s\n",
                            s, natsStatus_GetText(s), nats_connection->ndw_connection->debug_desc);
        if (NULL != nats_connection->js_context) {
            jsCtx_Destroy(nats_connection->js_context);
            nats_connection->js_context = NULL;
        }

        return -1;
    }

    return 0;
} // end method ndw_NATS_JSInitWithLock

INT_T
ndw_NATS_JSConnect(ndw_NATS_Connection_T* nats_connection)
{
    pthread_mutex_lock(&ndw_NATS_JS_ConnectionLock);
    INT_T ret_code = ndw_NATS_JSInitWithLock(nats_connection);
    pthread_mutex_unlock(&ndw_NATS_JS_ConnectionLock);
    return ret_code;

} // end method ndw_NATS_JSConnect

static void
ndw_NATS_JS_AsyncMsgHandler(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    const CHAR_T* user_msg = natsMsg_GetData(msg);
    INT_T msg_size = natsMsg_GetDataLength(msg);

    if (NULL == closure) {
        NDW_LOGERR( "*** FATAL ERROR: closure is NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }


    ndw_NATS_Topic_T* nats_t = (ndw_NATS_Topic_T*) closure;
    ndw_Topic_T* t = nats_t->ndw_topic;
    if (NULL == t) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_Topic_T Pointer is NULL in ndw_NATS_Topic_T\n");
        ndw_exit(EXIT_FAILURE);
    }

    CHAR_T* ip_address = NULL;
    natsConnection_GetClientIP(nc, &ip_address);
    if (NULL != ip_address) {
        if (ndw_CopyMaxBytes(t->last_received_ip_address,
                                sizeof(t->last_received_ip_address), ip_address) < 0) {
            NDW_LOGERR( "*** FATAL ERROR: invalid ip_address\n");
            ndw_exit(EXIT_FAILURE);
        }
    } else {
        t->last_received_ip_address[0] = '\0';
    }

    jsMsgMetaData *meta = NULL;
    natsStatus meta_status = natsMsg_GetMetaData(&meta, msg);
    if ((NATS_OK != meta_status) || (NULL == meta)) {
        NDW_LOGERR("*** ERROR: natsMsg_GetMetaData(...) failed with <%d, %s> for %s\n",
                    meta_status, natsStatus_GetText(meta_status), t->debug_desc);
        // Continue and do NOT return!
    }
    else {
        t->last_received_durable_ack_sequence = meta->Sequence.Consumer;
        t->last_received_durable_ack_global_sequence = meta->Sequence.Stream;
        t->last_received_durable_total_delivered = meta->NumDelivered;
        t->last_received_durable_total_pending = meta->NumPending;

        jsMsgMetaData_Destroy(meta);
    }

    if (ndw_verbose > 4) {
        const CHAR_T* subject = (NULL == sub) ? "Unknown!" : natsSubscription_GetSubject(sub);
        NDW_LOGX("[Connection IP Address: <%s>] received message on subject <%s> msg_length: %d\n",
                    (NULL == ip_address) ? "?" : ip_address, subject, msg_size);
    }

    free(ip_address);

    ndw_NATS_Closure_T* vendor_closure = NDW_NATS_GET_EMPTY_CLOSURE(t);
    vendor_closure->is_js = true;
    vendor_closure->nats_msg = msg;
    vendor_closure->user_msg = user_msg;
    vendor_closure->msg_size = msg_size;
    vendor_closure->counter += 1;

    ndw_HandleVendorAsyncMessage(t, (UCHAR_T*) user_msg, msg_size, vendor_closure);

} // end method ndw_NATS_JS_AsyncMsgHandler

INT_T
ndw_NATS_JSSubscribe(ndw_Topic_T* topic, bool push_mode)
{
    if (! topic->is_sub_enabled) {
        NDW_LOGERR("Topic is NOT enabled for Subscription! %s\n", topic->debug_desc);
        return -1;
    }

    if (ndw_verbose > 2) {
        NDW_LOGX("%s Mode Subscription on %s\n", (push_mode ? "PUSH (ASYNCHRONOUS)" : "PULL (Poll)"), topic->debug_desc);
    }

    ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;
    if (NULL != nats_topic->nats_subscription) {
        return 0; // Already susbscribed!
    }

    ndw_NATS_Connection_T* nats_connection = nats_topic->nats_connection;

    ndw_NATS_JS_Attr_T* js_attr = &(nats_topic->nats_js_attr);

    if ((! js_attr->is_initialized) || (! js_attr->is_enabled) || (js_attr->is_config_error) || (! js_attr->is_sub_enabled)) {
        NDW_LOGERR("*** ERROR: Cannot determine if JetStream Subscribe is enabled for %s\n", topic->debug_desc);
        return -2;
    }

    if (push_mode) {
         if (! js_attr->is_push_mode) {
            NDW_LOGERR("*** FATAL ERROR: Internal Error. Invoked for PUSH mode on JetStream when it is NOT "
                        "configured as such " "for %s\n", topic->debug_desc);
            ndw_exit(EXIT_FAILURE);
        }
    }
    else {
        if (! js_attr->is_pull_mode) {
            NDW_LOGERR("*** FATAL ERROR: Internal Error. Invoked for PULL mode on JetStream when it is NOT "
                        "configured as such " "for %s\n", topic->debug_desc);
            ndw_exit(EXIT_FAILURE);
        }
    }

    if (js_attr->is_push_mode)
    {
        jsErrCode errCode = 0;
        jsSubOptions subOpts;
        memset(&subOpts, 0,sizeof(subOpts));
        subOpts.Stream = js_attr->stream_name;
        subOpts.Consumer = js_attr->durable_name; // You need this for Ack to work, else queue will keep growng!
        subOpts.Config.AckPolicy = js_AckExplicit;
        subOpts.ManualAck = true;

#if 1
        NDW_LOGX("js_Subscribe: subOpts.Stream = {%s} subOpts.Consumer = {%s} subject = {%s}\n",
                subOpts.Stream, subOpts.Consumer, js_attr->filter);
#endif

        natsStatus s = js_Subscribe(&(nats_topic->nats_subscription),
                                    nats_connection->js_context,
                                    js_attr->filter,
                                    ndw_NATS_JS_AsyncMsgHandler,
                                    (void*) nats_topic, // Closure
                                    NULL, // no jsOptions
                                    &subOpts,
                                    &errCode);   

        if (NATS_OK != s) {
            NDW_LOGERR("*** ERROR: JetStream Durable PUSH Subscription Failed. natsStats<%d, %s> errCode<%d> for %s\n",
                            s, natsStatus_GetText(s), errCode, topic->debug_desc);
            if (NULL != nats_topic->nats_subscription) {
                natsSubscription_Destroy(nats_topic->nats_subscription);
                nats_topic->nats_subscription = NULL;
            }

            return -2;
        }

        return 0;
    }
    else if (js_attr->is_pull_mode)
    {
        jsErrCode errCode = 0;

        natsStatus s = js_PullSubscribe(&(nats_topic->nats_subscription),
                                        nats_connection->js_context,
                                        js_attr->filter,
                                        js_attr->durable_name,
                                        NULL,
                                        NULL,
                                        &errCode);
        if (NATS_OK != s) {
            NDW_LOGERR("*** ERROR: JetStream Durable PULL Subscription Failed. natsStats<%d, %s> errCode<%d> for %s\n",
                            s, natsStatus_GetText(s), errCode, topic->debug_desc);
            if (NULL != nats_topic->nats_subscription) {
                natsSubscription_Destroy(nats_topic->nats_subscription);
                nats_topic->nats_subscription = NULL;
            }

            return -3;
        }

        return 0;
    }
    else {
        NDW_LOGERR("*** ERROR: nwd_NATS_JS_Attr_T indiates it is NOT configured either for Push or Pull mode! "
                    "For %s\n", topic->debug_desc);
        return -4;
    }
} // end method ndw_NATS_JSSubscribe

INT_T
ndw_NATS_JSPublish(ndw_Topic_T* topic)
{
    if (! topic->is_pub_enabled) {
        NDW_LOGERR("Topic NOT enabled for Publication! %s\n", topic->debug_desc);
        return -1;
    }

    ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;

    ndw_NATS_JS_Attr_T* js_attr = &(nats_topic->nats_js_attr);
    if ((! js_attr->is_initialized) || (! js_attr->is_enabled) || (js_attr->is_config_error) || (! js_attr->is_pub_enabled)) {
        NDW_LOGERR("*** ERROR: Failed to determine if it is JSPubSub! On %s\n", topic->debug_desc);
        return -2;
    }

    ndw_NATS_Connection_T* nats_connection = nats_topic->nats_connection;

    ndw_OutMsgCxt_T* cxt = ndw_GetOutMsgCxt();
    if (NULL == cxt) {
        NDW_LOGERR("*** FATAL ERROR: ndw_GetOutMsg() returned NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    UCHAR_T* start_address = (UCHAR_T*) cxt->header_address;
    INT_T header_size = cxt->header_size;
    //UCHAR_T* message_address = cxt->message_address;
    INT_T msg_size = cxt->message_size;
    INT_T total_size = header_size + msg_size;

    INT_T max_tries = 2;
    INT_T ret_code = -3;
    jsPubAck* ack = NULL;
    for (INT_T i = 0; i < max_tries; i++ )
    {
        ack = NULL;

#if 0
        NDW_LOGX("js_Publish: subject = {%s} len = %zu\n", topic->pub_key, strlen(topic->pub_key));
        for (size_t i = 0; i < strlen(topic->pub_key); ++i)
            NDW_LOGX("  [%02zu] = 0x%02X (%c)\n", i, (UCHAR_T)topic->pub_key[i],
               isprint((UCHAR_T)topic->pub_key[i]) ? topic->pub_key[i] : '.');
#endif
        natsStatus s = js_Publish(&ack, nats_connection->js_context, topic->pub_key,
                                    start_address, total_size, NULL, NULL);

        if (NATS_OK == s) {
            nats_topic->total_messages_published += 1;
            ret_code = 0;
            break; // Successful Publish!
        }

        NDW_LOGERR("*** ERROR: Failed to publish to JetStream on %s\n", topic->debug_desc);
        ndw_NATS_PublicationBackoff(nats_connection);
    }

    if (NULL != ack) {
        jsPubAck_Destroy(ack);
    }

    return ret_code;
} // end method ndw_NATS_JSPublish

INT_T
ndw_NATS_JSPollForMsg(ndw_Topic_T* topic, const CHAR_T** msg, INT_T* msg_length, LONG_T timeout_ms, void** vendor_closure)
{
    if ((NULL == msg) || (NULL == msg_length)) {
        NDW_LOGERR("*** FATAL ERROR: msg and/or msg_length POINTER parameter(s) is NULL! For %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (timeout_ms < 0)
        timeout_ms = 1; // One millisecond is default.

    if (NULL == vendor_closure) {
        NDW_LOGERR("*** FATAL ERROR: vendor_closure Pointer to Pointer is NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    *vendor_closure = NULL;

    ndw_NATS_Topic_T* nats_topic = (ndw_NATS_Topic_T*) topic->vendor_opaque;
    ndw_NATS_JS_Attr_T* js_attr = &(nats_topic->nats_js_attr);

    if (! js_attr->is_pull_mode) {
        NDW_LOGX("Cannot Polling on JetStream Subject<%s> as is_pull_mode NOT set on %s\n",
                    topic->sub_key, topic->debug_desc);
        return -1;
    }

    if (NDW_ISNULLCHARPTR(topic->sub_key)) {
        NDW_LOGX("Cannot poll on JetStream Subject as Topic sub_key is NOT set for %s\n", topic->debug_desc);
        return -2;
    }

    if (js_attr->is_push_mode) {
        NDW_LOGX("Cannot poll on JetStream Subject<%s> it is set to PUSH and NOT POLL mode for %s\n",
                    topic->sub_key, topic->debug_desc);
        return -3;
    }

    if (ndw_verbose > 4) {
        NDW_LOGX("Polling on JetStream Subject<%s> with timeout<%ld ms> on %s\n",
                    topic->sub_key, timeout_ms, topic->debug_desc);
    }

    natsMsgList msgList;
    memset(&msgList, 0, sizeof(msgList));

    INT_T num_messages = 1; // XXX: Need to support batch mode fetching!

    natsStatus s = natsSubscription_Fetch(&msgList, nats_topic->nats_subscription, num_messages, timeout_ms, NULL);
    if (NATS_TIMEOUT == s) {
        if (ndw_verbose > 4) {
            NDW_LOGX("NOTE: natsSubscription_Fetch() returned with NATS_TIMEOUT for %s\n", topic->debug_desc);
        }

        return 0;   // No message.
    }

    if (NATS_OK != s) {
        NDW_LOGERR("*** ERROR: natsSubscrption_Fetch (PULL poll mode) failed with code<%d, %s> for %s\n",
                    s, natsStatus_GetText(s), topic->debug_desc);
        return -4;
    }

    if ((0 == msgList.Count) || (msgList.Count > num_messages)) {
        NDW_LOGERR("*** ERROR: natsSubscrption_Fetch Return <%d> messages when Expected NOT more than <%d> for %s\n",
                    msgList.Count, num_messages, topic->debug_desc);
        return -5;
    }

    // XXX: ToDo: Handle batched polled messages.
    for (INT_T i = 0; i < msgList.Count; i++)
    {
        natsMsg* nats_msg = msgList.Msgs[i];

        jsMsgMetaData *meta = NULL;
        natsMsg_GetMetaData(&meta, nats_msg);
        if (NULL == meta) {
            NDW_LOGERR("*** ERROR: natsMsg_GetMetaData returned NULL POINTER for %s\n", topic->debug_desc);
        }
        else {
            js_attr->durable_ack_sequence = meta->Sequence.Consumer;
            js_attr->durable_ack_global_sequence = meta->Sequence.Stream;
            topic->last_received_durable_ack_sequence =  meta->Sequence.Consumer;
            topic->last_received_durable_ack_global_sequence = meta->Sequence.Stream;
            topic->last_received_durable_total_delivered += 1;
            topic->last_received_durable_total_pending = 0;

            jsMsgMetaData_Destroy(meta);
        }

        ndw_NATS_Closure_T* closure = NDW_NATS_GET_EMPTY_CLOSURE(topic);
        LONG_T dropped = 0;
        natsSubscription_GetDropped(nats_topic->nats_subscription, &dropped);
        nats_topic->subscription_DroppedMsgs = (LONG_T) dropped;

        // XXX: Note that this point we polling for ONLY 1 message, else this code will have to maintain a list.
        const CHAR_T* user_msg = natsMsg_GetData(nats_msg);
        INT_T nats_msg_size = natsMsg_GetDataLength(nats_msg);
        closure->is_js = true;
        closure->nats_msg = nats_msg;
        closure->user_msg = user_msg;
        closure->msg_size = nats_msg_size;
        closure->counter += 1;

        *msg = user_msg;
        *msg_length = nats_msg_size;

        *vendor_closure = closure;
    } // for each message


    free(msgList.Msgs); // Important

    return 1;
} // end method ndw_NATS_JSPollForMsg

INT_T
ndw_NATS_ProcessConfiguration(ndw_Topic_T* topic)
{
    ndw_Connection_T* connection = topic->connection;
    if (NULL == connection) {
        NDW_LOGERR("*** FATAL ERROR: ndw_Connection_T NOTset for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_Connection_T* nats_connection = (ndw_NATS_Connection_T*) connection->vendor_opaque;
    if (NULL == nats_connection)
    {
        // A connection can have multiple topics.
        // The first topic initialization set this.
        nats_connection = calloc(1, sizeof(ndw_NATS_Connection_T));
        nats_connection->ndw_connection = connection;
        connection->vendor_opaque = nats_connection;

        // Parse and set properties of the NATS Connection
        ndw_NATS_Connection_T* conn = nats_connection;

        conn->tcp_nodelay = false;
        conn->number_of_callback_threads = 0;
        conn->flush_timeout_ms = NDW_NATS_CONNECTION_FLUSH_TIMEOUT_MS;
        conn->publication_backoff_bytes = NDW_NATS_CONNECTION_PUBLICATION_DEFAULT_BACKOFF_BYTES;
        conn->backoff_sleep_us = NDW_NATS_CONNECTION_PUBLICATION_DEFAULT_BACKOFF_SLEEP_US;
        conn->backoff_attempts = NDW_NATS_CONNECTION_PUBLICATION_DEFAULT_BACKOFF_ATTEMPTS;
        conn->iobuf_size = NDW_NATS_CONNECTION_DEFAULT_IO_BUFSIZE_KB;

        bool exists = false;
        INT_T value = (INT_T) ndw_NATS_ParseConnectionOptions(connection, NDW_NATS_CONNECTION_TCP_NODELAY_OPTION, &exists);
        if (exists && (value >=0))
            conn->tcp_nodelay = (0 == value) ? false : true;
        NDW_LOGX("Connection Option: TCP_NODELAY<%s> for %s\n", (conn->tcp_nodelay ? "True" : "false"), connection->debug_desc);

        exists = false;
        value = (INT_T) ndw_NATS_ParseConnectionOptions(connection, NDW_NATS_CONNECTION_NUMBER_OF_CALLBACK_THREADS, &exists);
        if (exists && (value > 0))
            conn->number_of_callback_threads = value;
        NDW_LOGX("Connection Option: # of Callback Threads<%ld> for %s\n", conn->number_of_callback_threads, connection->debug_desc);

        exists = false;
        value = (INT_T) ndw_NATS_ParseConnectionOptions(connection, NDW_NATS_CONNECTION_FLUSH_TIMEOUT_MS_OPTION, &exists);
        if (exists && (value > 0))
            conn->flush_timeout_ms = value;
        NDW_LOGX("Connection Option: flush_timeout_ms <%ld> for %s\n", conn->flush_timeout_ms, connection->debug_desc);
            
        exists = false;
        value = ndw_NATS_ParseConnectionOptions(connection, NDW_NATS_CONNECTION_PUBLICATION_BACKOFF_BYTES, &exists);
        if (exists && (value > 0))
            conn->publication_backoff_bytes = value;
        NDW_LOGX("Connection Option: # of publication_backoff_bytes <%ld> for %s\n", conn->publication_backoff_bytes, connection->debug_desc);

        exists = false;
        value = ndw_NATS_ParseConnectionOptions(connection, NDW_NATS_CONNECTION_PUBLICATION_BACKOFF_SLEEP_US, &exists);
        if (exists && (value > 0))
            conn->backoff_sleep_us = value;
        NDW_LOGX("Connection Option: # of backoff_sleep_us <%ld> for %s\n", conn->backoff_sleep_us, connection->debug_desc);

        exists = false;
        value = ndw_NATS_ParseConnectionOptions(connection, NDW_NATS_CONNECTION_PUBLICATION_BACKOFF_ATTEMPTS, &exists);
        if (exists && (value > 0))
            conn->backoff_attempts = value;
        NDW_LOGX("Connection Option: Number of backoff_attempts <%ld> for %s\n", conn->backoff_attempts, connection->debug_desc);

        exists = false;
        value = ndw_NATS_ParseConnectionOptions(connection, NDW_NATS_CONNECTION_IO_BUFSIZE_KB, &exists);
        if (exists && (value > 0))
            conn->iobuf_size = value * 1024; // Input must be in KB (Kilobytes).
        NDW_LOGX("Connection Option: iobuf_size input<%d> Total Bytes<%ld> for %s\n", value, conn->iobuf_size, connection->debug_desc);

        exists = false;
        value = ndw_NATS_ParseConnectionOptions(connection, NDW_NATS_CONNECTION_TIMEOUT_MS, &exists);
        if (exists && (value > 0))
            conn->connection_timeout = value;
        NDW_LOGX("Connection Option: connection_timeout input<%d> for %s\n", value, connection->debug_desc);

        exists = false;
        value = ndw_NATS_ParseConnectionOptions(connection, NDW_NATS_CONNECTION_MAX_RECONNECTS, &exists);
        if (exists && (value > 0))
            conn->connection_max_reconnects = value;
        NDW_LOGX("Connection Option: connection_max_reconnects input<%d> for %s\n", value, connection->debug_desc);

        exists = false;
        value = ndw_NATS_ParseConnectionOptions(connection, NDW_NATS_CONNECTION_MAX_RECONNECT_WAIT_MS, &exists);
        if (exists && (value > 0))
            conn->connection_reconnection_wait = value;
        NDW_LOGX("Connection Option: connection_reconnection_wait input<%d> for %s\n", value, connection->debug_desc);

        exists = false;
        value = ndw_NATS_ParseConnectionOptions(connection, NDW_NATS_CONNECTION_NOECHO, &exists);
        if (exists && (value > 0))
            conn->connection_no_echo = true;
        NDW_LOGX("Connection Option: connection_no_echo input<%d> for %s\n", value, connection->debug_desc);

    } // end if NULL connection Pointer


    if (NULL != topic->vendor_opaque) {
        NDW_LOGERR("*** FATAL ERROR: vendor_opaque NOT NULL during initialization for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_Topic_T* nats_topic = calloc(1, sizeof(ndw_NATS_Topic_T));
    topic->vendor_opaque = nats_topic;
    nats_topic->ndw_topic = topic;
    nats_topic->nats_connection = nats_connection;
    nats_topic->initiation_time = ndw_GetCurrentUTCNanoseconds();

    if (! ndw_is_really_NATS_connection(connection)) {
        NDW_LOGERR("*** FATAL ERROR: Cannot process Connection Configuration for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_IsJSPubSub(topic);

    ndw_NATS_JS_Attr_T* attr = &(nats_topic->nats_js_attr);
    if (! attr->is_initialized) {
        NDW_LOGERR("*** FATAL ERROR: ndw_NATS_JS_Attr_T is NOT initialized for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (! attr->is_enabled) {
        // No Durability feature. JetStream NOT needed.
        nats_topic->durable_topic = false;
        topic->durable_topic = false;
        return 0;
    }

    // Durability is expected. JetStream needed.
    if (attr->is_config_error) {
        NDW_LOGERR("*** FATAL ERROR: ndw_NATS_JS_Attr_T initialized has CONFIG error for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (! topic->is_pub_enabled)
        attr->is_pub_enabled = false;

    if (! topic->is_sub_enabled)
        attr->is_sub_enabled = false;

    if ((!attr->is_pub_enabled) && (!attr->is_sub_enabled)) {
        NDW_LOGERR("*** FATAL ERROR: ndw_NATS_JS_Attr_T has neither pub NOR sub enabled for %s\n",
                    topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (attr->is_push_mode && attr->is_pull_mode) {
        NDW_LOGERR("*** FATAL ERROR: ndw_NATS_JS_Attr_T BOTH PUSH and PULL mode enabled for %s\n",
                    topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    // Check sub. Cannot be both PUSH and PULL mode.
    if (attr->is_sub_enabled) {
        bool atleast_one_mode = attr->is_push_mode || attr->is_pull_mode;
        if (! atleast_one_mode) {
            NDW_LOGERR("*** FATAL ERROR: ndw_NATS_JS_Attr_T Neither PUSH NOR PULL mode enabled for %s\n",
                    topic->debug_desc);
            ndw_exit(EXIT_FAILURE);
        }
    }

    bool no_stream_name = NDW_ISNULLCHARPTR(attr->stream_name);
    bool no_stream_subject_name = NDW_ISNULLCHARPTR(attr->stream_subject_name);
    bool no_durable_name = NDW_ISNULLCHARPTR(attr->durable_name);
    bool no_filter = NDW_ISNULLCHARPTR(attr->filter);

    //  Pub: Needs stream_subject_name.
    if (attr->is_pub_enabled && no_stream_subject_name) {
        NDW_LOGERR("*** FATAL ERROR: ndw_NATS_JS_Attr_T: Publish need stream_subject_name for %s\n",
                    topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    // Check Sub attributes for PUSH and PULL.
    if (attr->is_sub_enabled) {
        if (attr->is_push_mode) {
            // Sub: PUSH Mode: Needs stream_name, durable_name and filter.
            if (no_stream_name || no_durable_name || no_filter) {
                NDW_LOGERR("*** FATAL ERROR: ndw_NATS_JS_Attr_T: PUSH mode needs stream_name and durable_name "
                            "and filter for %s\n", topic->debug_desc);
                ndw_exit(EXIT_FAILURE);
            }
        }
        else if (attr->is_pull_mode) {
            // Sub: PULL Mode: durable_name and filter.
            if (no_durable_name || no_filter) {
                NDW_LOGERR("*** FATAL ERROR: ndw_NATS_JS_Attr_T: PULL mode needs durable_name and filter for %s\n",
                                topic->debug_desc);
                ndw_exit(EXIT_FAILURE);
            }
        }
        else {
            NDW_LOGERR("*** FATAL ERROR: ndw_NATS_JS_Attr_T Neither PUSH or PULL mode enabled for %s\n", topic->debug_desc);
            ndw_exit(EXIT_FAILURE);
        }

    } // end if sub is enabled

    nats_topic->durable_topic = true;
    topic->durable_topic = true;
    nats_connection->js_enabled_count += 1; // At least one Topic needs JetStream!

    ndw_Build_JSAttributes_String(topic);
    NDW_LOG("** Topic->debug_desc with JetStream ==> %s\n", topic->debug_desc);

    return 0;
} // end method ndw_NATS_ProcessConfiguration

INT_T
ndw_NATS_CommitQueuedMsg(ndw_Topic_T* topic, void* vendor_closure)
{
    ndw_NATS_Closure_T* c = (ndw_NATS_Closure_T*) vendor_closure;
    if (NULL == c) {
        NDW_LOGERR("*** FATAL ERROR: vendor_closure is NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    if (ndw_verbose > 3) {
        NDW_LOGX("Closure: is_js<%s> msg_size<%d> topic<%s>\n",
                    (c->is_js ? "Y" : "n"), c->msg_size, topic->debug_desc);
    }

    if (NULL == c->nats_msg) {
        NDW_LOGERR("*** FATAL ERROR: nats_msg Pointer is NULL in vendor_closure!\n");
        ndw_exit(EXIT_FAILURE);
    }

    INT_T ret_code = 0;

    if (c->is_js) {
        natsStatus ack_status = natsMsg_Ack(c->nats_msg, NULL);
        if (NATS_OK != ack_status) {
            NDW_LOGERR("*** ERROR: natsMsg_Ack(...) failed with status<%d> status_text<%s> for %s\n",
                          ack_status, natsStatus_GetText(ack_status), topic->debug_desc);
            ret_code = -1;
        }
        else if (ndw_verbose > 2) {
            NDW_LOGX("natsMsg_Ack(...) OKAY on <%s>!\n", topic->debug_desc);
        }
    }

    natsMsg_Destroy(c->nats_msg);

    c->nats_topic = NULL;
    c->nats_connection = NULL;
    c->nats_msg = NULL;
    c->msg_size = 0;

#if 0
    // Wait for cleanup operator to invoke CleanupQueuedMsg
    free(c);
#endif

    return ret_code;

} // end method ndw_NATS_CommitQueuedMsg

INT_T
ndw_NATS_CleanupQueuedMsg(ndw_Topic_T* topic, void* vendor_closure)
{
    ndw_NATS_Closure_T* c = (ndw_NATS_Closure_T*) vendor_closure;
    if (NULL == c) {
        NDW_LOGERR("*** FATAL ERROR: vendor_closure is NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    if (ndw_verbose > 3) {
        NDW_LOGX("Closure: is_js<%s> msg_size<%d> topic<%s>\n",
                    (c->is_js ? "Y" : "n"), c->msg_size, topic->debug_desc);
    }

    INT_T ret_code = 0;

    if (c->is_js && (NULL != c->nats_msg)) {
#if 0
        //
        // When we cleaning up messages implies that we never want to ack those.
        // Cleanup is called during shutdown and these messages were never handled.
        // All we can do is destroy the NATS message but NOT ack a persistent one!
        //
        natsStatus ack_status = natsMsg_Ack(c->nats_msg, NULL);
        if (NATS_OK != ack_status) {
            NDW_LOGERR("*** ERROR: natsMsg_Ack(...) failed with status<%d> status_text<%s> for %s\n",
                          ack_status, natsStatus_GetText(ack_status), topic->debug_desc);
            ret_code = -1;
        }
        else if (ndw_verbose > 2) {
            NDW_LOGX("natsMsg_Ack(...) OKAY on <%s>!\n", topic->debug_desc);
        }
#endif
    }

    if (NULL != c->nats_msg) {
        natsMsg_Destroy(c->nats_msg);
        c->nats_msg = NULL;
    }

    free(c);

    return ret_code;

} // end method ndw_NATS_CleanupQueuedMsg

INT_T
ndw_NATS_CommitLastMsg(ndw_Topic_T* topic, void* vendor_closure)
{
    if (NULL == topic) {
        NDW_LOGERR("*** FATAL ERROR: NULL Topic Pointer\n");
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == vendor_closure) {
        NDW_LOGERR("*** FATAL ERROR: NULL vendor_closure Pointer for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_Closure_T* arg_closure = (ndw_NATS_Closure_T*) vendor_closure;

    ndw_NATS_Closure_T* closure = (ndw_NATS_Closure_T*) pthread_getspecific(ndw_NATS_tls_closure);
    if (NULL == closure) {
        NDW_LOGERR("*** FATAL ERROR: NULL vendor_closure Pointer in ndw_NATS_tls_closure for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (arg_closure != closure) {
        NDW_LOGERR("*** FATAL ERROR: vendor_closure Pointer <%p> and closure Pointer <%p> "
                    "in ndw_NATS_tls_closure do NOT match %s\n", vendor_closure, closure, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (arg_closure->counter != closure->counter) {
        NDW_LOGERR("*** FATAL ERROR: vendor_closure counter <%ld> and closure Pointer <%ld> "
                    "in ndw_NATS_tls_closure do NOT match for %s\n",
                    arg_closure->counter, closure->counter, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == arg_closure->topic) {
        NDW_LOGERR("*** FATAL ERROR: vendor_closure has NULL ndw_Topic_T Pointer for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (closure->topic != arg_closure->topic) {
        NDW_LOGERR("*** FATAL ERROR: vendor_closure ndw_Topic_T Pointer<%p> is different from "
                    " arg_closure Topic Pointer<%p> for %s\n", closure->topic, arg_closure->topic, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_Topic_T* nats_topic = closure->nats_topic;
    if (NULL == nats_topic) {
        NDW_LOGERR("*** FATAL ERROR: vendor_closure has NULL ndw_NATS_Topic_T Pointer for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_NATS_Connection_T* nats_connection = closure->nats_connection;
    if (NULL == nats_connection) {
        NDW_LOGERR("*** FATAL ERROR: vendor_closure has NULL ndw_NATS_Connection_T Pointer for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    natsMsg* nats_msg = closure->nats_msg;
    if (NULL == nats_msg) {
        NDW_LOGERR("*** FATAL ERROR: vendor_closure has NULL natsMsg Pointer for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (closure->msg_size <= 0) {
        NDW_LOGERR("*** FATAL ERROR: vendor_closure has invalid msg_size<%d> natsMsg msg for %s\n",
                    closure->msg_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (closure->is_js) {
        natsStatus ack_status = natsMsg_Ack(nats_msg, NULL);
        if (NATS_OK != ack_status) {
            topic->msgs_failed_ack_count += 1;
            NDW_LOGERR("*** ERROR: natsMsg_Ack(...) failed with status<%d> status_text<%s> for %s\n",
                          ack_status, natsStatus_GetText(ack_status), topic->debug_desc);
        }
        else {
            topic->msgs_ack_count += 1;
            if (ndw_verbose > 4) {
                NDW_LOGX("natsMsg_Ack(...) OKAY!\n");
            }
        }
    }

    topic->msgs_commit_count += 1;

    natsMsg_Destroy(nats_msg);

    NDW_CLEAR_CLOSURE(topic);

    return 0;

} // end method ndw_NATS_CommitLastMsg

