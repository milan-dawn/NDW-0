
#include "VendorImpl.h"
#include "AbstractMessaging.h"


// Setting this greater than zero will trigger verbose output.
INT_T ndw_verbose = 0;

// Setting this greater than zero will trigger verbose output on msg_headers.
// A value of 1 warns on invalid message headers.
// A value > 1 prints debugging information on message headers.
INT_T ndw_debug_msg_header = 0;

INT_T ndw_app_id = -1;
INT_T ndw_capture_latency = 0;

void ndw_tls_ndw_error_Destructor(void* ptr)
{
#if 1
    free(ptr);
#endif
}

pthread_key_t   ndw_tls_ndw_error;
pthread_once_t  ndw_tls_ndw_error_once = PTHREAD_ONCE_INIT;
static void  ndw_tls_ndw_error_Init()
{
    if (0 != pthread_key_create(&ndw_tls_ndw_error, ndw_tls_ndw_error_Destructor))
    {
        NDW_LOGERR("*** FATAL ERROR: Failed to create ndw_tls_ndw_error!\n");
        ndw_exit(EXIT_FAILURE);
    }
}

static ndw_ImplAPI_T ndw_impl_api_structure[NDW_MAX_API_IMPLEMENTATIONS + 1];
ndw_ImplAPI_T* ndw_impl_api = &ndw_impl_api_structure[0];

ndw_AsyncCallbackPtr_T ndw_async_callback_ptr = NULL;
ndw_BadMessageCallbackPtr_T ndw_bad_message_callback_ptr = NULL;

ndw_GOAsyncMsgHandler ndw_go_msghandler = NULL;

/*
 * Queue Item when Asynchronous Messages are to be queued.
 *
 */
typedef struct ndw_QAsync_Item
{
    ndw_Topic_T* topic;
    void* vendor_closure;
    UCHAR_T* msg;
    INT_T msg_size;
} ndw_QAsync_Item_T;

void
ndw_QAsync_CleanupOperator(void* item)
{
    ndw_QAsync_Item_T* q_item = (ndw_QAsync_Item_T*) item;


    ndw_Topic_T* t = q_item->topic;
    if (NULL == t) {
        NDW_LOGERR("*** FATAL ERROR: ndw_Topic_T Pointer is NULL in ndw_QAsync_Item_T* structure!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Connection_T* c = t->connection;
    if (NULL == c) {
        NDW_LOGERR("*** FATAL ERROR: ndw_Connection_T Pointer is in Topic Structure for %s\n", t->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_ImplAPI_T *impl = &ndw_impl_api_structure[c->vendor_id];
    impl->CleanupQueuedMsg(t, q_item->vendor_closure);

    q_item->topic = NULL;
    q_item->vendor_closure = NULL;
    q_item->msg = NULL;
    q_item->msg_size = 0;

    free(q_item);
} // end method ndw_QAsync_CleanupOperator

/*
 * We keep track of the last message received on a Synchronous poll.
 * And we free it on subsequent invocations to Synchronous poll.
 * Thereby relieving of the app to free the allocated memory.
 * At Shutdown we check for this too.
 */
typedef struct ndw_LastSyncPollMsg
{
    ndw_Topic_T* topic;
    void* vendor_closure;
    CHAR_T* msg;
    INT_T msg_size;
} ndw_LastSyncPollMsg_T;

void ndw_tls_lastsyncpollmsg_Destructor(void* ptr)
{
    ndw_LastSyncPollMsg_T* last_sync_poll_msg = (ndw_LastSyncPollMsg_T*) ptr;
    if (NULL != last_sync_poll_msg) {
        last_sync_poll_msg->topic = NULL;
        last_sync_poll_msg->vendor_closure = NULL;
        free(last_sync_poll_msg->msg);
        last_sync_poll_msg->msg = NULL;
        last_sync_poll_msg->msg_size = 0;
        free(last_sync_poll_msg);
    }
}

pthread_key_t ndw_tls_lastsyncpollmsg;
pthread_once_t ndw_tls_lastsyncpollmsg_once = PTHREAD_ONCE_INIT;
static void ndw_tls_lastsyncpollmsg_Init()
{
    if (0 != pthread_key_create(&ndw_tls_lastsyncpollmsg, ndw_tls_lastsyncpollmsg_Destructor))
    {
        NDW_LOGERR("*** FATAL ERROR: Failed to create ndw_tls_lastsyncpollmsg!\n");
        ndw_exit(EXIT_FAILURE);
    }
}


ndw_ErrorDetails_T*
ndw_GetErrorDetails()
{
    ndw_ErrorDetails_T* errMsg = pthread_getspecific(ndw_tls_ndw_error);
    if (NULL == errMsg) {
        NDW_LOGERR("*** FATAL ERROR: ndw_tls_ndw_error has no per thread ErrorDetails!\n");
        ndw_exit(EXIT_FAILURE);
    }

    return errMsg;
} // end method ndw_GetErrorDetails


ndw_ErrorDetails_T*
ndw_ClearErrorDetails()
{
    ndw_ErrorDetails_T* errD = ndw_GetErrorDetails();
    memset(errD, 0, sizeof(ndw_ErrorDetails_T));
    return errD;
} // end method ndw_ClearErrorDetails

void
ndw_print_BadMessage(ndw_BadMessage_T* bad_msg)
{
    if (NULL != bad_msg) {
        NDW_LOGERR( "*** BAD MESSAGE: "
            "TimeUTC<%ld> header_id<%d> header_size<%d> msg_size<%d> on Internal Message<%s> for %s\n",
            bad_msg->received_time, bad_msg->header_id, bad_msg->header_size, bad_msg->msg_size,
            ((NULL == bad_msg->error_msg) ? "?" : bad_msg->error_msg),
            (NULL == bad_msg->topic) ? "?": bad_msg->topic->debug_desc);
    }
                    
} // end method ndw_print_BadMessage

INT_T
ndw_GetAppId()
{
    return ndw_app_id;
}

ndw_ImplAPI_T*
get_Implementation_API(INT_T vendor_id)
{
    if ((vendor_id < 0) || (vendor_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
        // NOTE: This might be invoked in such a way that stdio functions printf and fprint might NOT work!
        // In such a case all you can do is test it out inside the debugger!
        exit(EXIT_FAILURE);
    }

    return &ndw_impl_api_structure[vendor_id];

} // end method get_Implementation_API

// Returns number of vendors configured.
INT_T
ndw_initialize_API_Implementations()
{
    ndw_ImplAPI_T *impl = NULL;
    INT_T num_vendors = 0;
    INT_T init_code = 0;

    for (INT_T i = 0; i < NDW_MAX_API_IMPLEMENTATIONS; i++)
    {
        impl = &ndw_impl_api_structure[i];
        if (0 == ndw_impl_api[i].vendor_id)
        { 
            impl->vendor_id = -1 * (i+1);
        }
        else
        {
            if (i != impl->vendor_id) {
                NDW_LOGERR( "*** FATAL ERROR: Error at Index <%d> and Impl vendor ID does not match which is: <%d>\n",
                            i, impl->vendor_id);
                ndw_exit(EXIT_FAILURE);
            }

            if (NULL == impl->Init) {
                NDW_LOGERR( "*** FATAL ERROR: Vendor Init() function not set for Impl vendor ID <%d>\n", impl->vendor_id);
                ndw_exit(EXIT_FAILURE);
            }

            if (0 != (init_code = impl->Init(impl, i))) {
                NDW_LOGERR( "*** FATAL ERROR: Vendor Init() function failed with return code <%d> for vendor ID <%d>\n", init_code, impl->vendor_id);
                ndw_exit(EXIT_FAILURE);
            }

            if (ndw_verbose) {
                NDW_LOGX("===> Vendor<%d, %s, %d> configured!\n", impl->vendor_id, impl->vendor_name, impl->vendor_logical_version);
            }

            ++num_vendors;
        }
    }

    if (ndw_verbose)
        NDW_LOGX("*** NOTE: Number of Vendors configured <%d>\n", num_vendors);

    if (0 == num_vendors)
        NDW_LOGERR( "*** WARNING: There are no Vendors configured!\n");

    return num_vendors;

} // end method ndw_initialize_API_Implementations()

static void
ndw_ProcessVendorConfigurations()
{
    NDW_LOGX("\n\n====== BEGIN: Processing Vendor Configurations ======\n\n");

    INT_T total_domains = 0;
    ndw_Domain_T** domains = ndw_GetAllDomains(&total_domains);
    if ((NULL == domains) || (total_domains <= 0)) {
        NDW_LOGERR("*** FATAL ERROR: There are NO Domains defined!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Domain_T* d = NULL;
    for (INT_T i = 0; i < total_domains; i++)
    {
        d = domains[i];
        NDW_LOG("[%d] ==> %s\n\n", i, d->debug_desc);

        INT_T total_connections = 0;
        ndw_Connection_T** connections = ndw_GetAllConnectionsFromDomain(d, &total_connections);
        if ((NULL == connections) || (total_connections <= 0))
            continue;

        for (INT_T j = 0; j < total_connections; j++)
        {
            ndw_Connection_T* c = connections[j];
            NDW_LOG("\n---> [%d] %s\n", j, c->debug_desc);

            INT_T impl_id = c->vendor_id;

            if ((impl_id <= 0) || (impl_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
                NDW_LOGERR( "*** ERROR: Invalid impl_id <%d> for %s\n", impl_id, c->debug_desc);
                exit(EXIT_FAILURE);
            }

            ndw_ImplAPI_T* impl = &ndw_impl_api_structure[impl_id];
            if (NULL == impl) {
                NDW_LOGERR( "*** ERROR: Invalid Impl Pointer for impl_id <%d> for %s\n", impl_id, c->debug_desc);
                exit(EXIT_FAILURE);
            }

            INT_T total_topics = 0;
            ndw_Topic_T** topics = ndw_GetAllTopicsFromConnection(c, &total_topics);
            if ((NULL  == topics) || (total_topics <=0)) 
                continue;

            for (INT_T k = 0; k < total_topics; k++)
            {
                ndw_Topic_T* t = topics[k];
                NDW_LOG("\n\n> [%d] %s\n", k, t->debug_desc);
                impl->ProcessConfiguration(t);

                if (t->q_async_enabled) {
                    ndw_QSetCleanupOperator(t->q_async, ndw_QAsync_CleanupOperator);
                }
            } // Loop Topics

            free(topics);
            
        } // Loop Connections

        free(connections);

    } // Loop Domains

    free(domains);

    NDW_LOGX("\n\n====== END: Processing Vendor Configurations ======n\n");

} // end method ndw_ProcessVendorConfigurations

static INT_T
ndw_CreatePerThreadDataStructures()
{
    pthread_once(&ndw_tls_ndw_error_once, ndw_tls_ndw_error_Init);
    pthread_setspecific(ndw_tls_ndw_error, calloc(1, sizeof(ndw_ErrorDetails_T)));

    pthread_once(&ndw_tls_lastsyncpollmsg_once, ndw_tls_lastsyncpollmsg_Init);
    pthread_setspecific(ndw_tls_lastsyncpollmsg, calloc(1, sizeof(ndw_LastSyncPollMsg_T)));

    return 0;
} // end method ndw_CreatePerThreadDataStructures

static INT_T
ndw_DestroyPerThreadDataStructures()
{
    pthread_t this_thread = pthread_self();
#if 1
    if (NDW_IS_NDW_INIT_THREAD(this_thread)) {
        ndw_LastSyncPollMsg_T* last_sync_poll_msg =
        (ndw_LastSyncPollMsg_T*) pthread_getspecific(ndw_tls_lastsyncpollmsg);
        if (NULL != last_sync_poll_msg) {
            last_sync_poll_msg->topic = NULL;
            free(last_sync_poll_msg->msg);
            last_sync_poll_msg->msg = NULL;
            last_sync_poll_msg->msg_size = 0;
            free(last_sync_poll_msg);
        }
    }
#endif
    ndw_safe_PTHREAD_KEY_DELETE("ndw_tls_lastsyncpollmsg", ndw_tls_lastsyncpollmsg);

#if 1
    if (NDW_IS_NDW_INIT_THREAD(this_thread)) {
        ndw_ErrorDetails_T* errMsg = pthread_getspecific(ndw_tls_ndw_error);
        free(errMsg);
    }
#endif
    ndw_safe_PTHREAD_KEY_DELETE("ndw_tls_ndw_error", ndw_tls_ndw_error);

    return 0;
} // end method ndw_DestroyPerThreadDataStructures


INT_T
ndw_ThreadInit()
{
    if (ndw_verbose > 1) {
        NDW_LOGX("TLS: Per Thread data structure creation...\n");
    }

    ndw_InitThreadSpecificMsgHeaders();
    ndw_CreatePerThreadDataStructures();

    
    // Per thread data structure initialization for vendor implementations.
    for (INT_T i = 0; i < NDW_MAX_API_IMPLEMENTATIONS; i++ ) {
        ndw_ImplAPI_T* api_impl = &ndw_impl_api_structure[i];
        if ((NULL != api_impl) && (NULL != api_impl->ThreadInit)) {
                api_impl->ThreadInit(api_impl, api_impl->vendor_id);
        }
    }

    return 0;
} // end method ndw_ThreadInit

INT_T
ndw_ThreadExit()
{
    if (ndw_verbose > 1) {
        NDW_LOGX("TLS: Per Thread data structure destruction...\n");
    }

    ndw_DestroyThreadSpecificMsgHeaders();

    ndw_DestroyPerThreadDataStructures();

    // Per thread data structure destruction for vendor implementations.
    for (INT_T i = 0; i < NDW_MAX_API_IMPLEMENTATIONS; i++ ) {
        ndw_ImplAPI_T* api_impl = &ndw_impl_api_structure[i];
        if ((NULL != api_impl) && (NULL != api_impl->ThreadInit)) {
                api_impl->ThreadExit();
        }
    }

    return 0;
} // end method ndw_ThreadExit

INT_T
ndw_Init()
{
    pthread_self_ndw_Init = pthread_self();
    
    INT_T ret = 0;

    const CHAR_T* verboseness = getenv(NDW_VERBOSE);
    if ((NULL != verboseness) && ('\0' != *verboseness)) {
        ndw_verbose = atoi(verboseness);
        if (ndw_verbose < 0)
            ndw_verbose = 0;
    }

    const CHAR_T* debug_msg_header = getenv(NDW_DEBUG_MSG_HEADERS);
    if ((NULL != debug_msg_header) && ('\0' != *debug_msg_header)) {
        ndw_debug_msg_header = atoi(debug_msg_header);
        if (ndw_debug_msg_header < 0)
            debug_msg_header = 0;
    }

    ndw_InitThreadSafeLogger();

    NDW_LOGX("NDW_VERBOSE = %d\n", ndw_verbose);

    const CHAR_T* config_file_path = getenv(NDW_APP_CONFIG_FILE);
    if ((NULL == config_file_path) ||  ('\0' == *config_file_path)) {
        NDW_LOGERR( "Env Variable <%s> NOT set!\n", NDW_APP_CONFIG_FILE);
        return -1;
    }

    NDW_LOGX("%s = <%s>\n", config_file_path, config_file_path);

    const CHAR_T* app_domains = getenv(NDW_APP_DOMAINS);
    if ((NULL == app_domains) || ('\0' == *app_domains)) {
        NDW_LOGERR( "Env Variable <%s> NOT set!\n", NDW_APP_DOMAINS);
        return -2;
    }

    NDW_LOGX("%s = <\"%s\">\n", NDW_APP_DOMAINS , app_domains);

    CHAR_T** domains = ndw_split_string(app_domains, ',');
    if (NULL == domains) {
        NDW_LOGERR( "Env Variable <%s> does not contain a domain list!\n", NDW_APP_DOMAINS);
        return -3;
    }

    INT_T i = 0;
    const CHAR_T* ptr = domains[i];
    while ((NULL != ptr) && ('\0' != *ptr)) {
        NDW_LOGX("Domain[%d] = <\"%s\">\n", i, ptr);
        ptr = domains[++i];
    }
    
    
    const CHAR_T* p_app_id = getenv(NDW_APP_ID);
    if ((NULL == p_app_id) || ('\0' == *p_app_id)) {
        NDW_LOGERR( "Env Variable <%s> NOT set!\n", NDW_APP_ID);
        return -3;
    }

    INT_T app_id_value = atoi(p_app_id);
    NDW_LOGX("APP_ID = <%d>\n", app_id_value);

    if ((app_id_value <= 0) || (app_id_value > SHRT_MAX)) {
        NDW_LOGERR( "Invalid Env Variable <%s> value: %d\n", NDW_APP_ID, app_id_value);
        return -3;
    }

    ndw_app_id = app_id_value;

    CHAR_T*  p_capture_latency = getenv(NDW_CAPTURE_LATENCY);
    if ((NULL != p_capture_latency) && ('\0' != *p_capture_latency)) {
        ndw_capture_latency = atoi(p_capture_latency);
        if (ndw_capture_latency < 0)
            ndw_capture_latency = 0;
    }

    ret = ndw_InitializeRegistry();
    if (0 != ret) {
        NDW_LOGERR( "*** ERROR: ndw_InitializeRegistry failed with return code<%d>!\n", ret);
        return -4;
    }

    ret = ndw_LoadDomains(config_file_path, domains);
    if (0 != ret) {
        NDW_LOGERR( "*** ERROR: FAILED to load or parse <%s> with return code <%d>\n",
                    config_file_path, ret);
        return -5;
    }

    if (ndw_verbose) {
        ndw_DebugAndPrintDomainConfigurations();
    }

    ndw_Domain_T* pDomain = NULL;
    i = 0;
    const CHAR_T* domain_name = domains[i];
    while ((NULL != domain_name) && ('\0' != *domain_name)) {
        if (NULL == (pDomain = ndw_GetDomainByName(domain_name))) {
            NDW_LOGERR( "*** ERROR: FAILED to get ndw_Domain_T POINTER from Domain Name \"<%s>\"\n", domain_name);
            return -6;
        }
        domain_name = domains[++i];
    }

    ndw_InitMsgHeaders();
    ndw_ThreadInit(); // Initialize all per thread data structures.

    ndw_initialize_API_Implementations();


    ndw_free_string_list(domains);

    // Process Vendor Configurations.
    // If there is an error it will exit the application!
    ndw_ProcessVendorConfigurations();

    // Initialize message callback function POINTER.
    ndw_async_callback_ptr = (ndw_AsyncCallbackPtr_T)
            dlsym(RTLD_DEFAULT, NDW_ASYNC_CALLBACK_FUNCTION_NAME);

    const CHAR_T* err = dlerror();
    if (NULL != err) {
        NDW_LOGERR( "*** WARNING: Could not get reference to POINTER to function with name <%s> with dlsym error as <%s>\n", NDW_ASYNC_CALLBACK_FUNCTION_NAME, err);
        //exit(EXIT_FAILURE);
    }

    if (NULL == ndw_async_callback_ptr) {
        NDW_LOGERR( "*** WARNING: Could not get reference to POINTER to function with name <%s>\n", NDW_ASYNC_CALLBACK_FUNCTION_NAME);
        //exit(EXIT_FAILURE);
    }

#if 0
    if (ndw_verbose > 2) {
        INT_T result = ndw_async_callback_ptr(NULL, NULL);
        NDW_LOGX("Testing: ndw_async_callback result = <%d>\n", result);
    }
#endif

    // Initialize bad message callback function POINTER.
    ndw_bad_message_callback_ptr = (ndw_BadMessageCallbackPtr_T)
            dlsym(RTLD_DEFAULT, NDW_BAD_MESSAGE_FUNCTION_NAME);

    err = dlerror();
    if (NULL != err) {
        if (ndw_verbose > 4) {
            NDW_LOGERR( "** WARNING: FAILED to get reference to POINTER to function with name <%s> with dlsym error as <%s>\n",
                    NDW_BAD_MESSAGE_FUNCTION_NAME, err);
        }
    }
    else {
            ndw_BadMessage_T bad_msg;
            memset(&bad_msg, 0, sizeof(ndw_BadMessage_T));
            bad_msg.received_time = ndw_GetCurrentUTCNanoseconds();
            bad_msg.header_id = 255;
            bad_msg.header_size = 250;
            bad_msg.error_msg = "Just testing callback...";

            // Just a test!
            ndw_bad_message_callback_ptr(&bad_msg);
    }

    return 0;

} // end method NDW_Init

INT_T
ndw_Connect(const CHAR_T* domain_name, const CHAR_T* connection_name)
{
    ndw_Connection_T* connection = ndw_GetConnectionByName(domain_name, connection_name);
    if (NULL == connection) {
        NDW_LOGERR( "** WARNING: Cannot find Connection for: <%s, %s>\n", 
                ((NULL == domain_name)? "" : domain_name), (NULL == connection_name) ? "" : connection_name);
        return -1;
    }

    if (connection->disabled)
        return 0;

    INT_T impl_id = connection->vendor_id;
    if ((impl_id < 1) || (impl_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
        NDW_LOGERR( "*** FATAL ERROR: Invalid connection vendor_id <%d> for %s\n", impl_id, connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == ndw_impl_api[impl_id].Connect) {
        NDW_LOGERR( "*** FATAL ERROR: Connect Function Pointer NOT set! impl_id<%d> for %s\n", impl_id, connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    INT_T ret_code = ndw_impl_api[impl_id].Connect(connection);
    return ret_code;
} // end method ndw_Connect

INT_T
ndw_ConnectToDomains(const CHAR_T** domains)
{
    if (NULL == domains)
        return -1;

    for (const CHAR_T** ptr = domains; NULL != *ptr; ptr++)
    {
        if (NULL != *ptr) {
            NDW_LOGX("Trying to intiate all Connections for Domain<%s>\n", *ptr);
            INT_T ret_code = ndw_ConnectToDomain(*ptr);
            if (0 != ret_code) {
                NDW_LOGERR("Some or ALL Connections to Domain FAILED with code<%d>\n", ret_code);
                return -2;
            }
        }
    }

    return 0;
} // ndw_ConnectToDomains

// Returns 0 if all connections okay, else number of failed connections.
INT_T
ndw_ConnectToDomain(const CHAR_T* domain)
{
    if (NULL == domain) {
        NDW_LOGERR("Input parameter domain is NULL!\n");
        return -1;
    }
    
    ndw_Connection_T** list_failed = ndw_ConnectAll(domain);
    if (NULL == list_failed)
        return 0;

    INT_T fail_count = 0;
    for (ndw_Connection_T* c = list_failed[fail_count]; NULL != c; c = list_failed[++fail_count])
        ;

    free(list_failed);
    return fail_count;
} // end method ndw_ConnectToDomain


// Returns a list of failed connections. Returns NULL list if no failures.
// List is null terminated. It is the responsiblity of the call to free the list.
ndw_Connection_T**
ndw_ConnectAll(const CHAR_T* domain)
{
    if (NULL == domain) {
        NDW_LOGERR("Input parameter domain is NULL!\n");
        return NULL;
    }

    ndw_Domain_T* d = ndw_GetDomainByName(domain);
    if (NULL == d) {
        NDW_LOGERR( "*** ERROR: Domain <%s> not found!\n", domain);
        return NULL;
    }

    INT_T total_connections = 0;
    ndw_Connection_T** list = ndw_GetAllConnectionsFromDomain(d, &total_connections);
    if (NULL == list) {
        NDW_LOGERR( "*** ERROR: Domain <%s> has NO connections configured!\n", domain);
        return NULL;
    }

    if (0 == total_connections) {
        NDW_LOGERR( "*** ERROR: The Domain <%s> has NO connections configured!\n", domain);
        free(list);
        return NULL;
    }

    INT_T index_failed = 0;
    INT_T ret_code = 0;
    for (INT_T i = 0; i < total_connections; i++) {
        ndw_Connection_T* conn = list[i];
        const CHAR_T* connection_name = conn->connection_unique_name;
        ret_code = ndw_Connect(domain, connection_name);
        if (ret_code < 0) {
            NDW_LOGERR( "*** ERROR: FAILED to Connect for %s\n", conn->debug_desc);
            list[index_failed++] = conn;
        }
    }

    if (index_failed > 0) {
        if (index_failed > total_connections) {
            NDW_LOGERR( "*** FATAL ERROR: Invalid Index <%d> withtotal indices <%d>\n", index_failed, total_connections);
            ndw_exit(EXIT_FAILURE);
        }

        list[index_failed] = NULL;
        return list;
    }
    else {
        free(list);
        return NULL;
    }
} // end method ndw_ConnectAll

INT_T
ndw_Disconnect(const CHAR_T* domain_name, const CHAR_T* connection_name)
{
    ndw_Connection_T* connection = ndw_GetConnectionByName(domain_name, connection_name);
    if (NULL == connection) {
        NDW_LOGERR( "** WARNING: Cannot find Connection for: <%s, %s>\n", 
                ((NULL == domain_name)? "" : domain_name), (NULL == connection_name) ? "" : connection_name);
        return -1;
    }

    if (connection->disabled)
        return 0;

    INT_T impl_id = connection->vendor_id;
    if ((impl_id < 1) || (impl_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
        NDW_LOGERR( "*** FATAL ERROR: Invalid connection impl_id<%d> for %s\n", impl_id, connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == ndw_impl_api[impl_id].Disconnect) {
        NDW_LOGERR( "*** FATAL ERROR: Disconnect Function Ptr NOT set! impl_id<%d> for %s\n", impl_id, connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    INT_T ret_code = ndw_impl_api[impl_id].Disconnect(connection);
    return ret_code;

} // end method ndw_Disconnect

bool
ndw_IsConnected(const CHAR_T* domain_name, const CHAR_T* connection_name)
{
    ndw_Connection_T* connection = ndw_GetConnectionByName(domain_name, connection_name);
    if (NULL == connection) {
        NDW_LOGERR( "** WARNING: Cannot find Connection for: <%s, %s>\n",
                ((NULL == domain_name)? "" : domain_name), (NULL == connection_name) ? "" : connection_name);
        return -1;
    }

    if (connection->disabled) {
        return false;
    }

    INT_T impl_id = connection->vendor_id;
    if ((impl_id < 1) || (impl_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
        NDW_LOGERR( "*** FATAL ERROR: Invalid connection vendor_id <%d> for %s\n", impl_id, connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == ndw_impl_api[impl_id].IsConnected) {
        NDW_LOGERR( "*** FATAL ERROR: Connect Function Pointer NOT set! impl_id<%d> for %s\n", impl_id, connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    bool ret_code = ndw_impl_api[impl_id].IsConnected(connection);
    return ret_code;
} // end method ndw_IsConnected

bool
ndw_IsClosed(const CHAR_T* domain_name, const CHAR_T* connection_name)
{
    ndw_Connection_T* connection = ndw_GetConnectionByName(domain_name, connection_name);
    if (NULL == connection) {
        NDW_LOGERR( "** WARNING: Cannot find Connection for: <%s, %s>\n",
                ((NULL == domain_name)? "" : domain_name), (NULL == connection_name) ? "" : connection_name);
        return -1;
    }

    if (connection->disabled) {
        return false;
    }

    INT_T impl_id = connection->vendor_id;
    if ((impl_id < 1) || (impl_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
        NDW_LOGERR( "*** FATAL ERROR: Invalid connection vendor_id <%d> for %s\n", impl_id, connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == ndw_impl_api[impl_id].IsClosed) {
        NDW_LOGERR( "*** FATAL ERROR: Connect Function Pointer NOT set! impl_id<%d> for %s\n", impl_id, connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    bool ret_code = ndw_impl_api[impl_id].IsClosed(connection);
    return ret_code;
} // end method ndw_IsClosed

bool
ndw_IsDraining(const CHAR_T* domain_name, const CHAR_T* connection_name)
{
    ndw_Connection_T* connection = ndw_GetConnectionByName(domain_name, connection_name);
    if (NULL == connection) {
        NDW_LOGERR( "** WARNING: Cannot find Connection for: <%s, %s>\n",
                ((NULL == domain_name)? "" : domain_name), (NULL == connection_name) ? "" : connection_name);
        return -1;
    }

    if (connection->disabled) {
        return false;
    }

    INT_T impl_id = connection->vendor_id;
    if ((impl_id < 1) || (impl_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
        NDW_LOGERR( "*** FATAL ERROR: Invalid connection vendor_id <%d> for %s\n", impl_id, connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == ndw_impl_api[impl_id].IsDraining) {
        NDW_LOGERR( "*** FATAL ERROR: Connect Function Pointer NOT set! impl_id<%d> for %s\n", impl_id, connection->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    bool ret_code = ndw_impl_api[impl_id].IsDraining(connection);
    return ret_code;
} // end method ndw_IsDraining


// Returns a list of failed connections on which disconnect failed. Returns NULL list if no failures.
// List is null terminated. It is the responsiblity of the call to free the list.
ndw_Connection_T**
ndw_DisconnectAll(const CHAR_T* domain)
{
    if (NULL == domain)
        return NULL;

    ndw_Domain_T* d = ndw_GetDomainByName(domain);
    if (NULL == d) {
        NDW_LOGERR( "*** ERROR: Domain <%s> not found!\n", domain);
        return NULL;
    }

    INT_T total_connections = 0;
    ndw_Connection_T** list = ndw_GetAllConnectionsFromDomain(d, &total_connections);
    if (NULL == list) {
        NDW_LOGERR( "*** ERROR: Domain <%s> has NO connections configured!\n", domain);
        return NULL;
    }

    if (0 == total_connections) {
        NDW_LOGERR( "*** ERROR: The Domain <%s> has NO connections configured!\n", domain);
        free(list);
        return NULL;
    }

    INT_T index_failed = 0;
    INT_T ret_code = 0;
    for (INT_T i = 0; i < total_connections; i++) {
        ndw_Connection_T* conn = list[i];
        const CHAR_T* connection_name = conn->connection_unique_name;
        ret_code = ndw_Disconnect(domain, connection_name);
        if (ret_code < 0) {
            NDW_LOGERR( "*** ERROR: FAILED to Disconnect for %s\n", conn->debug_desc);
            list[index_failed++] = conn;
        }
    }

    if (index_failed > 0) {
        if (index_failed > total_connections) {
            NDW_LOGERR( "*** FATAL ERROR: Invalid Index <%d> with total indices <%d>\n", index_failed, total_connections);
            ndw_exit(EXIT_FAILURE);
        }

        list[index_failed] = NULL;
        return list;
    }
    else {
        free(list);
    }

    return NULL;
} // end method ndw_DisconnectAll

void
ndw_Shutdown()
{
    NDW_LOGX("\n\n===> BEGIN: ndw_Shutdown() ...\n\n");

    pthread_t pthread_self_ndw_Shutdown = pthread_self();
    NDW_LOGX("==> ndw_Shutdown Thread ID<%lu>\n", pthread_self_ndw_Shutdown);
    if (pthread_self_ndw_Shutdown != pthread_self_ndw_Init) {
        NDW_LOGERR("*** FATAL ERROR: ndw_Shutdown called from Thread ID<%lu> "
                    "while NDW_Init was invoked from Thread ID<%lu>\n",
                    pthread_self_ndw_Shutdown, pthread_self_ndw_Init); 
        ndw_exit(EXIT_FAILURE);
    }

    INT_T total_domains = 0;
    ndw_Domain_T** domains = ndw_GetAllDomains(&total_domains);

    if ((NULL != domains) && (total_domains > 0))
    {
        ndw_Domain_T* d = NULL;
        for (INT_T i = 0; i < total_domains; i++)
        {
            d = domains[i];
            INT_T total_connections = 0;
            ndw_Connection_T** connections = 
                ndw_GetAllConnectionsFromDomain(d, &total_connections);

            if ((NULL != connections) && (total_connections > 0))
            {
                for (INT_T j = 0; j < total_connections; j++)
                {
                    ndw_Connection_T* c = connections[j];

                    INT_T impl_id = c->vendor_id;
                    if ((impl_id <= 0) || (impl_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
                        NDW_LOGERR( "*** ERROR: Invalid impl_id <%d> for %s\n", impl_id, c->debug_desc);
                        exit(EXIT_FAILURE);
                    }

                    ndw_ImplAPI_T* impl = &ndw_impl_api_structure[impl_id];
                    if (NULL == impl) {
                        NDW_LOGERR( "*** ERROR: Invalid Impl Pointer for impl_id <%d> for %s\n", impl_id, c->debug_desc);
                        exit(EXIT_FAILURE);
                    }

                    INT_T total_topics = 0;
                    ndw_Topic_T** topics = 
                    ndw_GetAllTopicsFromConnection(c, &total_topics);

                    if ((NULL != topics) && (total_topics > 0))
                    {
                        for (INT_T k = 0; k < total_topics; k++)
                        {
                            ndw_Topic_T* t = topics[k];
                            impl->Unsubscribe(t);

                            if (NULL != t->q_async) {
                                ndw_QCleanup(t->q_async);
                                free(t->q_async);
                                t->q_async = NULL;
                            }
                        }
                    }

                    free(topics);

                    if (NULL == impl->Disconnect) {
                        NDW_LOGERR( "*** ERROR: Invalid Impl POINTER for impl_id <%d> for %s\n", impl_id, c->debug_desc);
                        exit(EXIT_FAILURE);
                    }

                    impl->Disconnect(c);
                    impl->ShutdownConnection(c);
                }
            } // end connections

            free(connections);

        } // end domains

        free(domains);
    }

    for (INT_T i = 0; i < NDW_MAX_API_IMPLEMENTATIONS; i++ ) {
        ndw_ImplAPI_T* api_impl = &ndw_impl_api_structure[i];
        if ((NULL != api_impl) && (NULL != api_impl->Shutdown)) {
                api_impl->Shutdown();
        }
    }

    ndw_CleanupRegistry();
    ndw_impl_api = NULL;

    ndw_ThreadExit(); // Destroy all per thread data structures.

    NDW_LOGX("\n\n===> END: ndw_Shutdown() ...\n\n");

    ndw_DestroyThreadSafeLogger();

#if 0
    pthread_exit(NULL);
#endif

} // end method ndw_Shutdown

// NOTE: msg can be NULL, and in that case set msg_size to zero too.
ndw_OutMsgCxt_T*
ndw_CreateOutMsgCxt(ndw_Topic_T* topic, INT_T header_id, INT_T msg_encoding_format, UCHAR_T* msg, INT_T msg_size)
{
    if (NULL == topic) {
        NDW_LOGERR( "*** ERROR: NULL ndw_Topic parameter!\n");
        return NULL;
    }

    ndw_Connection_T* conn = topic->connection;
    if (NULL == conn) {
        NDW_LOGERR("*** FATAL ERROR: FAILED to get ndw_Connnection_T* for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Domain_T* domain = topic->domain;
    if (NULL == domain) {
        NDW_LOGERR("*** FATAL ERROR: FAILED to get ndw_Domain_T* for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if ((header_id <= 0) || (header_id > NDW_MAX_HEADER_TYPES))  {
        NDW_LOGERR( "*** ERROR: Invalid header_id <%d> specified and for %s!\n", header_id, topic->debug_desc);
        return NULL;
    }

    if (msg_size < 0) {
        NDW_LOGERR( "*** ERROR: Invalid msg_size<%d> with header_id<%d> and for %s\n", msg_size, header_id, topic->debug_desc);
        return NULL;
    }

    if ((0 == msg_size) && (NULL != msg)) {
        NDW_LOGERR( "*** ERROR: Invalid msg_size <%d> for NULL msg parameter with header_id <%d> and for %s\n",
            msg_size, header_id, topic->debug_desc);
        return NULL;
    }

    if (0 == msg_size) {
        msg = NULL;
    }

    if (msg_size > NDW_MAX_MESSAGE_SIZE) {
        NDW_LOGERR( "*** ERROR: Too big a msg_size specified <%d> Max Possible<%d>. Request for header_id <%d> and for %s\n",
            msg_size, NDW_MAX_MESSAGE_SIZE, header_id, topic->debug_desc);
        return NULL;
    }

    if ((msg_encoding_format < 1) || (msg_encoding_format > NDW_MAX_ENCODING_FORMAT)) {
        NDW_LOGERR( "*** ERROR: Unspported encoding_format parameter <%d> msg_size specified <%d> for "
                    "NULL msg parameter with header_id <%d> and for %s\n",
                    msg_encoding_format, msg_size, header_id, topic->debug_desc);
    }

    INT_T header_size = ndw_MsgHeaderImpl[header_id].LE_MsgHeaderSize(header_id);
    if ((header_size <= 0) || (header_size > NDW_MAX_HEADER_SIZE)) {
        NDW_LOGERR("*** FATAL ERROR: Invalid header_size<%d> returned for header_id<%d> and for %s\n",
                    header_size, header_id, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_OutMsgCxt_T* cxt = ndw_GetOutMsgCxt();
    if (NULL == cxt) {
            NDW_LOGERR( "*** FATAL ERROR: ndw_GetOutMsgCxt() return NULL for header_id <%d> with msg_size <%d> and for%s\n",
                        header_id, msg_size, topic->debug_desc);
            cxt = NULL;

        ndw_exit(EXIT_FAILURE);
    }

    memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));

    cxt->app_id = ndw_app_id;
    cxt->topic = topic;
    cxt->connection = conn;
    cxt->domain = domain;
    cxt->vendor_opaque = (ULONG_T*) NULL;
    cxt->header_id = header_id;
    cxt->header_size = header_size;
    cxt->message_size = msg_size;
    cxt->encoding_format = msg_encoding_format;

    INT_T ret_code = ndw_GetMsgHeaderAndBody(cxt);
    if (ret_code < 0) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_GetMsgHeaderAndBody failed with return code <%d> for header_id <%d> "
                    "with msg_size <%d> and for %s\n", ret_code, header_id, msg_size, topic->debug_desc);
        cxt = NULL;
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == cxt->header_address) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_GetMsgHeaderAndBody returned has invalid header_address <%d> for "
            "header_id <%d> with msg_size <%d> and for %s\n", cxt->header_size, header_id, msg_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (header_id != cxt->header_id) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_GetMsgHeaderAndBody returned has invalid header_id <%d> for "
            "expected header_id <%d> with msg_size <%d> and for %s\n", cxt->header_id, header_size, msg_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (header_size != cxt->header_size) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_GetMsgHeaderAndBody returned has invalid header_size <%d> "
                    "for header_id <%d> with expected header_size <%d> and for %s\n",
                    cxt->header_size, header_id, header_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (msg_size != cxt->message_size) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_GetMsgHeaderAndBody returned has invalid message_size <%d> "
                    "for header_id <%d> with msg_size <%d> and for %s\n",
                    cxt->message_size, header_id, msg_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if ((msg_size > 0) && (NULL == cxt->message_address)) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_GetMsgHeaderAndBody returned has message_address header_id <%d> "
                    "header_size <%d> with msg_size <%d> and for %s\n", header_id, header_size, msg_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (cxt->current_allocation_size < cxt->header_size) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_GetMsgHeaderAndBody returned has invalid current allocation_size <%d> "
                    " for header_id <%d> with msg_size <%d> and for %s\n",
                    cxt->current_allocation_size, header_id, msg_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    INT_T total_size = header_size + msg_size;
    if (cxt->current_allocation_size < total_size) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_GetMsgHeaderAndBody returned has invalid allocation_size <%d> "
                    "while total needed is <%d> for header_id <%d> with msg_size <%d> and for %s\n",
                    cxt->current_allocation_size, total_size, header_id, msg_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if ((msg_size > 0) && (NULL != msg)) {
        memcpy(cxt->message_address, msg, msg_size);
    }

    return cxt;
} // end method ndw_CreateOutMsgCxt

INT_T
ndw_ConvertHeaderToLE(ndw_OutMsgCxt_T* cxt)
{
    if (NULL == cxt) {
        NDW_LOGERR( "*** FATAL ERROR: returned NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ULONG_T* header_address = cxt->header_address;
    INT_T header_size = cxt->header_size;
    UCHAR_T* message_address = cxt->message_address;
    INT_T msg_size = cxt->message_size;

    if (NULL == header_address) {
        NDW_LOGERR( "*** FATAL ERROR: NULL header address!\n");
        return -1;
    }

    const UCHAR_T* addr = (UCHAR_T*) header_address;
    addr += header_size;
    if (addr != message_address) {
        NDW_LOGERR( "*** FATAL ERROR: header_address <0x%lX> + header_size <%d> not equal to message_address <0x%lX>\n",
                ((ULONG_T) header_address), header_size, ((ULONG_T) message_address));
        return -2;
    }

    INT_T total_size = header_size + msg_size;
    if (total_size > cxt->current_allocation_size) {
        NDW_LOGERR( "*** FATAL ERROR: header_address <0x%lX> + header_size <%d> message_address <0x%lX> msg_size <%d> total_size <%d> greater than allocation_size <%d>\n",
                ((ULONG_T) header_address), header_size,
                ((ULONG_T) message_address), msg_size,
                total_size, cxt->current_allocation_size);
        return -3;
    }

    INT_T header_id = cxt->header_id;
    INT_T header_id_in_cxt = (INT_T) *((UCHAR_T*) header_address);
    if (header_id != header_id_in_cxt) {
        NDW_LOGERR( "*** FATAL ERROR: header_address <0x%lX> + header_size <%d> message_address <0x%lX> msg_size <%d> header_id <%d> does NOT match header_id_in_cxt <%d>\n",
                ((ULONG_T) header_address), header_size,
                ((ULONG_T) message_address), msg_size,
                header_id, header_id_in_cxt);
        ndw_exit(EXIT_FAILURE);
    }

    INT_T header_size_in_cxt = (INT_T) *(((UCHAR_T*) header_address) + 1);
    if (header_size != header_size_in_cxt) {
        NDW_LOGERR( "*** FATAL ERROR: header_address <0x%lX> + header_size <%d> message_address <0x%lX> msg_size <%d> header_id <%d> (header_size<%d> does NOT match header_size_in_cxt <%d>)\n",
                ((ULONG_T) header_address), header_size,
                ((ULONG_T) message_address), msg_size,
                header_id, header_size, header_size_in_cxt);
        ndw_exit(EXIT_FAILURE);
    }

    INT_T ret_code = ndw_MsgHeaderImpl[header_id].ConvertToLE((UCHAR_T*) header_address);

    if (0 != ret_code) {
        NDW_LOGERR( "*** ERROR: FAILED to convert Message Header fields into LE format!"
                    " header_address <0x%lX> header_size <%d> "
                    " message_address <0x%lX> msg_size <%d> "
                    " header_id <%d>\n",
                        ((ULONG_T) header_address), header_size,
                        ((ULONG_T) message_address), msg_size,
                        header_id);
    }

    return ret_code;
} // end method ndw_ConvertHeaderToLE

// Send message to a Topic (Subject).
// It will use the ndw_OutMsgCxt_T* built by invoking ndw_CreateOutMsgCxt(...) method.
// Returns 0 on success.
INT_T
ndw_PublishMsg()
{
    ndw_OutMsgCxt_T* cxt = ndw_GetOutMsgCxt();
    if (NULL == cxt) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_GetOutMsgCxt(): returned NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Topic_T* t = cxt->topic;
    if (NULL == t) {
        NDW_LOGERR( "*** WARNING: Topic NOT set!");
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -1; // Something is wrong. Maybe event ndw_CreateOutMsgCxt(...) was not invoked?!
    }

    if (t->disabled)
        return 0;

    if (! t->is_pub_enabled) {
        NDW_LOGERR("*** WARNING: Topic is not enabled for Publishing! %s\n", t->debug_desc);
        return -2;
    }

    ULONG_T* header_address = cxt->header_address;
    if (NULL == header_address) {
        NDW_LOGERR( "*** WARNING: %s header_address not set in cxt!\n", t->debug_desc);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -2; // Something is wrong. Maybe event ndw_CreateOutMsgCxt(...) was not invoked?!
    }

    ndw_Connection_T* conn = t->connection;
    if (NULL == conn) {
        NDW_LOGERR( "*** FATAL: Connection POINTER not set for header_address not set in cxt! For %s\n", t->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    // Build the header fields.
    INT_T header_id = cxt->header_id;
    INT_T header_size = cxt->header_size;
    if ((header_id <= 0) || (header_id > NDW_MAX_HEADER_TYPES)) {
        NDW_LOGERR( "*** ERROR: Invalid header_id<%d> with header_size<%d> in cxt! For %s\n",
                   header_id, header_size, t->debug_desc);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -3;
    }

    INT_T ret_set_fields = ndw_MsgHeaderImpl[header_id].SetOutMsgFields(cxt);
    if (0 != ret_set_fields) {
        NDW_LOGERR( "*** ERROR: FAILED to set fields for header_id<%d> with header_size<%d> with ret_set_fields<%d> For %s\n",
                    header_id, header_size, ret_set_fields, t->debug_desc);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -4;
    }

    // Convert outgoing message to LE format.
    INT_T conversion_code = ndw_ConvertHeaderToLE(cxt);
    if (0 != conversion_code) {
        NDW_LOGERR("*** ERROR ndw_ConvertHeaderToLE() returned conversion_code<%d> for %s\n", conversion_code, t->debug_desc);
        NDW_LOGTOPICERRMSG("*** ERROR: Could not convert outgoing message header fields to LE format!\n", t);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -5;
    }

    INT_T vendor_id = conn->vendor_id;
    ndw_ImplAPI_T* impl = &ndw_impl_api_structure[vendor_id];

    INT_T ret_code = impl->PublishMsg();
    if (ret_code < 0) {
        NDW_LOGERR( "*** ERROR: FAILED to publish message with ret_code<%d> For %s\n", ret_code, t->debug_desc);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -6;
    }
        
    // NOTE: Make sure to zero out message cxt for next message processing!
    memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));

    return ret_code;

} // ndw_PublishMsg()

INT_T
ndw_SubscribeAsyncToTopicNames(CHAR_T** topic_names)
{
    INT_T failure_count = 0;
    CHAR_T** topic_name = topic_names;
    while (NULL != *topic_name) {
        ndw_Topic_T* t = ndw_GetTopicFromFullPath(*topic_name);
        if (NULL == t) {
            NDW_LOGERR("*** ERROR: Cannot get Topic from Full Path Name <%s>\n", *topic_name);
            ++failure_count;
        }
        else {
            if ((! t->disabled) && (t->is_sub_enabled)) {
                INT_T ret_code = ndw_SubscribeAsync(t);
                if (0 != ret_code) {
                    NDW_LOGERR("*** ERROR: FAILED to subscribe to %s\n", t->debug_desc);
                    ++failure_count;
                }
            }
        }

        ++topic_name;
    }

    return failure_count;
} // end method ndw_SubscribeAsyncToTopicNames

INT_T
ndw_SubscribeAsync(ndw_Topic_T* topic)
{
    if (NULL == topic) {
        NDW_LOGERR("ndw_Topic_T* topic parameter is NULL!\n");
        return -1;
    }

    if (topic->disabled)
        return 0;

    if (! topic->is_sub_enabled) {
        NDW_LOGERR("Topic is NOT enabled for Subscription! %s\n", topic->debug_desc);
        return -2;
    }

    ndw_Connection_T* connection = topic->connection;
    if (NULL == connection) {
        NDW_LOGERR("*** FATAL: Connection POINTER not set for %s NOTE: header_address not set in cxt!\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    INT_T vendor_id = connection->vendor_id;
    ndw_ImplAPI_T* impl = &ndw_impl_api_structure[vendor_id];

    INT_T ret_code = impl->SubscribeAsync(topic);
    if (ret_code < 0) {
        NDW_LOGTOPICERRMSG("*** ERROR: FAILED to Subscribe to Topic!", topic);
        return ret_code;
    }
    
    if (ndw_verbose > 0) {
        NDW_LOGTOPICMSG("*** NOTE: Subscribed to Topic!", topic);
    }

    return 0;

} // end method ndw_SubscribeToTopic

INT_T
ndw_Unsubscribe(ndw_Topic_T* topic)
{
    if (NULL == topic) {
        NDW_LOGERR("ndw_Topic_T* topic parameter is NULL!\n");
        return -1;
    }

    if (topic->disabled || (! topic->is_sub_enabled))
        return 0;

    ndw_Connection_T* connection = topic->connection;
    if (NULL == connection) {
        NDW_LOGERR("*** FATAL: Connection POINTER not set for %s NOTE: header_address not set in cxt!\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    INT_T vendor_id = connection->vendor_id;
    ndw_ImplAPI_T* impl = &ndw_impl_api_structure[vendor_id];

    INT_T ret_code = impl->SubscribeAsync(topic);
    if (ret_code < 0) {
        NDW_LOGTOPICERRMSG("*** ERROR: FAILED to Unsubscribe from Topic!", topic);
        return ret_code;
    }
    
    if (ndw_verbose > 0) {
        NDW_LOGTOPICMSG("*** NOTE: Unsubscribed from Topic!", topic);
    }

    return 0;

} // end method ndw_UnsubscribeToTopic

// Send message to NATS INBOX (Subject).
// It will use the ndw_OutMsgCxt_T* built by invoking ndw_CreateOutMsgCxt(...) method.
// Returns 0 on success.
INT_T
ndw_Publish_ResponseForRequestMsg(ndw_Topic_T* topic)
{
    if (NULL == topic) {
        NDW_LOGERR("*** FATAL ERROR: NULL ndw_Topic_T Pointer!\n");
    }

    void* vendor_closure = topic->last_msg_vendor_closure;
    if (NULL == vendor_closure) {
        NDW_LOGERR("*** FATAL ERROR: There is no vendor_closure Pointer in for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == topic->last_msg_header_received) {
        NDW_LOGERR("*** FATAL ERROR: There is no last_msg_header_received Pointer in for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == topic->last_msg_received) {
        NDW_LOGERR("*** FATAL ERROR: There is no last_msg_received Pointer in for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (topic->last_msg_received_size <= 0) {
        NDW_LOGERR("*** FATAL ERROR: Invalid last_msg_received_size<%d> for %s\n",
                     topic->last_msg_received_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }


    ndw_OutMsgCxt_T* cxt = ndw_GetOutMsgCxt();

    if (NULL == cxt) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_GetOutMsgCxt(): returned NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Topic_T* t = cxt->topic;
    if (NULL == t) {
        NDW_LOGERR( "*** WARNING: Topic NOT set!");
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -1; // Something is wrong. Maybe event ndw_CreateOutMsgCxt(...) was not invoked?!
    }

    if (t->disabled)
        return 0;

    if (! t->is_pub_enabled) {
        NDW_LOGERR("*** WARNING: Topic is not enabled for Publishing! %s\n", t->debug_desc);
        return -2;
    }

    ULONG_T* header_address = cxt->header_address;
    if (NULL == header_address) {
        NDW_LOGERR( "*** WARNING: %s header_address not set in cxt!\n", t->debug_desc);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -2; // Something is wrong. Maybe event ndw_CreateOutMsgCxt(...) was not invoked?!
    }

    ndw_Connection_T* conn = t->connection;
    if (NULL == conn) {
        NDW_LOGERR( "*** FATAL: Connection POINTER not set for header_address not set in cxt! For %s\n", t->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    // Build the header fields.
    INT_T header_id = cxt->header_id;
    INT_T header_size = cxt->header_size;
    if ((header_id <= 0) || (header_id > NDW_MAX_HEADER_TYPES)) {
        NDW_LOGERR( "*** ERROR: Invalid header_id<%d> with header_size<%d> in cxt! For %s\n",
                   header_id, header_size, t->debug_desc);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -3;
    }

    INT_T ret_set_fields = ndw_MsgHeaderImpl[header_id].SetOutMsgFields(cxt);
    if (0 != ret_set_fields) {
        NDW_LOGERR( "*** ERROR: FAILED to set fields for header_id<%d> with header_size<%d> with ret_set_fields<%d> For %s\n",
                    header_id, header_size, ret_set_fields, t->debug_desc);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -4;
    }

    // Convert outgoing message to LE format.
    INT_T conversion_code = ndw_ConvertHeaderToLE(cxt);
    if (0 != conversion_code) {
        NDW_LOGERR("*** ERROR ndw_ConvertHeaderToLE() returned conversion_code<%d> for %s\n", conversion_code, t->debug_desc);
        NDW_LOGTOPICERRMSG("*** ERROR: Could not convert outgoing message header fields to LE format!\n", t);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -5;
    }

    INT_T vendor_id = conn->vendor_id;
    ndw_ImplAPI_T* impl = &ndw_impl_api_structure[vendor_id];

    INT_T ret_code = impl->Publish_ResponseForRequestMsg(topic);
    if (0 != ret_code) {
        NDW_LOGERR("*** ERROR: Vendor Implementation could NOT handle Response For Request of Last Message for %s\n",
                    topic->debug_desc);
    }

    return ret_code;

} // end ndw_Publish_ResponseForRequestMsg()

INT_T
ndw_GetResponseForRequestMsg(ndw_Topic_T* topic, LONG_T timeout_ms)
{
    ndw_OutMsgCxt_T* cxt = ndw_GetOutMsgCxt();
    if (NULL == cxt) {
        NDW_LOGERR( "*** FATAL ERROR: ndw_GetRsponseForRequestMsg(): returned NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Topic_T* t = cxt->topic;
    if (NULL == t) {
        NDW_LOGERR( "*** WARNING: Topic NOT set!");
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -1; // Something is wrong. Maybe event ndw_CreateOutMsgCxt(...) was not invoked?!
    }

    if (t->disabled)
        return 0;

    if (! t->is_pub_enabled) {
        NDW_LOGERR("*** WARNING: Topic is not enabled for Publishing! %s\n", t->debug_desc);
        return -2;
    }

    ULONG_T* header_address = cxt->header_address;
    if (NULL == header_address) {
        NDW_LOGERR( "*** WARNING: %s header_address not set in cxt!\n", t->debug_desc);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -2; // Something is wrong. Maybe event ndw_CreateOutMsgCxt(...) was not invoked?!
    }

    ndw_Connection_T* connection = t->connection;
    if (NULL == connection) {
        NDW_LOGERR( "*** FATAL: Connection POINTER not set for header_address not set in cxt! For %s\n", t->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    // Build the header fields.
    INT_T header_id = cxt->header_id;
    INT_T header_size = cxt->header_size;
    if ((header_id <= 0) || (header_id > NDW_MAX_HEADER_TYPES)) {
        NDW_LOGERR( "*** ERROR: Invalid header_id<%d> with header_size<%d> in cxt! For %s\n",
                   header_id, header_size, t->debug_desc);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -3;
    }

    INT_T ret_set_fields = ndw_MsgHeaderImpl[header_id].SetOutMsgFields(cxt);
    if (0 != ret_set_fields) {
        NDW_LOGERR( "*** ERROR: FAILED to set fields for header_id<%d> with header_size<%d> with ret_set_fields<%d> For %s\n",
                    header_id, header_size, ret_set_fields, t->debug_desc);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -4;
    }

    // Convert outgoing message to LE format.
    INT_T conversion_code = ndw_ConvertHeaderToLE(cxt);
    if (0 != conversion_code) {
        NDW_LOGERR("*** ERROR ndw_ConvertHeaderToLE() returned conversion_code<%d> for %s\n", conversion_code, t->debug_desc);
        NDW_LOGTOPICERRMSG("*** ERROR: Could not convert outgoing message header fields to LE format!\n", t);
        memset(cxt, 0, sizeof(ndw_OutMsgCxt_T));
        return -5;
    }

    INT_T impl_id = connection->vendor_id;
    if ((impl_id < 1) || (impl_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
        NDW_LOGERR( "*** FATAL ERROR: Invalid connection vendor_id <%d> for %s\n", impl_id, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_ImplAPI_T *impl = &ndw_impl_api_structure[impl_id];

    CHAR_T* msg = NULL;
    INT_T msg_length = 0;
    void* vendor_closure = NULL;

    INT_T ret_code = impl->GetResponseForRequestMsg(
                    topic, (const CHAR_T**) &msg, &msg_length, timeout_ms, &vendor_closure);

    if (ret_code < 0) {
        NDW_LOGERR("impl->GetResponseForRequestMsg returned error code <%d> impl_id<%d> for %s\n",
                    ret_code, impl_id, topic->debug_desc);
        return ret_code;
    }

    if (0 == ret_code) {
        // This is a timeout case.
        // We do NOT expect a vendor_closure in such a case.
        if (NULL != vendor_closure) {
            NDW_LOGERR("*** FATAL ERROR: While polling for message it timed out, but incorrectly getting "
                        "a vendor_closure object for %s\n", topic->debug_desc);
            ndw_exit(EXIT_FAILURE);
        }

        return 0;
    }

    if ((NULL == msg) || (msg_length <= 0)) {
        return 0;
    }

    NDW_LOGX("<<< Success [%s] GETRESPFORREQ MESSAGE: msg<%s> msg_length<%d>"
             " for %s\n", topic->topic_unique_name, msg, msg_length, topic->debug_desc);

    return 1;
}

void
ndw_PrintStatsForTopic(ndw_Topic_T* t)
{
    if (NULL == t)
        return;

    const CHAR_T* spaces = "    ";
    NDW_LOG("%s--> BEGIN: Statistics for %s\n", spaces, t->debug_desc);
    NDW_LOG("%s  * sequence_number<%ld> total_published_msgs<%ld> total_received_msgs<%ld> "
                "total_bad_msgs_received <%ld>\n", spaces,
                t->sequence_number, t->total_published_msgs, t->total_received_msgs, t->total_bad_msgs_received);
    NDW_LOG("%s--> END: Statistics for %s\n", spaces, t->debug_desc);
} // end method ndw_PrintStatsForTopic

void
ndw_PrintStatsForConnection(ndw_Connection_T* c)
{
    if (NULL == c)
        return;

    const CHAR_T* spaces = "   ";
    NDW_LOG("%s--> BEGIN: Statistics for %s\n", spaces, c->debug_desc);
    INT_T total_topics = 0;
    ndw_Topic_T** topics = ndw_GetAllTopicsFromConnection(c, &total_topics);
    if ((NULL == topics) || (total_topics <=0)) {
        NDW_LOG("NOTE: There are NO Topics<%d>\n", total_topics);
        free(topics);
        return;
    }

    for (INT_T k = 0; k < total_topics; k++) {
        ndw_PrintStatsForTopic(topics[k]);
    }

    free(topics);

    NDW_LOG("%s--> END: Statistics for %s\n", spaces, c->debug_desc);
} // end method ndw_PrintStatsForConnection

void
ndw_PrintStatsForDomain(ndw_Domain_T* d)
{
    if (NULL == d)
        return;

    NDW_LOG(" ---> BEGIN: Statistics for %s\n", d->debug_desc);

    INT_T total_connections = 0;
    ndw_Connection_T** connections =
    ndw_GetAllConnectionsFromDomain(d, &total_connections);
    if ((NULL == connections) || (total_connections <=0)) {
        NDW_LOG("NOTE: There are NO Connections<%d>\n", total_connections);
        free(connections);
        return;
    }

    for (INT_T j = 0; j < total_connections; j++) {
        ndw_Connection_T* c = connections[j];
        if (NULL == c)
            continue;

        ndw_PrintStatsForConnection(c);
    } // End loop Connections

    free(connections);

    NDW_LOG(" ---> END: Statistics for %s\n\n", d->debug_desc);

} // end method ndw_PrintStatsForDomain

void
ndw_PrintStatsForDomainByName(const CHAR_T* domain)
{
    if (NDW_ISNULLCHARPTR(domain))
        return;

    ndw_Domain_T* d = ndw_GetDomainByName(domain);
    if (NULL == domain) {
        NDW_LOGERR("*** WARNING: Cannot find Domain<%s>\n", domain);
        return;
    }

    ndw_PrintStatsForDomain(d);
} // end method ndw_PrintStatsForDomainByName

void
ndw_PrintStatsForDomains(const CHAR_T** domains)
{
    if (ndw_verbose <= 0)
        return;

    if ((NULL == domains) || (NULL == *domains))
        return;

    NDW_LOG("\n============ BEGIN: ndw_printStatsForDomain ================\n");

    for (const CHAR_T** domain = domains; NULL != *domain; domain++)
        ndw_PrintStatsForDomainByName(*domain);

    NDW_LOG("\n============ END: ndw_printStatsForDomain ================\n\n");
} // end method ndw_PrintStatsForDomains

INT_T
ndw_PollAsyncQueue(ndw_Topic_T* topic, LONG_T timeout_us)
{
    if (NULL == topic) {
        NDW_LOGERR("NULL Topic!\n");
        return -1;
    }

    if (! topic->q_async_enabled) {
        NDW_LOGERR("Topic NOT enabled for asynchronous message queuing for %s\n", topic->debug_desc);
        return -2;
    }

    if (NULL == topic->q_async) {
        NDW_LOGERR("*** FATAL ERROR: q_async Pointer is NULL for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    NDW_QData_T q_data;
    memset(&q_data, 0, sizeof(NDW_QData_T));

    int ret_code = ndw_QGet(topic->q_async, &q_data, timeout_us);
    if (0 == ret_code) {
        return 0;
    }

    if (1 != ret_code) {
        NDW_LOGERR("ndw_QGet returned unrecognized return code<%d> for %s\n", ret_code, topic->debug_desc);
        return 0; // XXX: Review return code. Probably better to let user know.
    }

    if (ndw_verbose > 3) {
        NDW_LOGX("Received Message from q_async for %s\n", topic->debug_desc);
    }

    ndw_QAsync_Item_T* q_item = (ndw_QAsync_Item_T*) q_data.data;
    if (NULL == q_item) {
        NDW_LOGERR("*** FATAL ERROR: ndw_QAsync_Item* is NULL for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    topic->last_msg_header_received = NULL;
    topic->last_msg_received = NULL;

    ndw_InMsgCxt_T* msginfo = ndw_LE_to_MsgHeader(q_item->msg, q_item->msg_size);
    if (NULL == msginfo) {
        NDW_LOGERR("*** FATAL ERROR:  ndw_MsgHeader_Info_T* returned is NULL! msg_size<%d>. For<%s>\n",
                    q_item->msg_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    topic->last_msg_received_time = ndw_GetCurrentUTCNanoseconds();
    topic->last_msg_header_received = msginfo->header_addr;
    topic->last_msg_received = msginfo->msg_addr;
    topic->last_msg_received_size = msginfo->msg_size;
    topic->total_received_msgs += 1;

    topic->q_async_closure = q_item;

    return 1;
} // end method ndw_PollAsyncQueue

INT_T
ndw_CommitAsyncQueuedMessge(ndw_Topic_T* topic)
{
    ndw_QAsync_Item_T* q_item = (ndw_QAsync_Item_T*) topic->q_async_closure;
    if (NULL == q_item) {
        NDW_LOGERR("q_async_closure is NULL for %s\n", topic->debug_desc);
        return -1;
    }

    if (NULL == q_item->topic) {
        NDW_LOGERR("ndw_QAsync_Item_T* has topic pointer as NULL for %s\n", topic->debug_desc);
        return -2;
    }

    if (q_item->topic != topic) {
        NDW_LOGERR("ndw_QAsync_Item_T* has DIFFERENT Topic pointer <%s> EXPECTED Topic Pointer %s\n",
                    q_item->topic->debug_desc, topic->debug_desc);
        return -3;
    }

    if (NULL == topic->q_async) {
        NDW_LOGERR("*** FATAL ERROR: q_async is NULL for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    INT_T impl_id = topic->connection->vendor_id;
    if ((impl_id < 1) || (impl_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
        NDW_LOGERR( "*** FATAL ERROR: Invalid connection vendor_id <%d> for %s\n", impl_id, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_ImplAPI_T *impl = &ndw_impl_api_structure[impl_id];
    INT_T vendor_ret_code = impl->CommitQueuedMsg(topic, q_item->vendor_closure);
    if (0 != vendor_ret_code) {
        NDW_LOGERR("*** ERROR: CommitQueuedMsg return code<%d> for %s\n", vendor_ret_code, topic->debug_desc);
#if 0
        // XXX: Not sure if we should exit since there is not much we can do if vendor coud not commit!
        exit(EXIT_FAILURE); 
#endif
    }

    ndw_QDeleteCurrent(topic->q_async);

#if 0
    // Note: q_item is deleted in the closure operator!
    free(q_item);
#endif

    topic->q_async_closure = NULL;

    return 0;
} // end method ndw_CommitAsyncQueuedMessge(ndw_Topic_T* topic)


//
// Function Scope # 2: Vendor Implementations to invoke these following functions.
//
INT_T
ndw_HandleVendorAsyncMessage(ndw_Topic_T* topic, UCHAR_T* msg, INT_T msg_size, void* vendor_closure)
{
    if (NULL == topic) {
        NDW_LOGERR("*** FATAL ERROR: Parameter ndw_Topic_T* is NULL! (msg_size : <%d>)\n", msg_size);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Connection_T* connection = topic->connection;
    if (NULL == connection) {
        NDW_LOGERR("*** FATAL ERROR: ndw_Connection_T* is null for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (topic->q_async_enabled) {
        if (NULL == topic->q_async) {
            NDW_LOGERR("*** FATAL ERROR: Topic has q_async_enabled but q_async object is NULL for %s\n", topic->debug_desc);
            ndw_exit(EXIT_FAILURE);
        }

        if (NULL == vendor_closure) {
            NDW_LOGERR("*** FATAL ERROR: Topic has q_async_enabled but vendor_closure is NULL!%s\n", topic->debug_desc);
            ndw_exit(EXIT_FAILURE);
        }

        ndw_QAsync_Item_T* q_item = calloc(1, sizeof(ndw_QAsync_Item_T));
        q_item->topic = topic;
        q_item->vendor_closure = vendor_closure;
        q_item->msg = msg;
        q_item->msg_size = msg_size;

        if (0 != ndw_QInsert(topic->q_async, q_item)) {
            NDW_LOGERR("*** FATAL ERROR: ndw_QInsert failed for %s\n", topic->debug_desc);
            ndw_exit(EXIT_FAILURE);
        }

        return 0;
    }

    if (NULL != topic->last_msg_header_received) {
        NDW_LOGERR("*** FATAL ERROR: Asynchronous message arrived but last_msg_header_received is NOT NULL for %s\n",
                    topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL != topic->last_msg_received) {
        NDW_LOGERR("*** FATAL ERROR: Asynchronous message arrived but last_msg_received is NOT NULL for %s\n",
                    topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (0 != topic->last_msg_received_size) {
        NDW_LOGERR("*** FATAL ERROR: Asynchronous message arrived but last_msg_received_size<%d> is NOT zero for %s\n",
                    topic->last_msg_received_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    topic->last_msg_header_received = NULL;
    topic->last_msg_received = NULL;

    ndw_InMsgCxt_T* msginfo = ndw_LE_to_MsgHeader(msg, msg_size);
    if (NULL == msginfo) {
        NDW_LOGERR("*** FATAL ERROR:  ndw_MsgHeader_Info_T* returned is NULL! msg_size<%d>. For<%s>\n",
                    msg_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    topic->last_msg_received_time = ndw_GetCurrentUTCNanoseconds();
    topic->last_msg_header_received = msginfo->header_addr;
    topic->last_msg_received = msginfo->msg_addr;
    topic->last_msg_received_size = msginfo->msg_size;
    topic->total_received_msgs += 1;

    INT_T ret_code = -1;
    if (msginfo->is_bad) {
        topic->total_bad_msgs_received += 1;

        ndw_BadMessage_T bad_msg;
        bad_msg.received_time = topic->last_msg_received_time;
        bad_msg.header_id = msginfo->header_id;
        bad_msg.header_size = msginfo->header_size;
        bad_msg.msg_addr = msginfo->msg_addr;
        bad_msg.msg_size = msginfo->msg_size;
        bad_msg.topic = topic;
        bad_msg.connection = connection;
        bad_msg.domain = connection->domain;
        bad_msg.error_msg = msginfo->error_msg;

        ndw_bad_message_callback_ptr(&bad_msg);
    }
    else if (NULL != ndw_go_msghandler) {
        topic->last_msg_vendor_closure = vendor_closure;
        ndw_go_msghandler(topic);
    }
    else if (NULL != ndw_async_callback_ptr) {
        topic->last_msg_vendor_closure = vendor_closure;
        ndw_async_callback_ptr(topic, NULL);
        ret_code = 0;
    }
    else {
        NDW_LOGERR("NOTE: No callback available on receiving message on %s\n", topic->debug_desc);
        ret_code = 0;
    }

    return ret_code;
} // end method ndw_HandleVendorAsyncMessage


/*
 * Publish a diagnostic message for a Topic interaction.
 * Topic, Connection or Domain can be null.
 * If Topic is specified then it gets Connection and Domain, when Connection and Domain is NULL.
 */
void
ndw_LogTopicMsg(const CHAR_T* filename, INT_T line_number, const CHAR_T* function_name,
                    FILE* stream, const CHAR_T* msg, ndw_Topic_T* t)
{
    if (NULL == stream)
        stream = ndw_out_file;

    if (NULL == t)
        NDW_LOG("ndw_Topic_T*parameter is NULL!\n");
    else
        ndw_print(filename, line_number, function_name, stream, "%s {%s}\n", ((NULL == msg) ? "?" : msg), t->debug_desc);
} // end method ndw_LogTopicMsg

INT_T
ndw_GetQueuedMsgCount(ndw_Topic_T* topic, ULONG_T* count)
{
    if (NULL == topic)
        return -1;

    ndw_Connection_T* connection = topic->connection;
    if (topic->disabled)
        return 0;

    if (NULL == connection)
        return -2;

    INT_T impl_id = connection->vendor_id;
    if ((impl_id < 1) || (impl_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
        NDW_LOGERR( "*** FATAL ERROR: Invalid connection vendor_id <%d> for %s\n", impl_id, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }
    
    ndw_ImplAPI_T *impl = &ndw_impl_api_structure[impl_id];
    return impl->GetQueuedMsgCount(topic, count);
} // end method ndw_NATS_GetQueuedMsgCount

INT_T
ndw_SubscribeSynchronouslyWithTopicName(const CHAR_T* topic_name)
{
    return ndw_SubscribeSynchronous(ndw_GetTopicFromFullPath(topic_name));
} // end method ndw_SubscribeSynchronous


INT_T
ndw_SubscribeSynchronous(ndw_Topic_T* topic)
{
    if (NULL == topic)
        return -1;

    if (topic->disabled)
        return 0;

    ndw_Connection_T* connection = topic->connection;
    if (NULL == connection)
        return -2;

    INT_T impl_id = connection->vendor_id;
    if ((impl_id < 1) || (impl_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
        NDW_LOGERR("*** FATAL ERROR: Invalid connection vendor_id <%d> for %s\n", impl_id, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_ImplAPI_T *impl = &ndw_impl_api_structure[impl_id];
    return impl->SubscribeSynchronously(topic);
} // end method ndw_SubscribeSynchronous

INT_T
ndw_SynchronousPollForMsg(ndw_Topic_T* topic, LONG_T timeout_ms, LONG_T* dropped_messages)
{
    if (topic->disabled)
        return 0;

    if (! topic->is_sub_enabled) {
        NDW_LOGERR("Topic NOT enable for Subscription and Polling! %s\n", topic->debug_desc);
    }

    ndw_LastSyncPollMsg_T* last_sync_poll_msg =
        (ndw_LastSyncPollMsg_T*) pthread_getspecific(ndw_tls_lastsyncpollmsg);
    if (NULL == last_sync_poll_msg) {
        NDW_LOGERR("*** FATAL ERROR: pthread_getspecific(ndw_tls_lastsyncpollmsg) returned NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    memset(last_sync_poll_msg, 0,sizeof(ndw_LastSyncPollMsg_T));

    if (NULL == topic)
        return -1;

    ndw_Connection_T* connection = topic->connection;
    if (NULL == connection)
        return -2;

    if (NULL != topic->last_msg_header_received) {
        NDW_LOGERR("*** FATAL ERROR: Topic last_msg_header_received is NOT NULL for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL != topic->last_msg_received) {
        NDW_LOGERR("*** FATAL ERROR: Topic last_msg_received is NOT NULL for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (0 != topic->last_msg_received_size) {
        NDW_LOGERR("*** FATAL ERROR: Topic last_msg_received_size<%d> is NOT zero for %s\n",
                topic->last_msg_received_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    INT_T impl_id = connection->vendor_id;
    if ((impl_id < 1) || (impl_id >= NDW_MAX_API_IMPLEMENTATIONS)) {
        NDW_LOGERR( "*** FATAL ERROR: Invalid connection vendor_id <%d> for %s\n", impl_id, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_ImplAPI_T *impl = &ndw_impl_api_structure[impl_id];

    CHAR_T* msg = NULL;
    INT_T msg_length = 0;
    void* vendor_closure = NULL;

    INT_T ret_code = impl->SynchronousPollForMsg(
                    topic, (const CHAR_T**) &msg, &msg_length, timeout_ms, dropped_messages, &vendor_closure);

    if (ret_code < 0) {
        NDW_LOGERR("impl->ndw_SynchronousPollForMsg returned error code <%d> impl_id<%d> for %s\n",
                    ret_code, impl_id, topic->debug_desc);
        return ret_code;
    }

    if (0 == ret_code) {
        // This is a timeout case.
        // We do NOT expect a vendor_closure in such a case.
        if (NULL != vendor_closure) {
            NDW_LOGERR("*** FATAL ERROR: While polling for message it timed out, but incorrectly getting "
                        "a vendor_closure object for %s\n", topic->debug_desc);
        }

        return 0;
    }

    if ((NULL == msg) || (msg_length <= 0)) {
        return 0;
    }

    ndw_InMsgCxt_T* msginfo = ndw_LE_to_MsgHeader((UCHAR_T*) msg, msg_length);
    if (NULL == msginfo) {
        NDW_LOGERR( "*** FATAL ERROR:  ndw_MsgHeader_Info_T* returned is NULL! (msg_size : <%d>)\n", msg_length);
        ndw_exit(EXIT_FAILURE);
    }

    last_sync_poll_msg->topic = topic;
    last_sync_poll_msg->msg = msg;
    last_sync_poll_msg->msg_size = msg_length;
    last_sync_poll_msg->vendor_closure = vendor_closure;

    topic->last_msg_received_time = ndw_GetCurrentUTCNanoseconds();
    topic->last_msg_header_received = msginfo->header_addr;
    topic->last_msg_received = msginfo->msg_addr;
    topic->last_msg_received_size = msginfo->msg_size;
    topic->total_received_msgs += 1;
    topic->last_msg_vendor_closure = vendor_closure;

    if (msginfo->is_bad) {
        topic->total_bad_msgs_received += 1;

        ndw_BadMessage_T bad_msg;
        bad_msg.received_time = topic->last_msg_received_time;
        bad_msg.header_id = msginfo->header_id;
        bad_msg.header_size = msginfo->header_size;
        bad_msg.msg_addr = msginfo->msg_addr;
        bad_msg.msg_size = msginfo->msg_size;
        bad_msg.topic = topic;
        bad_msg.connection = connection;
        bad_msg.domain = connection->domain;
        bad_msg.error_msg = msginfo->error_msg;

        ndw_bad_message_callback_ptr(&bad_msg);

        return -3;
    }
    else {
        return ret_code;
    }

} // end method ndw_SynchronousPollForMsg

INT_T
ndw_CommitLastMsg(ndw_Topic_T* topic)
{
    if (NULL == topic) {
        NDW_LOGERR("*** FATAL ERROR: NULL ndw_Topic_T Pointer!\n");
    }

    void* vendor_closure = topic->last_msg_vendor_closure;
    if (NULL == vendor_closure) {
        NDW_LOGERR("*** FATAL ERROR: There is no vendor_closure Pointer in for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == topic->last_msg_header_received) {
        NDW_LOGERR("*** FATAL ERROR: There is no last_msg_header_received Pointer in for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == topic->last_msg_received) {
        NDW_LOGERR("*** FATAL ERROR: There is no last_msg_received Pointer in for %s\n", topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    if (topic->last_msg_received_size <= 0) {
        NDW_LOGERR("*** FATAL ERROR: Invalid last_msg_received_size<%d> for %s\n",
                     topic->last_msg_received_size, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_Connection_T* connection = topic->connection;
    if (NULL == connection) {
        NDW_LOGERR("*** FATAL ERROR: ndw_Connection_T Pointer is NULL in Topic Structure for %s\n",
                    topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_ImplAPI_T *impl = &ndw_impl_api_structure[connection->vendor_id];
    if (NULL == impl) {
        NDW_LOGERR("*** FATAL ERROR: impl->CommigLstMsg() function pointer is NULL for "
                    "Vendor ID: <%d> and for %s\n", connection->vendor_id, topic->debug_desc);
        ndw_exit(EXIT_FAILURE);
    }

    INT_T ret_code = impl->CommitLastMsg(topic, vendor_closure);
    if (0 != ret_code) {
        NDW_LOGERR("*** ERROR: Vendor Implementation could NOT handle Commit of Last Message for %s\n",
                    topic->debug_desc);
    }

    topic->last_msg_header_received = NULL;
    topic->last_msg_received = NULL;
    topic->last_msg_received_size = 0;
    topic->last_msg_vendor_closure = NULL;

    return ret_code;

} // end method ndw_CommitLastMsg


void
ndw_SetGoMessageHandler(ndw_GOAsyncMsgHandler handler)
{
    NDW_LOGX("NOTE: Adding GO Message Handler\n");
    ndw_go_msghandler = handler;
}

#if 0
// This is the non-inline function cgo can link to
ndw_GOAsyncMsgHandler exportedGetGoHandler(void) {
    NDW_LOGX("NOTE: Returning GO Message Handler\n");
    return getGoHandler();
}
#endif

