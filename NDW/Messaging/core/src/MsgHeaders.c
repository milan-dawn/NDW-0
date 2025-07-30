
#include "MsgHeaders.h"
#include "MsgHeader_1.h"

size_t ndw_max_message_size = NDW_MAX_MESSAGE_SIZE;

void ndw_TLSDestructor_MsgHeader_OutMsgCxt(void* ptr)
{
    NDW_LOGX("TLS Destructor: OutMsgCxt: ThreadID<%lu>\n", pthread_self());
#if 1
    if (NULL != ptr) {
        free(ptr);
    }
#endif
}

void ndw_TLSDestructor_MsgHeader_ReceivedMsgHeader(void* ptr)
{
    NDW_LOGX("TLS Destructor: ReceivedMsgHeader ThreadID<%lu>\n", pthread_self());
#if 1
    if (NULL != ptr) {
        free(ptr);
    }
#endif
}

void ndw_TLSDestructor_MsgHeader_MsgHeaderInfo(void* ptr)
{
    NDW_LOGX("TLS Destructor: MsgHeaderInfo ThreadID<%lu>\n", pthread_self());
#if 1
    if (NULL != ptr) {
        free(ptr);
    }
#endif
}

void ndw_TLSDestructor_header_and_message(void* ptr)
{
    NDW_LOGX("TLS Destructor: ndw_TLSDestructor_header_and_message: ThreadID<%lu>\n", pthread_self());
#if 1
    ndw_HeaderAndMsg_T* hm = ptr;
    if (NULL != hm) {
        if (NULL != hm->aligned_address) {
            free(hm->aligned_address);
            hm->aligned_address = NULL;
            hm->allocated_size = 0;
        }
        free(hm);
    }
#endif
} // end method ndw_TLSDestructor_header_and_message

pthread_key_t ndw_tls_header_and_message; // Per thread memory for Header and Message.
pthread_once_t ndw_tls_header_and_message_once = PTHREAD_ONCE_INIT; 
static void ndw_tls_header_and_message_Init()
{
    if (0 != pthread_key_create(&ndw_tls_header_and_message, ndw_TLSDestructor_header_and_message))
    {
        NDW_LOGERR("*** FATAL ERROR: Failed to create ndw_tls_OutMsgCxt!\n");
        ndw_exit(EXIT_FAILURE);
    }
}

pthread_key_t ndw_tls_OutMsgCxt; // Per thread memory for Out Message Context
pthread_once_t ndw_tls_OutMsgCx_once = PTHREAD_ONCE_INIT; 
static void ndw_tls_OutMsgCxt_Init()
{
    if (0 != pthread_key_create(&ndw_tls_OutMsgCxt, ndw_TLSDestructor_MsgHeader_OutMsgCxt))
    {
        NDW_LOGERR("*** FATAL ERROR: Failed to create ndw_tls_OutMsgCxt!\n");
        ndw_exit(EXIT_FAILURE);
    }
}

pthread_key_t ndw_tls_received_msg_header; // Per thread received message header.
pthread_once_t ndw_tls_received_msg_header_once = PTHREAD_ONCE_INIT; 
static void ndw_tls_received_msg_header_Init()
{
    if (0 != pthread_key_create(&ndw_tls_received_msg_header, ndw_TLSDestructor_MsgHeader_ReceivedMsgHeader))
    {
        NDW_LOGERR("*** FATAL ERROR: Failed to create &ndw_tls_received_msg_header,!\n");
        ndw_exit(EXIT_FAILURE);
    }
}

pthread_key_t ndw_tls_received_MsgHeaderInfo; // Per thread received message header.
pthread_once_t ndw_tls_received_MsgHeaderInfo_once = PTHREAD_ONCE_INIT; 
static void ndw_tls_received_MsgHeaderInfo_Init()
{
    if (0 != pthread_key_create(&ndw_tls_received_MsgHeaderInfo, ndw_TLSDestructor_MsgHeader_MsgHeaderInfo))
    {
        NDW_LOGERR("*** FATAL ERROR: Failed to create &ndw_tls_received_MsgHeaderInfo,!\n");
        ndw_exit(EXIT_FAILURE);
    }
}

static ndw_ImplMsgHeader_T ndw_MsgHeader_structure[NDW_MAX_HEADER_TYPES + 1];
ndw_ImplMsgHeader_T* ndw_MsgHeaderImpl = &ndw_MsgHeader_structure[0];


static bool ndw_ImplMsgHeader_IsValid(INT_T header_id) {
    NDW_LOGERR("Invalid Msg Header Function Request with header_id<%d>\n", header_id);
    return false;
}

static void ndw_ImplMsgHeader_Print(INT_T header_id, ULONG_T* pHeader) {
    NDW_LOGERR("Invalid Msg Header Function Request with header_id<%d> header_addr<0x%lX>\n",
                header_id, ((ULONG_T) pHeader));
}

static INT_T ndw_ImplMsgHeader_Compare(INT_T header_id, ULONG_T* pHeader1, ULONG_T* pHeader2)
{
    NDW_LOGERR("Invalid Msg Header Function Request with header_id<%d> "
                "pHeader1<0x%lX> pHeader2<0x%lX>\n", header_id,
                    ((ULONG_T) pHeader1), ((ULONG_T) pHeader2));
    return -1;
}

static INT_T ndw_ImplMsgHeader_LE_MsgHeaderSize(INT_T header_id) {
    NDW_LOGERR("Invalid LE MsgHeaderSize Function Request with header_id<%d>\n", header_id);
    return -1;
}

INT_T ndw_ImplMsgHeader_SetOutMsgFields(ndw_OutMsgCxt_T* cxt)
{
    NDW_LOGERR("Invalid Msg Header Function Request with header_id<%d>\n", ((NULL == cxt) ? -1 : cxt->header_id));
    return -1;
}

static INT_T ndw_ImplMsgHeader_ConvertToLE(UCHAR_T* header_address)
{
    NDW_LOGERR("Invalid Msg Header Function Request to ConvertToLE. header_address<0x%lX>\n",
                ((ULONG_T) header_address));
    return -1;
}

static INT_T ndw_ImplMsgHeader_ConvertFromLE(UCHAR_T* src,  UCHAR_T* dest)
{
    NDW_LOGERR("Invalid Msg Header Function Request src<0x%lX> dest<0x%lX>\n",
                    ((ULONG_T) src), ((ULONG_T) dest));
    return -1;
}

void
ndw_print_MessageHeaderInfo(FILE* stream, ndw_InMsgCxt_T* msginfo)
{
    if (NULL == msginfo)
        return;

    if (NULL == stream)
        stream = stdout;

    NDW_LOG(" ndw_MesHeaderInfo_T ==> \n"
                    "   Isbad?<%s> Error<%s> " 
                    "   data_addr<0x%lX>  data_size<%d> \n"
                    "   header_addr<0x%lX> header_id<%d> header_size<%d>\n"
                    "   msg_addr<0x%lX> msg_size<%d> \n",
                    (true == msginfo->is_bad) ? "true" : "false", msginfo->error_msg,
                    ((ULONG_T) msginfo->data_addr), msginfo->data_size, 
                    ((ULONG_T) msginfo->header_addr), msginfo->header_id, msginfo->header_size,
                    ((ULONG_T) msginfo->msg_addr), msginfo->msg_size);

    
} // end method ndw_print_MessageHeaderInfo

void
ndw_DebugMsgHeaders()
{
    NDW_LOG("\n====== END:  Debug Message Header ======\n");
    NDW_LOG("\n");

    INT_T header_and_message_creation_iterations = 10;
    INT_T message_body_increment_size = 100;
    INT_T msg_size = message_body_increment_size;
    INT_T ret_code = -1;
    for (INT_T i = 0; i < header_and_message_creation_iterations; i++, msg_size += message_body_increment_size)
    {
        ndw_OutMsgCxt_T* mha = ndw_GetOutMsgCxt();
        mha->header_size = NDW_MAX_HEADER_SIZE;
        mha->message_size = msg_size;

        ret_code = ndw_GetMsgHeaderAndBody(mha);

        ndw_HeaderAndMsg_T* hm = pthread_getspecific(ndw_tls_header_and_message);
        if (NULL == hm) {
            NDW_LOG("[%d] debug_message_headers(): TLS for HeaderMessage_T* is NULL!\n", i);
            exit(EXIT_FAILURE);
        }
        ULONG_T addrH = (ULONG_T) mha->header_address;
        ULONG_T addrM = (ULONG_T) mha->message_address;
        NDW_LOG("[%d] debug_message_headers(): msg_size %d (TLS allocation size %d) (current allocation size %d) Header Address = 0x%lX modulo 64 = %ld Memory Address = 0x%lX, modulo 64 = %ld\n",
                i, msg_size, hm->allocated_size, mha->current_allocation_size,
                addrH, (addrH % sizeof(ULONG_T)), addrM, (addrM % sizeof(ULONG_T)));

        if (0 != (addrH % sizeof(ULONG_T))) {
            NDW_LOGERR( "*** ERROR: debug_message_headers(): Header is NOT aligned!\n");
            exit(EXIT_FAILURE);
        }

        if (0 != ret_code)
        {
            NDW_LOGERR( "*** ERROR: debug_message_headers(): allocated failed with return code %d on message size request of %d bytes\n", ret_code, msg_size);

        }
    }

} // end method ndw_debug_message_headers

ndw_InMsgCxt_T*
ndw_LE_to_MsgHeader(UCHAR_T* le_msg, INT_T le_msg_size)
{
    ndw_InMsgCxt_T* msginfo = (ndw_InMsgCxt_T*)
                                    pthread_getspecific(ndw_tls_received_MsgHeaderInfo);
    if (NULL == msginfo) {
        CHAR_T* ptr_aligned_MsgInfo = ndw_alloc_align(sizeof(ndw_InMsgCxt_T));
        pthread_setspecific(ndw_tls_received_MsgHeaderInfo, ptr_aligned_MsgInfo);
        msginfo = (ndw_InMsgCxt_T*) pthread_getspecific(ndw_tls_received_MsgHeaderInfo);
        if (NULL == msginfo) {
            NDW_LOGERR( "*** FATAL ERROR: pthread_getspecific(ndw_tls_received_MsgHeaderInfo) returned NULL even after allocating an initial one for the thread!!\n");
            ndw_exit(EXIT_FAILURE);
        }
    }

    memset(msginfo, 0, sizeof(ndw_InMsgCxt_T));

    msginfo->is_bad = true;
    msginfo->error_msg = "";
    msginfo->data_addr = le_msg;
    msginfo->data_size = le_msg_size;
    msginfo->header_addr = pthread_getspecific(ndw_tls_received_msg_header);
    if (NULL == msginfo->header_addr) {
        CHAR_T* ptr_aligned = ndw_alloc_align(NDW_MAX_HEADER_SIZE * 2);
        pthread_setspecific(ndw_tls_received_msg_header, ptr_aligned);
        msginfo->header_addr = pthread_getspecific(ndw_tls_received_msg_header);
        if (NULL == msginfo->header_addr) {
            NDW_LOGERR( "*** FATAL ERROR: (ndw_tls_received_msg_header) returned NULL even after allocating it and settig it into TLS!\n");
            ndw_exit(EXIT_FAILURE);
        }
    }

    memset(msginfo->header_addr, 0, NDW_MAX_HEADER_SIZE);
    
    if (NULL == le_msg) {
        msginfo->error_msg = "Input parameter le_msg is NULL";
        return msginfo;
    }

    if (le_msg_size < NDW_MIN_HEADER_SIZE) {
        NDW_LOGERR("Input parameter le_msg_size is < NDW_MIN_HEADER_SIZE");
        msginfo->error_msg = "Input parameter le_msg_size is < NDW_MIN_HEADER_SIZE";
        return msginfo;
    }

    msginfo->header_id = (INT_T) (*le_msg);
    // NOTE 0 and 255 are NOT valid header identifiers! Do NOT use them!
    if ((msginfo->header_id < 0) || (msginfo->header_id > NDW_MAX_HEADER_TYPES)) {
        msginfo->error_msg = "header_id in le_msg is NOT a valid one";
        return msginfo;
    }

    msginfo->header_size = ((INT_T) (*(le_msg + 1)));
    if ((msginfo->header_size <= 0) || (msginfo->header_size > NDW_MAX_HEADER_SIZE)) {
        NDW_LOGERR("*** ERROR: Invalid header_size<%d> obtained\n", msginfo->header_size);
        msginfo->error_msg = "header_size obtained is 0 or too big";
        return msginfo;
    }

    INT_T expected_header_size = ndw_MsgHeaderImpl[msginfo->header_id].LE_MsgHeaderSize( msginfo->header_id);
    if (expected_header_size != msginfo->header_size) {
        NDW_LOGERR("*** ERROR: header_size mismatch! Got <%d> Expected<%d> for header_id<%d>\n",
                    msginfo->header_size, expected_header_size, msginfo->header_id);
        msginfo->error_msg = "Mismatched header_size\n";
        return msginfo;
    }

    msginfo->msg_addr = le_msg + msginfo->header_size;
    msginfo->msg_size = le_msg_size - msginfo->header_size;

    UCHAR_T* end_addr_check = ((UCHAR_T*) le_msg) + (msginfo->header_size);
    UCHAR_T* end_addr = ((UCHAR_T*) le_msg) + le_msg_size;
    if ((end_addr_check > end_addr) || (msginfo->msg_size < 0)) {
        NDW_LOGERR( "*** ERROR: "
                "data_addr <0x%lX> data_size <%d> end_addr <0x%lX> "
                "header_id <%d> header_size <%d> "
                "puts end_addr_check <0x%lX>  beyond last address of Header!",
                    ((ULONG_T) le_msg), le_msg_size, ((ULONG_T)  end_addr),
                    msginfo->header_id, msginfo->header_size,
                    ((ULONG_T) end_addr_check));
        msginfo->error_msg = "end_address_check: end address if beyond le_msg + le_msg_size";
       return msginfo;
    }

    INT_T ret_code = ndw_MsgHeaderImpl[msginfo->header_id].ConvertFromLE(le_msg, msginfo->header_addr);
    if (0 != ret_code) {
        NDW_LOGERR("FAILED convert Message Header contents from LE to native format for " "header_id<%d>\n", msginfo->header_id);
        return msginfo;
    }

    msginfo->is_bad = false;

    return msginfo;
} // end method ndw_LE_to_MsgHeader


CHAR_T*
ndw_CreateTestJsonString(ndw_Topic_T* topic, const CHAR_T* message, LONG_T sequencer_number, INT_T* message_size)
{
    if (NULL == message_size)
        return NULL;

    *message_size = 0;

    cJSON* obj = cJSON_CreateObject();
    if (NULL != topic)
        cJSON_AddStringToObject(obj, "Topic", topic->topic_unique_name);
    cJSON_AddStringToObject(obj, "message", message);
    cJSON_AddNumberToObject(obj, "sequencer_number", sequencer_number);

    CHAR_T* json_str = cJSON_Print(obj);
    *message_size = strlen(json_str);
    cJSON_Delete(obj);

    return json_str;
} // end method ndw_CreateTestJsonString

CHAR_T*
ndw_CreateTestJsonStringofNSize(ndw_Topic_T* topic, const CHAR_T* message, LONG_T sequencer_number,
                                INT_T target_size, INT_T* message_size)
{
    if (NULL == message_size)
        return NULL;

    cJSON* obj = cJSON_CreateObject();
    if (NULL != topic)
        cJSON_AddStringToObject(obj, "Topic", topic->topic_unique_name);
    cJSON_AddStringToObject(obj, "message", message);
    cJSON_AddNumberToObject(obj, "sequencer_number", sequencer_number);

    CHAR_T* temp_str = cJSON_Print(obj);
    INT_T current_size = strlen(temp_str);
    free(temp_str);
    temp_str = NULL;

    INT_T remaining_size = target_size - current_size;

    INT_T i = 0;
    while (remaining_size > 0) {
        CHAR_T key[32];
        sprintf(key, "key_%d", i);
        CHAR_T* value = malloc(100);
        memset(value, 'x', 99);
        value[99] = '\0';
        cJSON_AddStringToObject(obj, key, value);
        free(value);

        temp_str = cJSON_Print(obj);
        current_size = strlen(temp_str);
        free(temp_str);
        temp_str = NULL;

        remaining_size = target_size - current_size;
        i++;
    }

    CHAR_T* json_str = cJSON_Print(obj);
    *message_size = strlen(json_str);
    cJSON_Delete(obj);

    return json_str;
} // end method ndw_CreateTestJsonString

INT_T
ndw_InitThreadSpecificMsgHeaders()
{
    pthread_once(&ndw_tls_header_and_message_once, ndw_tls_header_and_message_Init);
    pthread_setspecific(ndw_tls_header_and_message, NULL);

    pthread_once(&ndw_tls_OutMsgCx_once, ndw_tls_OutMsgCxt_Init);
    ndw_OutMsgCxt_T* out_msg_cxt = calloc(1, sizeof(ndw_OutMsgCxt_T));
    NDW_LOGX("TLS: ALLOCATE: ThreadID<%lu> out_msg_cxt<%p>\n", pthread_self(), out_msg_cxt);
    pthread_setspecific(ndw_tls_OutMsgCxt, out_msg_cxt);

    pthread_once(&ndw_tls_received_msg_header_once, ndw_tls_received_msg_header_Init);
    CHAR_T* ptr_aligned = ndw_alloc_align(NDW_MAX_HEADER_SIZE * 2);
    pthread_setspecific(ndw_tls_received_msg_header, ptr_aligned);

    pthread_once(&ndw_tls_received_MsgHeaderInfo_once, ndw_tls_received_MsgHeaderInfo_Init);
    CHAR_T* ptr_aligned_MsgInfo = ndw_alloc_align(sizeof(ndw_InMsgCxt_T));
    pthread_setspecific(ndw_tls_received_MsgHeaderInfo, ptr_aligned_MsgInfo);

#if 0
    NDW_LOG("Messaging TLS-es:\n  ndw_tls_header_and_message <0x%lX>\n"
                "  ndw_tls_OutMsgCxt <0x%lX>\n"
                "  ndw_tls_received_msg_header <0x%lX>\n"
                "  ndw_tls_received_MsgHeaderInfo<0x%lX>\n"
                "  ptr_aligned <0x%lX>\n"
                "  ptr_aligned_msgInfo <0x%lX>\n",
                    (ULONG_T) pthread_getspecific(ndw_tls_header_and_message),
                    (ULONG_T) pthread_getspecific(ndw_tls_OutMsgCxt),
                    (ULONG_T) pthread_getspecific(ndw_tls_received_msg_header),
                    (ULONG_T) pthread_getspecific(ndw_tls_received_MsgHeaderInfo),
                    (ULONG_T) ptr_aligned,
                    (ULONG_T) ptr_aligned_MsgInfo);
#endif

    ndw_DebugMsgHeaders();

    return 0;
} // end method ndw_InitThreadSpecificMsgHeaders

INT_T
ndw_DestroyThreadSpecificMsgHeaders()
{
    pthread_t this_thread = pthread_self();
#if 0
    NDW_LOG("Messaging TLS-es:\n  ndw_tls_header_and_message <0x%lX>\n"
                "  ndw_tls_OutMsgCxt <0x%lX>\n"
                "  ndw_tls_received_msg_header <0x%lX>\n"
                "  ndw_tls_received_MsgHeaderInfo<0x%lX>\n",
                    (ULONG_T) pthread_getspecific(ndw_tls_header_and_message),
                    (ULONG_T) pthread_getspecific(ndw_tls_OutMsgCxt),
                    (ULONG_T) pthread_getspecific(ndw_tls_received_msg_header),
                    (ULONG_T) pthread_getspecific(ndw_tls_received_MsgHeaderInfo));
#endif

#if 1
    if (NDW_IS_NDW_INIT_THREAD(this_thread)) {
    ndw_HeaderAndMsg_T* hm = pthread_getspecific(ndw_tls_header_and_message);
    if (NULL != hm) {
        if (NULL != hm->aligned_address) {
            free(hm->aligned_address);
            hm->aligned_address = NULL;
            hm->allocated_size = 0;
        }
        free(hm);
    }
    }
#endif
    ndw_safe_PTHREAD_KEY_DELETE("ndw_tls_header_and_message", ndw_tls_header_and_message);

#if 1
    if (NDW_IS_NDW_INIT_THREAD(this_thread)) {
    ndw_OutMsgCxt_T* mha = pthread_getspecific(ndw_tls_OutMsgCxt);
    if (NULL != mha) {
        NDW_LOGX("TLS: FREE: ThreadID<%lu> out_msg_cxt<%p>\n", pthread_self(), mha);
        free(mha);
    }
    }
#endif
    ndw_safe_PTHREAD_KEY_DELETE("ndw_tls_OutMsgCxt", ndw_tls_OutMsgCxt);

#if 1
    if (NDW_IS_NDW_INIT_THREAD(this_thread)) {
    UCHAR_T* received_msg_header = pthread_getspecific(ndw_tls_received_msg_header);
    if (NULL != received_msg_header) {
        free(received_msg_header);
    }
    }
#endif
    ndw_safe_PTHREAD_KEY_DELETE("ndw_tls_received_msg_header", ndw_tls_received_msg_header);

#if 1
    if (NDW_IS_NDW_INIT_THREAD(this_thread)) {
    UCHAR_T* received_MsgHeaderInfo = pthread_getspecific(ndw_tls_received_MsgHeaderInfo);
    if (NULL != received_MsgHeaderInfo) {
        free(received_MsgHeaderInfo);
    }
    }
#endif
    ndw_safe_PTHREAD_KEY_DELETE("ndw_tls_received_MsgHeaderInfo", ndw_tls_received_MsgHeaderInfo);
    
    return 0;
} // end method ndw_DestroyThreadSpecificMsgHeaders

void
ndw_InitMsgHeaders()
{
    ndw_ImplMsgHeader_T* pHeader = NULL;
    for (INT_T i = 0; i < NDW_MAX_HEADER_TYPES+1; i++) {
        pHeader = &ndw_MsgHeader_structure[i];
        pHeader->Init = NULL;
        pHeader->IsValid = ndw_ImplMsgHeader_IsValid;
        pHeader->Print = ndw_ImplMsgHeader_Print;
        pHeader->Compare = ndw_ImplMsgHeader_Compare;
        pHeader->LE_MsgHeaderSize = ndw_ImplMsgHeader_LE_MsgHeaderSize;
        pHeader->SetOutMsgFields = ndw_ImplMsgHeader_SetOutMsgFields;
        pHeader->ConvertToLE = ndw_ImplMsgHeader_ConvertToLE;
        pHeader->ConvertFromLE = ndw_ImplMsgHeader_ConvertFromLE;
    }

    //
    // Initialize All Message Headers and Versions here.
    //
    ndw_MsgHeaderImpl[NDW_MSGHEADER_1].Init = ndw_MsgHeader1_Init;
    ndw_MsgHeaderImpl[NDW_MSGHEADER_1].Init();

#if 0
    // XXX: Not needed!
    ndw_InitThreadSpecificMsgHeaders();
    ndw_DebugMsgHeaders();
#endif

} // end method ndw_InitMsgHeaders()

INT_T
ndw_GetMsgHeaderAndBody(ndw_OutMsgCxt_T *mha)
{
    if (NULL == mha) {
        return -1;
    }

    if ((mha->header_size < 0) || (mha->header_size > 255)) {
        return -2;
    }

    if (mha->message_size < 0) {
        return -3;
    }

    mha->header_address = 0;
    mha->message_address = 0;
    mha->current_allocation_size = 0;

    INT_T sz = mha->header_size + mha->message_size;

    ndw_HeaderAndMsg_T* hm = pthread_getspecific(ndw_tls_header_and_message);
    if (NULL == hm)
    {
        hm = calloc(1, sizeof(ndw_HeaderAndMsg_T));
        if (NULL == hm)
            return -4;

        pthread_setspecific(ndw_tls_header_and_message, hm);
    }

    if (hm->allocated_size < 0) {
        NDW_LOGERR( "*** ERROR: get_message_header_and_body(): Invalid allocated_size in TLS: %d\n", hm->allocated_size);
        return -5;
    }

    if ((hm->allocated_size > 0) && (NULL == hm->aligned_address)) {
            NDW_LOGERR( "*** ERROR: get_message_header_and_body(): Requested total size = %d, TLS HeaderAndMessage has allocted_size: %d but aligned_address is NULL!\n",
                        sz, hm->allocated_size);
            return -6; // Severe error!
    }

    if (hm->allocated_size >= sz)
    {
        mha->header_address = (ULONG_T*) hm->aligned_address;
        mha->message_address = (UCHAR_T*) (((UCHAR_T*) hm->aligned_address) + mha->header_size);
        mha->current_allocation_size = hm->allocated_size;
        memset(mha->header_address, 0, sz);
        return 0;
    }

    if (hm->allocated_size > 0) {
        free(hm->aligned_address);
        hm->aligned_address = NULL;
        hm->allocated_size = 0;
        mha->current_allocation_size = 0;
    }

    CHAR_T* ptr_aligned = ndw_alloc_align(sz);
    hm->aligned_address = (UCHAR_T*) ptr_aligned;
    hm->allocated_size = sz;
    mha->header_address = (ULONG_T*) hm->aligned_address;
    mha->message_address = (UCHAR_T*) (((UCHAR_T*) hm->aligned_address) + mha->header_size);
    mha->current_allocation_size = sz;

    return 0;

} // end method ndw_GetMsgHeaderAndBody

ndw_OutMsgCxt_T*
ndw_GetOutMsgCxt()
{
    ndw_OutMsgCxt_T* mha = pthread_getspecific(ndw_tls_OutMsgCxt);
    if (NULL == mha) {
        NDW_LOGERR( "*** FATAL ERROR: ThreadID<%lu> ndw_tls_OutMsgCxt is NULL!\n", pthread_self());
        ndw_exit(EXIT_FAILURE);
    }
    return mha;
} // end method ndw_GetOutMsgCxt

UCHAR_T*
ndw_GetReceivedMsgHeader()
{
    UCHAR_T* msg_header = pthread_getspecific(ndw_tls_received_msg_header);
    if (NULL == msg_header) {
        NDW_LOGERR( "*** FATAL ERROR: NULL value returned from pthread_getspecific!\n");
        ndw_exit(EXIT_FAILURE);
    }

    return msg_header;

} // end method ndw_GetReceivedMsgHeader

