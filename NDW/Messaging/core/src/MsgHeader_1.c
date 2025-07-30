
#include "MsgHeader_1.h"

extern int ndw_verbose;

void
ndw_MsgHeader1_Init()
{
    if (NULL == ndw_MsgHeaderImpl) {
        NDW_LOGERR("*** FATAL ERROR: ndw_msgHeaderImpl global POINTER NOT set!\n");
        ndw_exit(EXIT_FAILURE);
    }

    int size_of_struct = sizeof(ndw_MsgHeader1_T);
    if (NDW_MSGHEADER1_V1_LE_MSG_SIZE != size_of_struct) {
        NDW_LOGERR("*** FATAL ERROR: Contract and Compiling Issue: NDW_MSGHEADER1_V1_LE_MSG_SIZE <%d> NOT-EQUAL-TO "
                    "sizeof(ndw_MsgHeader1_T) <%d>\n", NDW_MSGHEADER1_V1_LE_MSG_SIZE, size_of_struct);
        ndw_exit(EXIT_FAILURE);
    }

    ndw_MsgHeaderImpl[NDW_MSGHEADER_1].IsValid = ndw_MsgHeader1_IsValid;
    ndw_MsgHeaderImpl[NDW_MSGHEADER_1].Print = ndw_MsgHeader1_Print;
    ndw_MsgHeaderImpl[NDW_MSGHEADER_1].Compare = ndw_MsgHeader1_Compare;
    ndw_MsgHeaderImpl[NDW_MSGHEADER_1].LE_MsgHeaderSize = ndw_MsgHeader1_LE_MsgHeaderSize;
    ndw_MsgHeaderImpl[NDW_MSGHEADER_1].SetOutMsgFields = ndw_MsgHeader1_SetOutMsgFields;
    ndw_MsgHeaderImpl[NDW_MSGHEADER_1].ConvertToLE = ndw_MsgHeader1_ConvertToLE;
    ndw_MsgHeaderImpl[NDW_MSGHEADER_1].ConvertFromLE = ndw_MsgHeader1_ConvertFromLE;

    if (ndw_verbose > 1) {
        // Debug Sample Message Header.
        ndw_MsgHeader1_T h1;
        h1.header_number = NDW_MSGHEADER_1;
        h1.header_size = sizeof(ndw_MsgHeader1_T);
        if (h1.header_size != NDW_MSGHEADER1_V1_LE_MSG_SIZE) {
            NDW_LOGERR("*** ERROR FATAL: sizeof(ndw_MsgHeader1_T) <%zu> "
                        "should be exactly as same as NDW_MSGHEADER1_V1_LE_MSG_SIZE <%d>\n",
                        sizeof(ndw_MsgHeader1_T), (int) NDW_MSGHEADER1_V1_LE_MSG_SIZE);
            ndw_exit(EXIT_FAILURE);
        }

        h1.vendor_id = 3;
        h1.vendor_version = 4;
        h1.encoding_format = 5;
        h1.domain = 6;
        h1.priority = 7;
        h1.flags = 8;
        h1.source_id = 9;
        h1.topic_id = 10;
        h1.correlation_id = 11;
        h1.tenant_id = 12;
        h1.payload_size = 13;
        h1.message_id = 14;
        h1.message_sub_id = 15;
        h1.timestamp = 16;

        ndw_MsgHeaderImpl[NDW_MSGHEADER_1].Print(h1.header_number, (ULONG_T*) &h1);
    }

} // end method ndw_MsgHeader1_Init

bool
ndw_MsgHeader1_IsValid(int header_id)
{
    return (NDW_MSGHEADER_1 == header_id);
} // end method ndw_MsgHeader1_IsValid

void
ndw_MsgHeader1_Print(int header_id, ULONG_T* pHeader)
{
    if (! ndw_MsgHeader1_IsValid(header_id)) {
        NDW_LOGERR("Invalid Print request for header_id<%d>\n", header_id);
        return;
    }

    if (NULL == pHeader) {
        NDW_LOGERR("NULL header POINTER as argument! " "header_id<%d>\n", header_id);
        return;
    }

    ndw_MsgHeader1_T* ph = (ndw_MsgHeader1_T*) pHeader;

    NDW_LOG("\nMessage_Header_1:\n");
    NDW_LOG("  header_number: %d (offset: %zu)\n", ph->header_number, offsetof(ndw_MsgHeader1_T, header_number));
    NDW_LOG("  header_size: %d (offset: %zu)\n", ph->header_size, offsetof(ndw_MsgHeader1_T, header_size));
    NDW_LOG("  vendor_id: %d\n (offset: %zu)", ph->vendor_id, offsetof(ndw_MsgHeader1_T, vendor_id));
    NDW_LOG("  vendor_version: %d (offset: %zu)\n", ph->vendor_version, offsetof(ndw_MsgHeader1_T, vendor_version));
    NDW_LOG("  encoding_format: %d (offset: %zu)\n", ph->encoding_format, offsetof(ndw_MsgHeader1_T, encoding_format));
    NDW_LOG("  domain: %d (offset: %zu)\n", ph->domain, offsetof(ndw_MsgHeader1_T, domain));
    NDW_LOG("  priority: %d (offset: %zu)\n", ph->priority, offsetof(ndw_MsgHeader1_T, priority));
    NDW_LOG("  flags: %d (offset: %zu)\n", ph->flags, offsetof(ndw_MsgHeader1_T, flags));
    NDW_LOG("  source_id: %d (offset: %zu)\n", ph->source_id, offsetof(ndw_MsgHeader1_T, source_id));
    NDW_LOG("  topic_id: %d (offset: %zu)\n", ph->topic_id, offsetof(ndw_MsgHeader1_T, topic_id));
    NDW_LOG("  correlation_id: %lu (offset: %zu)\n", ph->correlation_id, offsetof(ndw_MsgHeader1_T, correlation_id));
    NDW_LOG("  tenant_id: %d (offset: %zu)\n", ph->tenant_id, offsetof(ndw_MsgHeader1_T, tenant_id));
    NDW_LOG("  payload_size: %d (offset: %zu)\n", ph->payload_size, offsetof(ndw_MsgHeader1_T, payload_size));
    NDW_LOG("  message_id: %d (offset: %zu)\n", ph->message_id, offsetof(ndw_MsgHeader1_T, message_id));
    NDW_LOG("  message_sub_id: %d (offset: %zu)\n", ph->message_sub_id, offsetof(ndw_MsgHeader1_T, message_sub_id));
    NDW_LOG("  timestamp: %lu (offset: %zu)\n", ph->timestamp, offsetof(ndw_MsgHeader1_T, timestamp));
    NDW_LOG("\n");

} // end method ndw_MsgHeader1_Print

int
ndw_MsgHeader1_Compare(int header_id, ULONG_T* pHeader1, ULONG_T* pHeader2)
{
    if (! ndw_MsgHeader1_IsValid(header_id)) {
        NDW_LOGERR("Invalid Print request for header_id<%d>\n", header_id);
        return -100;
    }

    ndw_MsgHeader1_T* ph1 = (ndw_MsgHeader1_T*) pHeader1;
    ndw_MsgHeader1_T* ph2 = (ndw_MsgHeader1_T*) pHeader2;

    if (NULL == ph1) {
        NDW_LOGERR("NULL pHeader1 POINTER as argument! " "header_id<%d>\n", header_id);
        return -101;
    }

    if (NULL == ph2) {
        NDW_LOGERR("NULL pHeader2 POINTER as argument! " "header_id<%d>\n", header_id);
        return -102;
    }

    if (ph1->header_number != ph2->header_number) {
        NDW_LOGERR("*** ERROR: header_number mismatch (%d != %d)\n", ph1->header_number, ph2->header_number);
        return -1;
    }

    if (ph1->header_size != ph2->header_size) {
        NDW_LOGERR("*** ERROR: header_size mismatch (%d != %d)\n", ph1->header_size, ph2->header_size);
        return -2;
    }

    if (ph1->vendor_id != ph2->vendor_id) {
        NDW_LOGERR("*** ERROR: vendor_id mismatch (%d != %d)\n", ph1->vendor_id, ph2->vendor_id);
        return -3;
    }

    if (ph1->vendor_version != ph2->vendor_version) {
        NDW_LOGERR("*** ERROR: vendor_version mismatch (%d != %d)\n", ph1->vendor_version, ph2->vendor_version);
        return -4;
    }

    if (ph1->encoding_format != ph2->encoding_format) {
        NDW_LOGERR("*** ERROR: encoding_format mismatch (%d != %d)\n", ph1->encoding_format, ph2->encoding_format);
        return -5;
    }

    if (ph1->domain != ph2->domain) {
        NDW_LOGERR("*** ERROR: domain mismatch (%d != %d)\n", ph1->domain, ph2->domain);
        return -6;
    }

    if (ph1->priority != ph2->priority) {
        NDW_LOGERR("*** ERROR: priority mismatch (%d != %d)\n", ph1->priority, ph2->priority);
        return -7;
    }

    if (ph1->flags != ph2->flags) {
        NDW_LOGERR("*** ERROR: flags mismatch (%d != %d)\n", ph1->flags, ph2->flags);
        return -8;
    }

    if (ph1->source_id != ph2->source_id) {
        NDW_LOGERR("*** ERROR: source_id mismatch (%d != %d)\n", ph1->source_id, ph2->source_id);
        return -9;
    }

    if (ph1->topic_id != ph2->topic_id) {
        NDW_LOGERR("*** ERROR: topic_id mismatch (%d != %d)\n", ph1->topic_id, ph2->topic_id);
        return -10;
    }

    if (ph1->correlation_id != ph2->correlation_id) {
        NDW_LOGERR("*** ERROR: correlation_id mismatch (%lu != %lu)\n", ph1->correlation_id, ph2->correlation_id);
        return -11;
    }

    if (ph1->tenant_id != ph2->tenant_id) {
        NDW_LOGERR("*** ERROR: tenant_id mismatch (%d != %d)\n", ph1->tenant_id, ph2->tenant_id);
        return -12;
    }

    if (ph1->payload_size != ph2->payload_size) {
        NDW_LOGERR("*** ERROR: payload_size mismatch (%d != %d)\n", ph1->payload_size, ph2->payload_size);
        return -13;
    }

    if (ph1->message_id != ph2->message_id) {
        NDW_LOGERR("*** ERROR: message_id mismatch (%d != %d)\n", ph1->message_id, ph2->message_id);
        return -14;
    }

    if (ph1->message_sub_id != ph2->message_sub_id) {
        NDW_LOGERR("*** ERROR: message_sub_id mismatch (%d != %d)\n", ph1->message_sub_id, ph2->message_sub_id);
        return -15;
    }

    if (ph1->timestamp != ph2->timestamp) {
        NDW_LOGERR("*** ERROR: timestamp mismatch (%lu != %lu)\n", ph1->timestamp, ph2->timestamp);
        return -16;
    }

    // If all fields match, return 0
    return 0;
} // end method ndw_MsgHeader1_Compare

int
ndw_MsgHeader1_LE_MsgHeaderSize(int header_id)
{
    if (NDW_MSGHEADER_1 == header_id) {

        int size_of_struct = sizeof(ndw_MsgHeader1_T);
        if (NDW_MSGHEADER1_V1_LE_MSG_SIZE != size_of_struct) {
            NDW_LOGERR("*** FATAL ERROR: Contract and Compiling Issue: NDW_MSGHEADER1_V1_LE_MSG_SIZE <%d> NOT-EQUAL-TO "
                        "sizeof(ndw_MsgHeader1_T) <%d>\n", NDW_MSGHEADER1_V1_LE_MSG_SIZE, size_of_struct);
            ndw_exit(EXIT_FAILURE);
        }

        return NDW_MSGHEADER1_V1_LE_MSG_SIZE;
    }

    return -1;

} // end method ndw_MsgHeader1_LE_MsgHeaderSize

int
ndw_MsgHeader1_SetOutMsgFields(ndw_OutMsgCxt_T* cxt)
{
    if (NULL == cxt) {
        NDW_LOGERR("Invalid ndw_OutMsgCxtT_* parameter is NULL for  request with header_id<%d> header_id<%d>\n",
                ((NULL == cxt) ? -1 : cxt->header_id), ((NULL == cxt) ? -1 : cxt->header_id));
        return -1;
    }

    if (! ndw_MsgHeader1_IsValid(cxt->header_id)) {
        NDW_LOGERR("Invalid SetOutMsgFields request for header_id<%d> header_size<%d>\n", cxt->header_id, cxt->header_size);
        return -2;
    }

    if (NDW_MSGHEADER1_V1_LE_MSG_SIZE != cxt->header_size) {
        NDW_LOGERR("Invalid SetOutMsgFields set for header_id<%d> with INVALID header_size<%d>\n",
            cxt->header_id, cxt->header_size);
        return -3;
    }

    ndw_MsgHeader1_T* ph = (ndw_MsgHeader1_T*) cxt->header_address;
    if (NULL == ph) {
        NDW_LOGERR("Header cxt->header_address is NULL for header_id<%d> header_size<%d>\n", cxt->header_id, cxt->header_size);
        return -4;
    }

    if (NDW_MSGHEADER1_V1_LE_MSG_SIZE != sizeof(ndw_MsgHeader1_T)) {
        NDW_LOGERR("*** FATAL ERROR: Make sure sizeof(ndw_MsgHeader1_T) <%zu> is equal to "
                    " NDW_MSGHEADER1_V1_LE_MSG_SIZE <%d>\n",
            sizeof(ndw_MsgHeader1_T), NDW_MSGHEADER1_V1_LE_MSG_SIZE);
        ndw_exit(EXIT_FAILURE);
    }

    ph->header_number = (UCHAR_T) NDW_MSGHEADER_1;
    ph->header_size = (UCHAR_T) NDW_MSGHEADER1_V1_LE_MSG_SIZE;
    ph->vendor_id = (UCHAR_T) cxt->topic->connection->vendor_id;
    ph->vendor_version = (UCHAR_T) cxt->topic->connection->vendor_logical_version;
    ph->encoding_format = (UCHAR_T) cxt->encoding_format;
    ph->domain = (UCHAR_T) cxt->domain->domain_id;
    ph->priority = (UCHAR_T) cxt->priority;
    ph->flags = (UCHAR_T) cxt->flags;
    ph->source_id = (SHORT_T) cxt->app_id;
    ph->topic_id = (SHORT_T) cxt->topic->topic_unique_id;
    ph->correlation_id = cxt->correlation_id;
    ph->tenant_id = (SHORT_T) cxt->tenant_id;
    ph->payload_size = cxt->message_size;
    ph->message_id = (SHORT_T) cxt->message_id;
    ph->timestamp = ndw_GetCurrentUTCNanoseconds();

    return 0;
    
} // end method ndw_MsgHeader1_SetOutMsgFields

int
ndw_MsgHeader1_ConvertToLE(UCHAR_T *header_address)
{
    if (NULL == header_address) {
        NDW_LOGERR("*** FATAL ERROR: NULL header_address parameter!\n");
        ndw_exit(EXIT_FAILURE);
    }

    ndw_MsgHeader1_T* src_header = (ndw_MsgHeader1_T*) header_address;
    if (NDW_MSGHEADER_1 != src_header->header_number) {
        NDW_LOGERR("Unsupported conversion specified for header_id<%d>\n", src_header->header_number);
        return -1;
    }

    if (NDW_MSGHEADER_1 != src_header->header_number) {
        NDW_LOGERR("*** FATAL ERROR: Expected header_id<%d> but src_header has header_id<%d>\n",
                    NDW_MSGHEADER_1, src_header->header_number);
        ndw_exit(EXIT_FAILURE);
    }

    if (NDW_MSGHEADER1_V1_LE_MSG_SIZE != src_header->header_size) {
        NDW_LOGERR("*** FATAL ERROR: src_header->header_size <%d> is NOT equal to "
                    "NDW_MSGHEADER1_V1_LE_MSG_SIZE <%d> which is also expected to be of "
                    "sizeof(ndw_MsgHeader1_T) <%zu>\n",
                    src_header->header_size, NDW_MSGHEADER1_V1_LE_MSG_SIZE, sizeof(ndw_MsgHeader1_T));
        ndw_exit(EXIT_FAILURE);
    }

    ndw_MsgHeader1_T tmp_header;
    memset(&tmp_header, 0, sizeof(ndw_MsgHeader1_T));

    UCHAR_T* le_msg = (UCHAR_T*) &tmp_header;

    // Copy char fields directly (no endianness conversion needed)
    le_msg[offsetof(ndw_MsgHeader1_T, header_number)] = src_header->header_number;
    le_msg[offsetof(ndw_MsgHeader1_T, header_size)] = src_header->header_size;
    le_msg[offsetof(ndw_MsgHeader1_T, vendor_id)] = src_header->vendor_id;
    le_msg[offsetof(ndw_MsgHeader1_T, vendor_version)] = src_header->vendor_version;
    le_msg[offsetof(ndw_MsgHeader1_T, encoding_format)] = src_header->encoding_format;
    le_msg[offsetof(ndw_MsgHeader1_T, domain)] = src_header->domain;
    le_msg[offsetof(ndw_MsgHeader1_T, priority)] = src_header->priority;
    le_msg[offsetof(ndw_MsgHeader1_T, flags)] = src_header->flags;

    // Convert SHORT_T fields to Little Endian
    uint16_t source_id_le = htole16(src_header->source_id);
    uint16_t topic_id_le = htole16(src_header->topic_id);
    uint16_t tenant_id_le = htole16(src_header->tenant_id);
    uint16_t message_id_le = htole16(src_header->message_id);
    uint16_t message_sub_id_le = htole16(src_header->message_sub_id);

    memcpy(le_msg + offsetof(ndw_MsgHeader1_T, source_id), &source_id_le, sizeof(source_id_le));
    memcpy(le_msg + offsetof(ndw_MsgHeader1_T, topic_id), &topic_id_le, sizeof(topic_id_le));
    memcpy(le_msg + offsetof(ndw_MsgHeader1_T, tenant_id), &tenant_id_le, sizeof(tenant_id_le));
    memcpy(le_msg + offsetof(ndw_MsgHeader1_T, message_id), &message_id_le, sizeof(message_id_le));
    memcpy(le_msg + offsetof(ndw_MsgHeader1_T, message_sub_id), &message_sub_id_le, sizeof(message_sub_id_le));

    // Convert int field to Little Endian
    UINT_T payload_size_le = htole32(src_header->payload_size);
    memcpy(le_msg + offsetof(ndw_MsgHeader1_T, payload_size), &payload_size_le, sizeof(payload_size_le));

        // Convert ULONG_T field to Little Endian (assuming 64-bit)
    ULONG_T correlation_id_le = htole64(src_header->correlation_id);
    ULONG_T timestamp_le = htole64(src_header->timestamp);
    memcpy(le_msg + offsetof(ndw_MsgHeader1_T, correlation_id), &correlation_id_le, sizeof(correlation_id_le));
    memcpy(le_msg + offsetof(ndw_MsgHeader1_T, timestamp), &timestamp_le, sizeof(timestamp_le));

    // Overwrite parameter header.
    memcpy(header_address, &tmp_header, NDW_MSGHEADER1_V1_LE_MSG_SIZE);

    return 0;
} // end method ndw_MsgHeader1_ConvertToLE

int
ndw_MsgHeader1_ConvertFromLE(UCHAR_T *src, UCHAR_T* dest)
{
    if (NULL == src) {
        NDW_LOGERR("*** FATAL ERROR: NULL src parameter!\n");
        ndw_exit(EXIT_FAILURE);
    }

    if (NULL == dest) {
        NDW_LOGERR("*** FATAL ERROR: NULL dest parameter!\n");
        ndw_exit(EXIT_FAILURE);
    }

    UCHAR_T* le_msg = src;

    int header_id = (int) (*le_msg);
    int header_size = (int) (*(le_msg+1));
    if (NDW_MSGHEADER_1 != header_id) {
        NDW_LOGERR("Unsupported conversion specified for header_id<%d>\n", header_id);
        return -1;
    }

    if (NDW_MSGHEADER1_V1_LE_MSG_SIZE != header_size) {
        NDW_LOGERR("header_size <%d> is not equal to "
                    "NDW_MSGHEADER1_V1_LE_MSG_SIZE <%d> which is expected to be equal to sizeof(ndw_MsgHeader1_T) <%zu>\n",
            header_size, NDW_MSGHEADER1_V1_LE_MSG_SIZE, sizeof(ndw_MsgHeader1_T));
        return -2;
    }

    ndw_MsgHeader1_T* dest_header = (ndw_MsgHeader1_T*) dest;

    // Copy char fields directly (no endianness conversion needed)
    dest_header->header_number = le_msg[offsetof(ndw_MsgHeader1_T, header_number)];
    dest_header->header_size = le_msg[offsetof(ndw_MsgHeader1_T, header_size)];

    if (NDW_MSGHEADER_1 != dest_header->header_number) {
        NDW_LOGERR("*** FATAL ERROR: conversion mismatched for header_id<%d> Source has header_id<%d>\n",
                    dest_header->header_number, header_id);
        return -3;
    }

    dest_header->vendor_id = le_msg[offsetof(ndw_MsgHeader1_T, vendor_id)];
    dest_header->vendor_version = le_msg[offsetof(ndw_MsgHeader1_T, vendor_version)];
    dest_header->encoding_format = le_msg[offsetof(ndw_MsgHeader1_T, encoding_format)];
    dest_header->domain = le_msg[offsetof(ndw_MsgHeader1_T, domain)];
    dest_header->priority = le_msg[offsetof(ndw_MsgHeader1_T, priority)];
    dest_header->flags = le_msg[offsetof(ndw_MsgHeader1_T, flags)];

    // Convert SHORT_T fields from Little Endian
    uint16_t source_id_le;
    uint16_t topic_id_le;
    uint16_t tenant_id_le;
    uint16_t message_id_le;
    uint16_t message_sub_id_le;

    memcpy(&source_id_le, le_msg + offsetof(ndw_MsgHeader1_T, source_id), sizeof(source_id_le));
    memcpy(&topic_id_le, le_msg + offsetof(ndw_MsgHeader1_T, topic_id), sizeof(topic_id_le));
    memcpy(&tenant_id_le, le_msg + offsetof(ndw_MsgHeader1_T, tenant_id), sizeof(tenant_id_le));
    memcpy(&message_id_le, le_msg + offsetof(ndw_MsgHeader1_T, message_id), sizeof(message_id_le));
    memcpy(&message_sub_id_le, le_msg + offsetof(ndw_MsgHeader1_T, message_sub_id), sizeof(message_sub_id_le));

    dest_header->source_id = le16toh(source_id_le);
    dest_header->topic_id = le16toh(topic_id_le);
    dest_header->tenant_id = le16toh(tenant_id_le);
    dest_header->message_id = le16toh(message_id_le);
    dest_header->message_sub_id = le16toh(message_sub_id_le);

    // Convert int field from Little Endian
    UINT_T payload_size_le;
    memcpy(&payload_size_le, le_msg + offsetof(ndw_MsgHeader1_T, payload_size), sizeof(payload_size_le));
    dest_header->payload_size = le32toh(payload_size_le);

    // Convert ULONG_T field from Little Endian (assuming 64-bit)
    ULONG_T correlation_id_le;
    ULONG_T timestamp_le;
    memcpy(&correlation_id_le, le_msg + offsetof(ndw_MsgHeader1_T, correlation_id), sizeof(correlation_id_le));
    memcpy(&timestamp_le, le_msg + offsetof(ndw_MsgHeader1_T, timestamp), sizeof(timestamp_le));
    dest_header->correlation_id = le64toh(correlation_id_le);
    dest_header->timestamp = le64toh(timestamp_le);

    return 0;
} // end method ndw_MsgHeader1_ConvertFromLE

