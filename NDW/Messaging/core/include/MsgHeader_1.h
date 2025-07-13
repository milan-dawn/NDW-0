
#ifndef _MSG_HEADER_1_H
#define _MSG_HEADER_1_H

#include "ndw_types.h"

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <endian.h>
#include <pthread.h>
#include <limits.h>

#include <cjson/cJSON.h>

#include "ndw_types.h"
#include "MsgHeaders.h"
#include "RegistryData.h"

/**
 * @file MsgHeader_1.h
 *
 * @brief We can support Multiple Message headers.
 * This is the implementation of the default message header which is 32 bytes.
 * The header contents must be in LE (Little Endian) format.
 * If you developing another message header, follow this pattern
 * to create a file header and source file and plug it into the framework.
 * Each messge header must have 5 fields at top in a certain order. These are:
 * 1) Header Identifier
 * 2) Header Size 
 * 3) Vendor Identifier (Over which pub-sub messaging is occuring).
 * 4) Vendor Version (Version number of vendor product being used).
 * 5) Encoding format (Type of data in message payload, e.g., JSON, XML, Binary, etc.).
 *
 * @author Andrena team member
 * @date 2025-07
 */

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/**
 * @def NDW_MSGHEADER_1
 * @brief This is the header identifier for default message header.
 * @note Do NOT use 0 for a header identifier value.
 */
#define NDW_MSGHEADER_1            20

/**
 * @def NDW_MSGHEADER1_V1_LE_MSG_SIZE
 * @brief This is the default header's size.
 * All code must assert that this is valid when sending a message and receving it.
 */
#define NDW_MSGHEADER1_V1_LE_MSG_SIZE 32

/**
 * @struct ndw_MsgHeader1_T
 * @brief The default message header structure.
 *
 * @note 
 * Here we use a struct for efficiency, but you better ensure the size of the structure
 *  EXACTLY matches the size of the header. If your compilier cannot support it then 
 *  implement it with byte manipulation techiques.
 */
typedef struct ndw_MsgHeader1
{
    UCHAR_T header_number;      // Header Identifier.
    UCHAR_T header_size;        // Header Size.
    UCHAR_T vendor_id;          // Vendor Identifier to identify message is being sent over which vendor implementation.
    UCHAR_T vendor_version;     // Logic version of the Vendor implementation code base.
    UCHAR_T encoding_format;    // Message encoding format (e.g., JSON, XML, Binary, etc.)
    UCHAR_T domain;             // Domain (It is a group of Connections and Topics).
    UCHAR_T priority;           // Message Priority.
    UCHAR_T flags;              // Message flags. 8 bits and hence 8 flags value possible.
    SHORT_T source_id;          // This is Application identifier who sent the message.
    SHORT_T topic_id;           // This is the logical Topic identifier. Topic encapsulates both pub and sub operations.
    SHORT_T correlation_id;     // Message correlation identifier.
    SHORT_T tenant_id;          // Message tentant identifier. Typically used for security.
    SHORT_T payload_size;       // The size of the user data (payload). Do not add header size to it.
    SHORT_T message_id;         // Message Identifiery or Message Type.
    SHORT_T message_sub_id;     // Message Sub-Identifier for further subdivision within Message Identifier.
    ULONG_T timestamp;          // Timestamp. Should be in UTC format.
} ndw_MsgHeader1_T;

/**
 * If you have used a C struct to use as a message header you better make sure the compiler created the exact size.
 */
_Static_assert(sizeof(ndw_MsgHeader1_T) == 32, "Size of default header structure must be 32 bytes as agreed");


/**
 * @brief Initialize Message Header Type.
 * @return None.
 */
extern void ndw_MsgHeader1_Init();

/**
 * @brief Check if header id is indeed for this Message header type.
 * @return true if it is a valid Message header of its type, else false.
 */
extern bool ndw_MsgHeader1_IsValid(INT_T header_id);

/**
 * @brief Print out messge header contents. Useful for debugging.
 *
 * @param[in] header_id Header identifier.
 * @param[in] pHeader Pointer to the header.
 *
 * @return None.
 */
extern void ndw_MsgHeader1_Print(INT_T header_id, ULONG_T* pHeader);

/**
 * @brief Compare two headers to see if their contents are the same.
 *
 * @param[in] header_id Header identifier.
 * @param[in] pHeader1 Pointer to the first header.
 * @param[in] pHeader2 Pointer to the second header.
 * @return 0 if equal, else < 0 indicating field number that diverged.
 */
extern INT_T ndw_MsgHeader1_Compare(INT_T header_id, ULONG_T* pHeader1, ULONG_T* pHeader2);

/**
 * @brief Given Message header identifier return expected size of the header.
 * @param[in] header_id Header identifier.
 *
 * @return Integer number indicating total size of header expected.
 */
extern INT_T ndw_MsgHeader1_LE_MsgHeaderSize(INT_T header_id);

/**
 * @brief Set fields on the header for an outgoing message.
 *  Examples of fields that are set are: Application Identifer, Domain Indentifer, Topic Identifier,
 *  Header Id, Header Version Id, Vendor Id, Vendor Logical Version Id, etc.
 *
 * @param[in] cxt Outbound per thread data structure containing header and message body.
 *
 * @return 0 on success, else < 0.
 */
extern INT_T ndw_MsgHeader1_SetOutMsgFields(ndw_OutMsgCxt_T* cxt);

/**
 * @brief Given header Pointer convert fields of the header to LE (Little Endian format).
 *
 * @param[in] header_address Start of header address.
 *
 * @return 0 on success, else < 0.
 */
extern INT_T ndw_MsgHeader1_ConvertToLE(UCHAR_T* header_address);

/**
 * @brief Given header Pointer convert fields from expected  LE (Little Endian format) to native Endianness format.
 *
 * @param[in] header_address Start of header address.
 *
 * @return 0 on success, else < 0.
 */
extern INT_T ndw_MsgHeader1_ConvertFromLE(UCHAR_T* src, UCHAR_T* dest);

#ifdef __cplusplus
}
#endif /* _cplusplus */

#endif /* _MSG_HEADER_1_H */

