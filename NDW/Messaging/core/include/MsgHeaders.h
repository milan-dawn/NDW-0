
#ifndef _MSG_HEADERS_H
#define _MSG_HEADERS_H

#include "ndw_types.h"

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <endian.h>
#include <pthread.h>

#include <cjson/cJSON.h>

#include "ndw_types.h"
#include "RegistryData.h"

/**
 * @file MsgHeaders.h
 *
 * @brief We can support Multiple Message headers.
 * This header file encapsulates the ability to support multiple header types.
 * Header contents must be in LE (Little Endian) format.
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
 * @def NDW_ENCODING_FORMAT_NONE
 * @brief No data encoding format specified in message header.
 */
#define NDW_ENCODING_FORMAT_NONE 0

/**
 * @def NDW_ENCODING_FORMAT_NONE
 * @brief No data encoding format specified in message header.
 */
#define NDW_ENCODING_FORMAT_STRING 1

/**
 * @def NDW_ENCODING_FORMAT_JSON
 * @brief No data encoding format specified in message header.
 * @brief Message body content encoding format is JSON and it is specified in message header.
 */
#define NDW_ENCODING_FORMAT_JSON 2

/**
 * @def NDW_ENCODING_FORMAT_BINARY
 * @brief Message body content encoding format is binary and it is specified in message header.
 */
#define NDW_ENCODING_FORMAT_BINARY 3

/**
 * @def NDW_ENCODING_FORMAT_XML
 * @brief Message body content encoding format is XML and it is specified in message header.
 */
#define NDW_ENCODING_FORMAT_XML 4

/**
 * @def NDW_MAX_ENCODING_FORMAT
 * @brief Maximum numberof encoding format supported so far.
 */
#define NDW_MAX_ENCODING_FORMAT 4

/**
 * @def NDW_MAX_HEADER_TYPES
 * @brief Maximum number of message header types possible.
 * @note 0 and 255 are NOT valid header (types) identifiers! Do NOT use them!
 */
#define NDW_MAX_HEADER_TYPES 255

/**
 * @def NDW_MAX_HEADER_SIZE
 * @brief Maximum bytes possible for any and all header types.
 */
#define NDW_MAX_HEADER_SIZE 255

/**
 * @def NDW_MAX_MESSAGE_SIZE
 * @brief Maximum bytes possible for a message body, including header size.
 */
#define NDW_MAX_MESSAGE_SIZE (12 * 1024 * 1024)


/**
 * @struct ndw_InMsgCxt_T
 * @brief Structure that holds the an incoming message attributes.
 *
 * @note 
 * This structure is per thread and hence MT safe.
 */
typedef struct ndw_InMsgCxt
{
    UCHAR_T* data_addr;         // Data address Pointer as returned by the Vendor implementation code.
    INT_T data_size;            // Data size as returned by the Vendor implementation code.
    INT_T header_id;            // Header identifer we insert after we parse minimal part of the header.
    INT_T header_size;          // Header size we insert after we parse minimal part of the header.

    UCHAR_T* header_addr;       // Header Pointer address.

    UCHAR_T* msg_addr;          // Message Body Pointer address.
    INT_T msg_size;             // Message Body size, does NOT include size of header.

    const CHAR_T* error_msg;    // Error message related to header parsing or any other contextual errors.
    bool is_bad;                // Indicate if inbound message is bad or not.

} ndw_InMsgCxt_T;

// A minimum header would include:
// header_number, header_size, vendor_id, vendor_version and encoding_format.
// Without these it is not possible to handle a message.

/**
 * @def NDW_MIN_HEADER_SIZE
 * @brief Minimum size of header in bytes we expect so we can parse rest of the header and then the message body Pointer.
 *  This is because we can support multiple header types.
 */
#define NDW_MIN_HEADER_SIZE 5

/**
 * @struct ndw_OutMsgCxt_T
 * @brief Struct that holds an outbound Message attribrutes.
 *
 * @note This structure is per thread and hence MT safe.
 */
typedef struct ndw_OutMsgCxt
{
    INT_T app_id;                   // Application Identifier
    ndw_Topic_T* topic;             // Topic structure Pointer for the Topic on which we sending out a message.
    ndw_Connection_T* connection;   // Connection structure Pointer over which we will send the message.
    ndw_Domain_T* domain;           // Domain structure Pointer so we can put domain identifier in the header.

    ULONG_T* vendor_opaque;         // Just in case you want to put vendor specific data

    INT_T header_id;                // Header type identifer.
    INT_T header_size;              // Header size.
    ULONG_T* header_address;        // Pointer where header starts.

    INT_T message_size;             // Message size; does NOT include header size.
    UCHAR_T* message_address;       // Pointer wher address starts.

    INT_T encoding_format;          // Message body encoding format type.
    INT_T priority;                 // Message priority, if supported by underlying messaging vendor.
    INT_T flags;                    // Typically a bitmask to use to set flags as deemed by the application.
    INT_T correlation_id;           // Correlation identifer for request response or even for putting classification for messages.
    INT_T tenant_id;                // Typically used for security like ACLs (Access Control List).
    INT_T message_id;               // Message Identifier.
    INT_T message_sub_id;           // Message Sub Identifier.

    INT_T current_allocation_size; // A hint for memory allocaton. This should be greater than the message_size else things are going wrong!

    bool loopback_test;             // For testing only. Loops the message back without sending it to the message system.

} ndw_OutMsgCxt_T;


/**
 * @brief Initialize various types of Message Headers we support.
 *
 * @return None.
 */
extern void ndw_InitMsgHeaders();

/**
 * @brief Initialize Message Headers that are kept per thread.
 *
 * @return 0 on success, else < 0.
 */
extern INT_T ndw_InitThreadSpecificMsgHeaders();

/**
 * @brief Destroy Message Headers that are kept per thread.
 *
 * @return 0 on success, else < 0.
 */
extern INT_T ndw_DestroyThreadSpecificMsgHeaders();

/**
 * @brief Print message information onto a stream for diagnosis.
 *
 * @param[in] stream File stream to log diagnostic message to.
 * @param[in] info Inbound Message Context.
 *
 * @return None.
 */
extern void ndw_print_MessageHeaderInfo(FILE* stream, ndw_InMsgCxt_T* info);

/**
 * @brief Print message header information onto a stream for diagnosis.
 *
 * @return Pointer to ndw_InMsgCxt_T which holds information of inbound message.
 *
 * @note Do NOT free the returned structure as it is maintained as a TLS (Thread Local Storage or Thread Specific Storage).
 */
extern ndw_InMsgCxt_T* ndw_LE_to_MsgHeader(UCHAR_T* le_msg, INT_T le_msg_size);

/**
 * @brief Create a JSON string for testing purposes ONLY.
 *
 * @param[in] topic Topic on which the message is being published.
 * @param[in] message Pointer to where the JSON string should be stored.
 * @param[in] sequence_number Sequencer number of the message that will get put inside the JSON string.
 * @param[out] message_size Outbound message size is set. Note in does NOT include the length when factored
 *  in the NULL byte at the end out string that is generated.
 *
 * @return Pointer to buffer allocated for the Test JSON string.
 *
 * @note The output message_size Outbound message size does NOT include the length when factored in
 *  the NULL byte at the end out string that is generated. Also do NOT forget to free the memory of the string returned.
 *
 */
extern CHAR_T* ndw_CreateTestJsonString(ndw_Topic_T* topic, const CHAR_T* message, LONG_T sequencer_number, INT_T *message_size);

/**
 * @brief Create a JSON string of at least a certain number of specifed bytes for testing purposes ONLY.
 *
 * @param[in] topic Topic on which the message is being published.
 * @param[in] message Pointer to where the JSON string should be stored.
 * @param[in] sequence_number Sequencer number of the message that will get put inside the JSON string.
 * @param[in] target_size Minimum number of bytes for which we have to generate a Test JSON string.
 * @param[out] message_size Outbound message size is set. Note in does NOT include the length when factored
 *  in the NULL byte at the end out string that is generated.
 *
 * @return Pointer to buffer allocated for the Test JSON string.
 *
 * @note The output message_size Outbound message size does NOT include the length when factored in
 *  the NULL byte at the end out string that is generated. Also do NOT forget to free the memory of the string returned.
 */
extern CHAR_T* ndw_CreateTestJsonStringofNSize(ndw_Topic_T* topic, const CHAR_T* message, LONG_T sequencer_number, INT_T target_size, INT_T* message_size);

/**
 * @brief Return the output message context (ndw_OutMsgCxt_T).
 *  This object is stored in the Thread Local Storage (per thread data structure).
 *
 * @return Out Message Context Pointer ndw_OutMsgCxt_T.
 *
 * @note
 *  This object is stored in the Thread Local Storage (per thread data structure). So do NOT free it.
 */
extern ndw_OutMsgCxt_T* ndw_GetOutMsgCxt();



/**
 * @brief 
 *
 * @param[in] param1
 * @param[out] param2
 *
 * @return None.
 *
 * @note
 *
 */

/**
 * @brief Prepare the outbound message header context to set enough memory space for outgoing message.
 *
 * @return On success it returns 0, else < 0.
 */
extern INT_T ndw_GetMsgHeaderAndBody(ndw_OutMsgCxt_T* mha);

/**
 * @brief Return pointer to Inbound Message Header.
 *
 * @return On success it returns 0, else < 0.
 */
extern UCHAR_T* ndw_GetReceivedMsgHeader();

/**
 * @var extern size_t ndw_max_message_size.
 * @brief Global variable to set the maximum size of message we can send or expect.
 *
 * XXX Review as it not used currently.
 */
extern size_t ndw_max_message_size;


/**
 * @struct ndw_HeaderAndMsg_T
 * @brief A per thread structure which holds the address and size of maximum buffer size we have allocated.
 *  We reuse this space for every outbound message.
 *  We only grow this buffer if the new message needs more buffer space than before.
 *
 */
typedef struct ndw_HeaderAndMsg
{
    INT_T allocated_size;           // Current allocated size of the buffer.
    UCHAR_T* aligned_address;       // Current start address of the buffer.
} ndw_HeaderAndMsg_T;


/**
 * @var extern pthread_key_t ndw_tls_header_and_message
 * @brief Thread Local Storage that holds current space for outbound message header and message body.
 */
extern pthread_key_t ndw_tls_header_and_message;

/**
 * @var extern pthread_key_t ndw_tls_message_header_attributes
 * @brief Thread Local Storage that holds attributes of current outbound message.
 */
extern pthread_key_t ndw_tls_message_header_attributes;

/**
 * @var extern pthread_key_t ndw_tls_received_msg_header
 * @brief Thread Local Storage that message header for the current inbound message.
 */
extern pthread_key_t ndw_tls_received_msg_header;

/**
 * @var extern pthread_key_t ndw_tls_received_MsgHeaderInfo
 * @brief Thread Local Storage that holds message header information for the current inbound message.
 */
extern pthread_key_t ndw_tls_received_MsgHeaderInfo;

/**
 * @struct ndw_ImplMsgHeader_T
 * @brief Since we support multiple header types we simuate inheritance through Pointer to functions..
 *  This header has series of functions needed to support multiple header types.
 */
typedef struct ndw_ImplMsgHeader
{
    // Initialize message header
    void (*Init)();

    // Returns true if message header is valid and supported.
    bool (*IsValid)(INT_T header_id);

    // Compare 2 message haeders and return 0 if they are equal, else return < 0 which is also an indicator of
    // the  positional field value where the coparision led to mismatch.
    INT_T (*Compare)(INT_T header_id, ULONG_T* pHeader1, ULONG_T* pHeader2);

    // Print the message header for debugging.
    void (*Print)(INT_T header_id, ULONG_T* pHeader);

    // Given header type identiifer return Message Header Size. LE is for Little Endian designation.
    INT_T (*LE_MsgHeaderSize)(INT_T header_id);

    // Set all the common and known fields in the outbound message structure.
    // Example of such common fields are: Application identifier, Domain identifer, Topic identifier, etc.
    INT_T (*SetOutMsgFields)(ndw_OutMsgCxt_T* cxt);

    // Convert an outbound message header to Little Endian format.
    INT_T (*ConvertToLE)(UCHAR_T* header_address);

    // Convert an inbound message header from Little Endian format to native format.
    INT_T (*ConvertFromLE)(UCHAR_T* src, UCHAR_T* dest);
} ndw_ImplMsgHeader_T;

/**
 * @var extern ndw_ImplMsgHeader_T ndw_MsgHeaderImpl
 * @brief This is the Pointer to array of ndw_MsgHeaderImpl_T, which holds function pointers
 *  to specific implementation of a Message Header.
 */

extern ndw_ImplMsgHeader_T* ndw_MsgHeaderImpl;

#ifdef __cplusplus
}
#endif /* _cplusplus */

#endif /* _MSG_HEADERS_H */

