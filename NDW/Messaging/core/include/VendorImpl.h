
#ifndef _VENDORIMPL_H
#define _VENDORIMPL_H

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

#include "NDW_Utils.h"
#include "RegistryData.h"
#include "MsgHeaders.h"

/**
 * @file VendorImpl.h
 *
 * @brief The Abstract messaging layer supports multiple vendor platforms.
 * In C language we do not have inheritance feature.
 * Hence we have to simulate it using pointers to functions.
 * Check the struct ndw_ImplAPI_T which embodies the functions a concrete implementation of
 *  the abstraction layer needed by the vendor implementation.
 *
 * @see ndw_ImplAPI_T
 *
 * @author Andrena team member
 * @date 2025-07
 */

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/**
 * @var extern INT_T ndw_verbose
 * @brief  Extern variable that controls level of (increased) logging verbosity.
 * Bump up the value to get more and more levels of verbose logging.
 */
extern INT_T ndw_verbose; // Setting this greater than zero will trigger verbose output.

/**
 * @var extern INT_T ndw_debug_msg_header.
 * @brief  Extern variable that controls message header debugging statements.
 * Set it to > 0 to get the debugging level of logging for message headers.
 */
extern INT_T ndw_debug_msg_header;

//
// Implementation Vendor Identifiers, Name and Logical Version
// NOTE: Slots 0 to 255. But slots 1 through 255, inclusive, can be used only.
// NOTE: Slot 0 should not be used! Hence never have a Vendor ID as zero!
//

/**
 * @def NDW_MAX_API_IMPLEMENTATIONS
 * @brief Number of vendor implementations, including vendor versions, that can be supported.
 *
 * @note
 * Slots 0 through 255, inclusive, are available for use.
 */

#define NDW_MAX_API_IMPLEMENTATIONS 255 // 0 to 255 slots available


/**
 * @def NDW_IMPL_NATS_IO_ID
 * @brief NATS.io vendor logical unique identifier.
 */
#define NDW_IMPL_NATS_IO_ID 100

/**
 * @def NDW_IMPL_NATS_IO_NAME
 * @brief NATS.io vendor logical name.
 */
#define NDW_IMPL_NATS_IO_NAME "NATS.io"

/**
 * @def NDW_IMPL_NATS_LOGICAL_VERSION
 * @brief NATS.io vendor logical version number.
 */
#define NDW_IMPL_NATS_LOGICAL_VERSION 1


/**
 * @def NDW_IMPL_AERON_ID
 * @brief Aeron vendor logical unique indentifier.
 */
#define NDW_IMPL_AERON_ID 103

/**
 * @def NDW_IMPL_AERON_NAME
 * @brief Aeron vendor logical name.
 */
#define NDW_IMPL_AERON_NAME "Aeron"

/**
 * @def NDW_IMPL_AERON_LOGICAL_VERSION
 * @brief Aeron vendor logical version number.
 */
#define NDW_IMPL_AERON_LOGICAL_VERSION 1

/**
 * @def NDW_IMPL_KAFKA_ID
 * @brief Kafka vendor logical unique indentifier.
 */
#define NDW_IMPL_KAFKA_ID 104

/**
 * @def NDW_IMPL_KAFKA_NAME
 * @brief Kafka vendor logical name.
 */
#define NDW_IMPL_KAFKA_NAME "Kafka"

/**
 * @def NDW_IMPL_KAFKA_LOGICAL_VERSION
 * @brief Kakfa vendor logical version number.
 */
#define NDW_IMPL_KAFKA_LOGICAL_VERSION 1



typedef struct ndw_ImplAPI ndw_ImplAPI_T;

/**
 * @struct ndw_ImplAPI
 * @brief Holds pointer to functions, thereby simulating inheritance in C code,
 *  for various functions that need to be supported for ALL vendors.
 */

typedef struct ndw_ImplAPI
{
    INT_T vendor_id;                // Unique vendor logical indentifer.
    const CHAR_T* vendor_name;      // Unique vendor logical name.
    INT_T vendor_logical_version;   // Vendor logical version.

    INT_T (*Init)(ndw_ImplAPI_T*, INT_T vendor_id);
    void (*ShutdownConnection)(ndw_Connection_T* connection);
    void (*Shutdown)();

    INT_T (*ThreadInit)(ndw_ImplAPI_T*, INT_T vendor_id);
    INT_T (*ThreadExit)();

    INT_T (*ProcessConfiguration)(ndw_Topic_T* topic);

    INT_T (*Connect)(ndw_Connection_T* connection);
    INT_T (*Disconnect)(ndw_Connection_T* connection);
    bool (*IsConnected)(ndw_Connection_T* connection);
    bool (*IsClosed)(ndw_Connection_T* connection);
    bool (*IsDraining)(ndw_Connection_T* connection);

    INT_T (*PublishMsg)();

    INT_T (*SubscribeAsync)(ndw_Topic_T* topic);
    INT_T (*Unsubscribe)(ndw_Topic_T* topic);

    INT_T (*SubscribeSynchronously)(ndw_Topic_T* topic);
    INT_T (*SynchronousPollForMsg)(ndw_Topic_T* topic, const CHAR_T** msg, INT_T* msg_length, LONG_T timeout_ms, LONG_T* dropped_messages, void** vendor_closure);

    INT_T (*Publish_ResponseForRequestMsg)(ndw_Topic_T* topic);
    INT_T (*GetResponseForRequestMsg)(ndw_Topic_T* topic, const CHAR_T** msg, INT_T* msg_length, LONG_T timeout_ms, void** vendor_closure);


    INT_T (*CommitLastMsg)(ndw_Topic_T* topic, void* vendor_closure);

    INT_T (*GetQueuedMsgCount)(ndw_Topic_T* topic, ULONG_T* count);

    INT_T (*CommitQueuedMsg)(ndw_Topic_T* topic, void* vendor_closure);

    INT_T (*CleanupQueuedMsg)(ndw_Topic_T* topic, void* vendor_closure);

} ndw_ImplAPI_T;

/**
 * @brief get the structure that holds function pointers for vendor specific implementation, given the unique vendor identifer.
 *
 * @param[in] vendor_id Unique vendor identifier.
 *
 * @return Pointer to ndw_ImplAPI_T struct that holds pointers to functions for vendor specific implementation.
 *
 * @see ndw_ImplAPI_T structure.
 *
 */
extern ndw_ImplAPI_T* get_Implementation_API(INT_T vendor_id);

/**
 * @var extern ndw_ImplAPI impl_api
 * @brief  Global variable that is the starting address of an array of ndw_ImplAPI_T.
 *
 * @see ndw_ImplAPI_T structure.
 */

extern ndw_ImplAPI_T* impl_api; // An Array of 255 possible vendor implementations!

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _VENDORIMPL_H */

