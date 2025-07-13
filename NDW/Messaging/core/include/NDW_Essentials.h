
#ifndef _NDW_ESSENTIALS_H
#define _NDW_ESSENTIALS_H

#include "ndw_types.h"

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <cjson/cJSON.h>

/**
 * @file NDW_Essentials.h
 *
 * @brief Instead of including multiple header files to build and application, the application can just include this one header file.
 *
 * @author Andrena team member
 * @date 2025-07
 */

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

#include "ndw_types.h"
#include "NDW_Utils.h"
#include "RegistryData.h"
#include "MsgHeaders.h"
#include "MsgHeader_1.h"
#include "AbstractMessaging.h"
#include "NATSImpl.h"

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _NDW_ESSENTIALS_H */

