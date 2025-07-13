
#ifndef _XDM_TYPES_H
#define _XDM_TYPES_H

#include <stdint.h>
#include <limits.h>   // if you later want to assert CHAR_BIT

/**
 * @file ndw_types.h
 *
 * @brief In C code there are platform specific build and usage considerations.
 *  We think there are standards, and to a large expect it is true.
 *  One needs to still work hard in coding towards types.
 *  We choose our definition for primitive types, so that on issues we can change those.
 *  Also using capitalized letter for primitive types, we felt it adds better focus and highlight to usage of primitive types.
 *
 * @author Andrena team member
 * @date 2025-07
 */

// 8‑bit
typedef char          CHAR_T;
_Static_assert(sizeof(CHAR_T) == 1, "CHAR_T must be 8 bits");

typedef unsigned char UCHAR_T;
_Static_assert(sizeof(UCHAR_T) == 1, "UCHAR_T must be 8 bits");

// 16‑bit
typedef int16_t       SHORT_T;
_Static_assert(sizeof(SHORT_T) == 2, "SHORT_T must be 16 bits");

// 16‑bit
typedef uint16_t       USHORT_T;
_Static_assert(sizeof(USHORT_T) == 2, "USHORT_T must be 16 bits");

// 32‑bit
typedef int32_t       INT_T;
_Static_assert(sizeof(INT_T) == 4, "INT_T must be 32 bits");

typedef uint32_t      UINT_T;
_Static_assert(sizeof(UINT_T) == 4, "UINT_T must be 32 bits");

// 64‑bit
typedef int64_t       LONG_T;
_Static_assert(sizeof(LONG_T) == 8, "LONG_T must be 64 bits");

typedef uint64_t      ULONG_T;
_Static_assert(sizeof(ULONG_T) == 8, "ULONG_T must be 64 bits");

// 64‑bit
// XXX: typedef int64_t       LONGLONG_T;
typedef long long       LONGLONG_T;
_Static_assert(sizeof(LONGLONG_T) == 8, "LONGLONG_T must be 64 bits");

#endif /* _XDM_TYPES_H */

