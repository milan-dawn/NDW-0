
#ifndef _NDW_UTILS_H
#define _NDW_UTILS_H

#include "ndw_types.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <endian.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdatomic.h>
#include <stdint.h>
#include <inttypes.h>
#include <getopt.h>
#include <errno.h>

#include "uthash.h"

#include <cjson/cJSON.h>

#include "ndw_types.h"

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/**
 * @file NDW_Utils.h
 *
 * @brief Many common utilities found in modern languages are missing in C language.
 * Over here we include or implement ones we need.
 * Examples of such utilties are functions related to time, string manipulations,
 *  logging with file name, function name and line number, etc., etc.
 *
 * @author Andrena team member
 * @date 2025-07
 */

/**
 * @brief TLS Thread Local Storage destructor function.
 *
 * @param [in] ptr Pointer to deallocate
 *
 * @return None.
 * @note It does not free the Pointer as there code does it itself.
 */
static inline void ndw_TLSDestructor(void* ptr) { (void) ptr; }

/**
 * @brief Deletes the pthread_key_t.
 * Note that ONLY the invoker of ndw_Init can do this as that is the one that does pthread_once invocation!
 *
 * @param [in] ptr Pointer to deallocate
 *
 * @return None.
 * @note Note that ONLY the invoker of ndw_Init can do this as that is the one that does pthread_once invocation!
 * @see pthread_self_ndw_Init
 */
extern int ndw_safe_PTHREAD_KEY_DELETE(const char* name, pthread_key_t key);

/**
 * @var extern pthread_self_ndw_Init.
 * @brief This is set when ndw_Init is called.
 * @note  The thread that can only invoke delete on pthread_key_t.
 */
extern pthread_t    pthread_self_ndw_Init;

static inline bool NDW_IS_NDW_INIT_THREAD(pthread_t id) { return (id == pthread_self_ndw_Init); }

/**
 * @def NDW_ISNULLCHARPTR
 * @brief Returns tre if the Pointer to the character buffer is NULL or the first character holds a NULL byte value.
 */
#define NDW_ISNULLCHARPTR(ptr) ((NULL == (ptr)) || ('\0' == *(ptr)))

/**
 * @brief Allocate aligned memory at 8 byte boundary.
 *
 * @param[in] size Number of bytes to allocate.
 *
 * @return Pointer to buffer allocated.
 */
extern CHAR_T* ndw_alloc_align(size_t size);

/**
 * @struct NDW_TestArgs_T
 * @brief Holds (stress) testing parameters.
 *
 * @see See method ndw_ParseProgramOptions
 */
typedef struct NDW_TestArgs
{
    int num_options_given;  // Total number of options passed in.
    int num_options_valid;  // Total number of options that are valid.
    char* config_file;      // Configuration file.
    int max_msgs;           // Maximum number of messagse to publish.
    int bytes_size;         // Approximate size of each message.
    int wait_time_seconds;  // Wait time in seconds when to exit if no messages are received.
    bool is_pub;            // Is Publication enabled?
    bool is_sub;            // Is Subscription enabled?
    bool run_valgrind;      // Run it with valgrind?
} NDW_TestArgs_T;

/**
 * @var extern CHAR_T ndw_test_args.
 * @brief Holds (stress) testing parameters.
 *
 * @see NDW_TestArgs_T
 */

/**
 * @brief Parses Program Options.
 *
 * @param[in] argc Number of command-line arguments [from main program]
 * @param[in] argv Array of command-line arguments [from main program]
 * @param[out] config Pointer to configuration file; defaults to "Registry.JSON"
 * @param[out] num_msgs Total number of messages to send; defaults to 1
 * @param[out] msg_bytes Approximate message size in bytes; defaults to 2048 bytes
 * @param[out] wait_time Time to wait in seconds prior to program exit
 *
 * @par Command-line Options:
 *   - `--c <file>`: Specify configuration file (default: Registry.JSON)
 *   - `--m <number>`: Specify number of messages
 *   - `--b <bytes>`: Specify approximate message size in bytes
 *   - `--w <time>`: Specify wait time before program exit
 *   - `--p <y>`: Publication enabled?
 *   - `--s <y>`: Subscription enabled?
 *   - `--v <y>`: Run valgrind?
 *
 * @return 0 on success, non-zero on failure
 *
 * @see ndw_TestArgs_T
 */
NDW_TestArgs_T*
ndw_ParseProgramOptions(INT_T argc, CHAR_T** argv);

/*
 * Copy maximum of N bytes.
 * max_dest_bytes should include the total bytes and ability to store the NULL character!
 * Returns string length of copy if a successful copy, else < 0.
 */

/**
 * @brief Copy maximum of N bytes.
 *
 * @param[in] dest Destination to copy to.
 * @param[in] max_dest_bytes Should include total bytes and ability to store the NULL character at the end!
 * @param[in] src Source address to copy from.
 *
 * @return Returns string length of copy if a successful copy, else < 0.
 */
extern int ndw_CopyMaxBytes(CHAR_T* dest, size_t max_dest_bytes, const CHAR_T* src);

// The last string parameter should be NULL.

/**
 * @brief Concatenate a variable number of strings. The last parameter must be NULL.
 *
 * @param[in] first_str; followed by a variable number of strings where the last parameter must be NULL.
 *
 * @return Pointer to concatenated string(s).
 *
 * @note
 * It is important to note that the last parameter must be NULL.
 * It is responsiblity of the caller to eventually free the allocated string.
 */
extern CHAR_T* ndw_ConcatStrings(CHAR_T* first_str, ...);

/**
 * @brief Contact 2 strings with a separator character.
 *
 * @param[in] s1 First string.
 * @param[in] s2 Second string.
 * @param[in] separator Separator character between the 2 strings to be concatenated.
 *
 * @return Newly formed concatenated string.
 *
 * @note
 * It is responsiblity of the caller to eventually free the allocated string.
 */
extern CHAR_T* ndw_Concat_Strings(const CHAR_T* s1, const CHAR_T* s2, const CHAR_T* separator);

// Split a string given a delimiter. The last element returned is NULL.

/**
 * @brief Split a string into a list of strings given a delimiter. The last element returned is NULL.
 *
 * @param[in] str String to be split.
 * @param[in] delimiter Delimiter character for splitting the input string.
 *
 * @return List of strings.
 *
 * @note
 * The last element of the list will be NULL.
 * It is responsiblity of the caller to eventually free the allocated string along with each element in the array.
 * @see ndw_free_string_list
 */
extern CHAR_T** ndw_split_string(const CHAR_T* str, CHAR_T delimiter);

/**
 * @brief Free a string list. Free all elements as well as the Pointer to the array.
 *
 * @param[in] list List of strings.
 *
 * @return None.
 *
 * @note will free the contents as well as pointer referring to the array of string.
 * So do NOT free that when calling this function!
 */
extern void ndw_free_string_list(CHAR_T** list);

/**
 * @struct NDW_NVPairs_T
 * @brief A structure that holds a list of names and corresponding values; both as strings.
 *
 * XXX: Use hashing eventually.
 * @note 
 * At this point not hold too many elements as this does not utilize hashing.
 */
typedef struct NDW_NVPairs
{
    int count;          // Number of names and corresponding values.
    CHAR_T** names;     // List of names.
    CHAR_T** values;    // List of corresponding values.
} NDW_NVPairs_T;


/**
 * @brief Convert an Ascii string to a long data type value.
 *
 * @param[in] str String whose long value is desired.
 * @param[out] value long data type value that is populated ONLY if parsing is successful.
 *
 * @return true if it is indeed a valid long data type value, else false.
 *
 * @note
 * Do not forget to check the return value to see if parsing has been successful.
 */
extern bool ndw_atol(const CHAR_T* str, LONG_T* value);

/**
 * @brief Convert an Ascii string to a int data type value.
 *
 * @param[in] str String whose int value is desired.
 * @param[out] value int data type value that is populated ONLY if parsing is successful.
 *
 * @return true if it is indeed a valid int data type value, else false.
 *
 * @note
 * Do not forget to check the return value to see if parsing has been successful.
 */
extern bool ndw_atoi(const CHAR_T* str, int* value);

/**
 * @brief Print name value pairs stored in a NDW_NVPairs_T data structure to the output log stream.
 *
 * @param[in] prefix A string to print out before the names and values are printed.
 * @param[out] nvpairs Data structure holding name value pairs (NDW_NVPairs_T).
 *
 * @return None.
 *
 * @note
 * Useful for debugging.
 *
 */
extern void ndw_PrintNVPairs(const CHAR_T* prefix, NDW_NVPairs_T* nvpairs);

/**
 * @brief REturn the value given the name.
 *
 * @param[in] name Name whose value is desired.
 * @param[in] nvpairs Data structure that holds the name value pairs.
 *
 * @return The value in string format if found, else NULL.
 *
 * @note
 * Do NOT free memory for the returned value.
 *
 */
extern const CHAR_T* ndw_GetNVPairValue(const CHAR_T* name, NDW_NVPairs_T* nvpairs);


/**
 * @brief Free memory for the name value pair data structure (NDW_NVPairs_T).
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
 * @brief Free necessary memory for a NDW_NVPairs_T structure which holds list of names and corresponding values in string format.
 *
 * @param[in] nvpairs The data structure containing list of names and corresponding names and values in string format.
 *
 * @return None.
 *
 * @note
 * It frees the list and not its contents as those were allocated during configuration loading and will be taken care of later.
 * But do free the pointer to the structure, if need be.
 */
extern void ndw_FreeNVPairs(NDW_NVPairs_T* nvpairs);

// NOTE: It modifies the string passed in.
// Caller has to free names and values only, but NOT contents of each.

/**
 * @brief Parse a string into names and values using the '^' (hat) character.
 * Note that it modifies the string that is passed in.
 *
 * @param[in] str The string that will be modified into name and value pairs.
 * @param[out] names Pointer to Pointer for the list of names.
 * @param[out] values Pointer to Pointer for the list of values.
 *
 * @return None.
 *
 * @ note It modifies the string that is passed in.
 * @see See the comments for the function ndw_FreeNVPairs
 */
extern int ndw_ParseNameValuePairs(CHAR_T *str, CHAR_T ***names, CHAR_T ***values);

/**
 * @brief Parse a string into names and values using the '^' (hat) character.
 * Note that it modifies the string that is passed in.
 *
 * @param[in] str The string that will be modified into name and value pairs.
 *
 * @return 0 on success, else < 0.
 *
 * @ note It modifies the string that is passed in.
 * @see See the comments for the function ndw_FreeNVPairs
 */
extern int ndw_ParseNVPairs(CHAR_T *str, NDW_NVPairs_T* nvpairs);

/**
 * @brief Get current time in nanoseconds in local time zone.
 *
 * @return Current time in nanoseconds in local time zone.
 */
extern uint64_t ndw_GetCurrentNanoSeconds();

/**
 * @brief Get current UTC time in nanoseconds.
 *
 * @return Current UTC time in nanoseconds.
 */
extern ULONG_T ndw_GetCurrentUTCNanoseconds();

/**
 * @brief Print hours, minutes, seconds, milliseconds, micrseconds nanoseconds give a UTC nanosecond input.
 *
 * @param[in] nanos Nanoseconds in UTC.
 * @return None.
 */
extern void ndw_PrintTimeFromNanos(ULONG_T nanos);

/**
 * @brief Returns a printable time in output log stream.
 * String format includes year, month, day, hour, minutes, seconds, milliseconds, microseonds and nanoseconds.
 *
 * @return String that can be printed.
 * String format includes year, month, day, hour, minutes, seconds, milliseconds, microseonds and nanoseconds.
 *
 * @note
 * It is responsiblity to free the returned string.
 *
 */
CHAR_T* ndw_GetPrintableCurrentTime();

/**
 * @brief Print current time into output log stream.
 * String format includes year, month, day, hour, minutes, seconds, milliseconds, microseonds and nanoseconds.
 *
 * @return None.
 */
void ndw_PrintCurrentTime();

// Time duration in milliseconds.


/**
 * @struct ndw_TimeDuration_T
 * @brief Holds a start time. One perodically queries to find if time has expired.
 */
typedef struct ndw_TimeDuration
{
    LONGLONG_T duration_millis; // Timeduration is in milliseconds.
    LONGLONG_T start_time;      // Start time in milliseconds.
    LONGLONG_T current_time;    // Current time in milliseconds.
    LONGLONG_T elapsed;         // Current elapsed time with respect to start_time.
    bool expired;               // Boolean flag to indicate if time has expired.
} ndw_TimeDuration_T;


/**
 * @brief Current thread to sleep for certain microseconds.
 *
 * @param[in] micros Number of microseconds to sleep for.
 *
 * @return None.
 */
extern void ndw_SleepMicros(LONG_T micros);

/**
 * @brief Current thread to sleep for certain millseconds.
 *
 * @param[in] millis Number of millseconds to sleep for.
 *
 * @return None.
 */
extern void ndw_SleepMillis(LONG_T millis);

/**
 * @brief Current thread to sleep for certain seconds.
 *
 * @param[in] seconds Number of seconds to sleep for.
 *
 * @return None.
 */
extern void ndw_SleepSeconds(LONG_T seconds);

/**
 * @brief Get current time in milliseconds.
 *
 * @return Return current time in milliseconds.
 */
extern LONGLONG_T ndw_GetCurrentTimeMillis();

/**
 * @brief Maintain time duration in a data structure.
 *
 * @param[in] Pointer to duration data structure.
 * @param[in] duration_value_millis Duration value in miliseconds to check for expiration.
 *
 * @return None.
 *
 * @see ndw_TimeDuration_T data structure.
 */
extern void ndw_TimeDurationMillisStart(ndw_TimeDuration_T* duration, LONGLONG_T duration_value_millis);

/**
 * @brief Maintain time duration in a data structure.
 *
 * @param[in] Pointer to duration data structure.
 * @param[in] duration_value_seconds Duration value in seconds to check for expiration.
 *
 * @return None.
 *
 * @see ndw_TimeDuration_T data structure.
 */
extern void ndw_TimeDurationSecondsStart(ndw_TimeDuration_T* duration, LONGLONG_T duration_value_seconds);

/**
 * @brief Calculate current time, set end time as current time, and then check for time duration expiration.
 *
 * @param[in] Pointer to duration data structure.
 *
 * @return true if time duration has expired with respect to current time, else false.
 *
 * @see ndw_TimeDuration_T data structure.
 */
extern bool ndw_TimeDurationStop(ndw_TimeDuration_T* duration);

/**
 * @brief Calculate current time, and then check for time duration expiration.
 *
 * @param[in] Pointer to duration data structure.
 *
 * @return true if time duration has expired with respect to current time, else false.
 *
 * @see ndw_TimeDuration_T data structure.
 */
extern bool ndw_TimeDurationExpired(ndw_TimeDuration_T* duration);

/**
 * @brief Sleep for duration milliseconds, then calculate current time, and then check for time duration expiration.
 *
 * @param[in] Pointer to duration data structure.
 *
 * @return true if time duration has expired with respect to current time after sleeping for duation milliseconds, else false.
 *
 * @see ndw_TimeDuration_T data structure.
 */
extern void ndw_TimeDurationSleepMillis(ndw_TimeDuration_T* duration);

// Uses Atomics to update and check for counter value.
// This is used for multi-threaded apps where threads update a value and
//    one or more threads check to see if a certain value has been reached or not.

/**
 * @struct ndw_Counter_T
 * @brief Uses an atomic counter.
 * Uses Atomics to update and check for counter value.
 * This is used for multi-threaded apps where threads update a value and
 *  one or more threads check to see if a certain value has been reached or not.
 * Wait for a counter to reach a value with an expiration time in milliseconds.
 */
typedef struct ndw_Counter
{
    atomic_long counter;
} ndw_Counter_T;

/**
 * @brief Create atomic counter that is held in a structure.
 *
 * @return Pointer to allocated structure that holds the atomic counter value.
 *
 * @see ndw_Counter_T
 *
 */
extern ndw_Counter_T* ndw_CreateCounter();


/**
 * @brief Get current value of Counter. Uses Atomics.
 *
 * @param[in] counter Pointer to struct that holds the atomic counter value.
 *
 * @return Current value of the counter that is kept inside the structure.
 *
 * @see ndw_Counter_T
 */
extern LONG_T ndw_CounterCurrentValue(ndw_Counter_T* counter);

// Uses Atomics to update the value.

/**
 * @brief Update the atomic counter stored in the structure and return current value.
 *
 * @param[in] counter Pointer to structure that holds the atomic counter value.
 * @param[in] update_value Value to add to the current atomic counter value kept in the structure.
 *
 * @return Current value of the atomic counter.
 *
 * @see ndw_Counter_T
 */
extern LONG_T ndw_CounterUpdate(ndw_Counter_T* counter, LONG_T update_value);

//
// Sleep for sleep_duration_millis and if counter is still less than check_value return false else true.
// Uses Atomics to check for the counter value.

/**
 * @brief Sleep for sleep_duration_millis and if counter is still less than check_value return false else true.
 *
 * @param[in] counter Pointer to structure that holds the atomic counter value.
 * @param[in] check_value
 * @param[in] sleep_duration_millis Time in milliseconds after which to check the atomic counter value.
 *
 * @return If counter is still less than input parameter check_value, then return false else true.
 */
extern bool ndw_CounterCheck(ndw_Counter_T* counter, LONG_T check_value, LONGLONG_T sleep_duration_millis);


// Statistics Utilities
// Latency Buckets with underflow and overflow slots.


/**
 * @struct ndw_LatencyBuckets_T
 * @brief Holds latency bucket information. A crude form of capturing percentile latency metrics.
 *
 * XXX: This needs to be implemented.
 */
typedef struct ndw_LatencyBuckets
{
    LONG_T counter;                 // Number of items added.
    LONG_T submicro_underflow;      // Underflow bucket.
    LONG_T microseconds[1000];      // Counters for microseconds level.
    LONG_T milliseconds[1000];      // Counters for milliseconds level.
    LONG_T seconds[60];             // Counters for microseconds level.
    LONG_T seconds_overflow;        // Overflow bucket.
} ndw_LatencyBuckets_T;

/**
 * @brief Update Latency Bucket structure with a nanoseconds (delta) latency value.
 *
 * @param[in] buckets Latency Bucket data structure Pointer.
 * @param[in] delta_ns Latency metric input in nanoseconds.
 *
 * XXX: This needs to be implemented.
 *
 * @return None.
 *
 * @see ndw_LatencyBuckets_T
 */

void ndw_AddToLatencyTimeBucket(ndw_LatencyBuckets_T* buckets, ULONG_T delta_ns);



/**
 * @def FORMAT_PRINTF
 * @brief Macro to perform printf statements.
 *
 * @return None.
 */
#if defined(__GNUC__) || defined(__clang__)
#define FORMAT_PRINTF(fmt_index, arg_index) __attribute__((format(printf, fmt_index, arg_index)))
#else
#define FORMAT_PRINTF(fmt_index, arg_index)
#endif

/**
 * @brief Since message notifications arrive in background thread, it necessitates logging to be MT safe.
 * We use a mutex for each log operation.
 *
 * @return None.
     */
void ndw_InitThreadSafeLogger();

/**
 * @brief Destroy mutex created for thread safe (MT safe) loggin.
 *
 */
void ndw_DestroyThreadSafeLogger();

/**
 * @brief Thread safe fprintf for logging to a stream. Default is stdout if stream parameter is NULL.
 *
 * @param[in] stream to log to. Default is stdout if this parameter is NULL.
 * @param[in] format Format specifier string and a variable number of arguments whose values are printed out.
 *
 * @return None.
 *
 * @note
 * Utilizes a mutex to make it MT safe.
 */
void ndw_fprintf(FILE *stream, const CHAR_T *format, ...) FORMAT_PRINTF(2, 3);

/**
 * @brief Logging to stream with source file name, function name, line number and in MT safe manner.
 *
 * @param[in] filename Invoking function's file name.
 * @param[in] line_number Invoking function's line number that made this call.
 * @param[in] function_name Name of the invoking function.
 * @param[stream] function_name Name of the invoking function.
 * @param[in] format Format specifier string and a variable number of arguments whose values are printed out.
 *
 * @return None.
 *
 * @note
 * Utilizes a mutex to make it MT safe.
 */
void ndw_print(const CHAR_T* filename, INT_T line_number, const CHAR_T* function_name, FILE* stream, const CHAR_T *format, ...) FORMAT_PRINTF(5, 6);

/**
 * @brief Print to standard out in MT safe manner.
 *
 * @param[in] format Format specifier string and a variable number of arguments whose values are printed out.
 *
 * @return None.
 * Utilizes a mutex to make it MT safe.
 *
 * @note
 * Utilizes a mutex to make it MT safe.
 */
void ndw_printf(const CHAR_T *format, ...) FORMAT_PRINTF(1, 2);



/**
 * @def CURRENT_FUNCTION
 * @brief Extracts current function name of the executing code.
 */
#if defined(__GNUC__)
  #define CURRENT_FUNCTION  __PRETTY_FUNCTION__
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L
  #define CURRENT_FUNCTION  __func__
#elif defined(__FUNCTION__)
  #define CURRENT_FUNCTION  __FUNCTION__
#else
  #define CURRENT_FUNCTION  "(unknown func)"
#endif

/**
 * @var extern FILE* ndw_out_file
 * @brief Global variable app can set during initialization to indiate where logging should go.
 */
extern FILE* ndw_out_file;

/**
 * @var extern FILE* ndw_err_file
 * @brief Global variable app can set during initialization to indiate where error logging should go.
 */
extern FILE* ndw_err_file;

/**
 * @brief static inline function tolog file name, function name, line number along with a log statement using format specifiers.
 *
 * @param[in] file Source file name.
 * @param[in] func Function name in source file.
 * @param[in] line Source file line number.
 * @param[in] Format specifier.
 * @param[in] Variable number of arguments for the format specifier.
 *
 * @return None.
 *
 * @note
 *    fmt is arg#5, variadics start at #6
 */
static inline void
_ndw_log_impl(const CHAR_T *file,
          const CHAR_T *func,
          INT_T          line,
          FILE        *out,
          const CHAR_T *fmt,   ...)
  __attribute__((format(printf, 5, 6)));

/**
 * @brief static inline function tolog file name, function name, line number along with a log statement using format specifiers.
 *
 * @param[in] file Source file name.
 * @param[in] func Function name in source file.
 * @param[in] line Source file line number.
 * @param[in] Format specifier.
 * @param[in] Variable number of arguments for the format specifier.
 *
 * @return None.
 */
static inline void
_ndw_log_impl(const CHAR_T *file,
          const CHAR_T *func,
          INT_T          line,
          FILE        *out,
          const CHAR_T *fmt,
          ...)
{
    va_list args;
    va_start(args, fmt);
    fprintf(out, "[%s:%d:%s] ", file, line, func);
    vfprintf(out, fmt, args);
    fprintf(out, "\n");
    va_end(args);
}


/**
 * @def NDW_LOG
 * @brief Use this to log to stream. It will NOT use source file name, source function name and line number in source file.
 */
#define NDW_LOG(fmt, ...)                              \
    ndw_print(NULL, -1, NULL,                          \
              ndw_out_file, fmt, ##__VA_ARGS__)

/**
 * @def NDW_LOGX
 * @brief Extended logging to ndw_out_file stream.
 * Use this to log to ndw_out_file stream.
 * It will use source file name, source function name and line number in source file.
 *
 * @see ndw_out_file
 */
#define NDW_LOGX(fmt, ...)                             \
    ndw_print(__FILE__, __LINE__, CURRENT_FUNCTION,    \
              ndw_out_file, fmt, ##__VA_ARGS__)

/**
 * @def NDW_LOGERR
 * @brief Use this to log errors to ndw_err_file stream.
 * It will use source file name, source function name and line number in source file.
 *
 * @see ndw_err_file
 */
#define NDW_LOGERR(fmt, ...)                           \
    ndw_print(__FILE__, __LINE__, CURRENT_FUNCTION,    \
              ndw_err_file, fmt, ##__VA_ARGS__)


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _NDW_UTILS_H */

