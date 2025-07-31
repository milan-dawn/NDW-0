
#include "NDW_Utils.h"

extern INT_T ndw_verbose;

FILE* ndw_out_file = NULL;
FILE* ndw_err_file = NULL;

pthread_t pthread_self_ndw_Init;

static pthread_mutex_t ndw_log_mutex;

#include <pthread.h>

static pthread_mutex_t key_create_mutex = PTHREAD_MUTEX_INITIALIZER;

int ndw_safe_PTHREAD_KEY_DELETE(const char* name, pthread_key_t key)
{
    int result = 0;

    pthread_t this_thread = pthread_self();
    if (this_thread == pthread_self_ndw_Init)
    {
        if (ndw_verbose > 0) {
            NDW_LOGX("==> pthread_t<%lu> Invoking pthread_key_delete for <%s>\n", pthread_self(), name);
        }

        pthread_mutex_lock(&key_create_mutex);
        result = pthread_key_delete(key);
        pthread_mutex_unlock(&key_create_mutex);
    }
    else {
        if (ndw_verbose > 0) {
            NDW_LOGX("==> pthread_t<%lu> SKIPPING pthread_key_delete for <%s>\n", pthread_self(), name);
        }
    }

    return result;
}

CHAR_T*
ndw_alloc_align(size_t size)
{
    void *memptr;
    INT_T ret_code = posix_memalign(&memptr, sizeof(ULONG_T), size);

    if ((0 != ret_code) || (NULL == memptr)) {
        NDW_LOGERR("*** FATAL ERROR: posix_memalign failed with ret_code<%d> errno<%d>\n",
                    ret_code, errno);
        ndw_exit(EXIT_FAILURE);
    }

    memset(memptr, 0, size);
    return (CHAR_T*) memptr;
} // end method ndw_alloc_align

/*
 * Copy maximum of N bytes.
 * max_dest_bytes should include the total bytes and ability to store the NULL Character!
 * Returns 0 if a successful copy, else < 0.
 */
INT_T
ndw_CopyMaxBytes(CHAR_T* dest, size_t max_dest_bytes, const CHAR_T* src)
{
    if ((NULL == dest) || (max_dest_bytes < 2) || (NULL == src))
        return -1;

    if ('\0' == *src) {
        *dest = '\0';
        return 0;
    }

    size_t src_len = strlen(src);
    if (src_len >= max_dest_bytes) {
        return -1;
    }

    snprintf(dest, max_dest_bytes, "%s", src);

    return 0;

} // end method ndw_CopyMaxBytes

CHAR_T*
ndw_ConcatStrings(CHAR_T* first_str, ...)
{
    va_list args;
    va_start(args, first_str);

    // Calculate total length
    size_t total_length = strlen(first_str);
    CHAR_T* str;
    va_start(args, first_str);
    while ((str = va_arg(args, CHAR_T*)) != NULL) {
        total_length += strlen(str);
    }
    va_end(args);

    // Allocate memory for the concatenated string
    CHAR_T* result = (CHAR_T*)malloc((total_length + 1) * sizeof(CHAR_T));
    if (!result) {
        return NULL; // Memory allocation failed
    }

    // Concatenate strings
    strcpy(result, first_str);
    va_start(args, first_str);
    while ((str = va_arg(args, CHAR_T*)) != NULL) {
        strcat(result, str);
    }
    va_end(args);

    return result;
} // end method ndw_ConcatStrings

CHAR_T*
ndw_Concat_Strings(const CHAR_T* s1, const CHAR_T* s2, const CHAR_T* separator)
{
    bool no_separator = NDW_ISNULLCHARPTR(separator);

    INT_T length = (no_separator) ?
            snprintf(NULL, 0, "%s%s", s1, s2) :
            snprintf(NULL, 0, "%s%s%s", s1, separator, s2);

    length += 1;
    CHAR_T* p = malloc(length * sizeof(CHAR_T));

    length = (no_separator) ?
            snprintf(p, length, "%s%s", s1, s2) :
            snprintf(p, length, "%s%s%s", s1, s2, separator);

    return p;
} // end method ndw_Concat_Strings

void
ndw_InitThreadSafeLogger()
{
    pthread_mutex_init(&ndw_log_mutex, NULL);

    if (NULL == ndw_out_file)
        ndw_out_file = stdout;

    if (NULL == ndw_err_file)
        ndw_err_file = stdout;

    if (ndw_verbose) {
        NDW_LOGX("...\n");
    }
} // end method ndw_InitThreadSafeLogger()

void
ndw_DestroyThreadSafeLogger()
{
    if (ndw_verbose) {
        NDW_LOGX("...\n");
    }

    pthread_mutex_destroy(&ndw_log_mutex);
} // end method ndw_DestroyThreadSafeLogger()

void
ndw_print(const CHAR_T* filename, INT_T line_number, const CHAR_T* function_name, FILE* stream, const CHAR_T *format, ...)
{
    if (stream == NULL) {
        stream = stdout;
    }

    va_list args;
    pthread_mutex_lock(&ndw_log_mutex);
    va_start(args, format);
    if (NULL != filename) {
        fprintf(stream, "[(tid: %lu) %s:%d:%s()] ", pthread_self(), filename, line_number, function_name);
    }
    vfprintf(stream, format, args);
    fflush(stream);
    va_end(args);
    pthread_mutex_unlock(&ndw_log_mutex);
} // end method ndw_print

void
ndw_fprintf(FILE *stream, const CHAR_T *format, ...)
{
    if (stream == NULL) {
        stream = stdout;
    }

    va_list args;
    pthread_mutex_lock(&ndw_log_mutex);
    va_start(args, format);
    vfprintf(stream, format, args);
    fflush(stream);
    va_end(args);
    pthread_mutex_unlock(&ndw_log_mutex);
}

void
ndw_printf(const CHAR_T *format, ...)
{
    va_list args;
    pthread_mutex_lock(&ndw_log_mutex);
    va_start(args, format);
    vfprintf(stdout, format, args);
    fflush(stdout);
    va_end(args);
    pthread_mutex_unlock(&ndw_log_mutex);
} // end method ndw_printf


CHAR_T **
ndw_split_string(const CHAR_T *input, CHAR_T delimiter)
{
    if (!input) return NULL;

    CHAR_T *copy = strdup(input);
    if (!copy) return NULL;

    size_t capacity = 10;
    size_t size = 0;
    CHAR_T **result = malloc(capacity * sizeof(CHAR_T *));
    if (!result) {
        free(copy);
        return NULL;
    }

    // Convert delimiter to a string to use with strtok
    CHAR_T delim[2] = { delimiter, '\0' };

    CHAR_T *token = strtok(copy, delim);
    while (token) {
        if (size >= capacity - 1) {
            capacity *= 2;
            CHAR_T **temp = realloc(result, capacity * sizeof(CHAR_T *));
            if (!temp) {
                for (size_t i = 0; i < size; i++) free(result[i]);
                free(result);
                free(copy);
                return NULL;
            }
            result = temp;
        }

        result[size++] = strdup(token);
        token = strtok(NULL, delim);
    }

    result[size] = NULL;  // Null-terminate
    free(copy);
    return result;
} // end method ndw_split_string


void
ndw_free_string_list(CHAR_T **list)
{
    if (!list) return;

    for (size_t i = 0; list[i] != NULL; i++) {
        free(list[i]);
    }
    free(list);
} // end method ndw_free_string_list

ULONG_T
ndw_GetCurrentNanoSeconds()
{
    struct timespec ts;
     
    // CLOCK_REALTIME or CLOCK_MONOTONIC (monotonic is preferred for Intervals)
    if (clock_gettime(CLOCK_REALTIME, &ts) == 0)
    {
#if 0
        NDW_LOG("Seconds: %ld\n", ts.tv_sec);
        NDW_LOG("Nanoseconds: %ld\n", ts.tv_nsec);
        NDW_LOG("Current time (ns): %ld\n", ts.tv_sec * 1000000000L + ts.tv_nsec);
#endif
    } else {
        perror("clock_gettime");
        return 1;
    }

    return (ULONG_T)ts.tv_sec * 1000000000ULL + (ULONG_T)ts.tv_nsec;
} // end method ndw_GetCurrentNanoSeconds

ULONG_T
ndw_GetCurrentUTCNanoseconds()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        perror("clock_gettime");
        return 0; // error case
    }

    return (ULONG_T)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
} // end method ndw_GetCurrentUTCNanoseconds

void
ndw_PrintTimeFromNanos(ULONG_T nanos)
{
    ULONG_T hours = nanos / 3600000000000ULL;
    nanos %= 3600000000000ULL;

    ULONG_T minutes = nanos / 60000000000ULL;
    nanos %= 60000000000ULL;

    ULONG_T seconds = nanos / 1000000000ULL;
    nanos %= 1000000000ULL;

    ULONG_T milliseconds = nanos / 1000000ULL;
    nanos %= 1000000ULL;

    ULONG_T microseconds = nanos / 1000ULL;
    ULONG_T remaining_nanos = nanos % 1000ULL;

    NDW_LOGX("Time: [HH:%" PRIu64 " MM:%" PRIu64 " SS:%" PRIu64 " ms:%" PRIu64 " us:%" PRIu64 " ns:%" PRIu64 "]\n",
            hours, minutes, seconds, milliseconds, microseconds, remaining_nanos);
} // end method ndw_PrintTimeFromNanos


static const size_t NUM_MICROSECONDS = 1000;
static const size_t NUM_MILLISECONDS = 1000;
static const size_t NUM_SECONDS = 60;

void
ndw_AddToLatencyTimeBucket(ndw_LatencyBuckets_T* buckets, ULONG_T delta_ns)
{
    buckets->counter += 1;

    if (delta_ns < 1000) {
        buckets->submicro_underflow += 1;
        return;
    }

    if (delta_ns < 1000000ULL) {
        size_t index = delta_ns / 1000;  // microseconds bucket (0-999)
        if (index < NUM_MICROSECONDS) {
            buckets->microseconds[index]++;
        }
    } else if (delta_ns < 1000000000ULL) {
        size_t index = delta_ns / 1000000;  // milliseconds bucket (0-999)
        if (index < NUM_MILLISECONDS) {
            buckets->milliseconds[index]++;
        }
    } else if (delta_ns < 60000000000ULL) {
        size_t index = delta_ns / 1000000000;  // seconds bucket (0-59)
        if (index < NUM_SECONDS) {
            buckets->seconds[index]++;
        }
    } else {
        buckets->seconds_overflow++;
    }
} // end method ndw_AddToLatencyTimeBucket

static long
ndw_PercentileLookup(const long *counts, size_t size, double percentile)
{
    long total = 0;
    for (size_t i = 0; i < size; i++) {
        total += counts[i];
    }
    if (total == 0) {
        return -1;
    }
    long threshold = (long)(total * percentile + 0.5); // round to nearest
    if (threshold == 0) threshold = 1;

    long cum = 0;
    for (size_t i = 0; i < size; i++) {
        cum += counts[i];
        if (cum >= threshold) {
            return (long)i;
        }
    }
    return (long)(size - 1);
} // end method ndw_PercentileLookup

void
ndw_PrintLatencyBuckets(const ndw_LatencyBuckets_T* buckets)
{
    const double percentiles[] = {0.0, 0.25, 0.5, 0.75, 0.80, 0.90, 1.0};
    const CHAR_T* labels[] = {"P0", "P25", "P50", "P75", "P80", "P90", "P100"};
    const size_t n = sizeof(percentiles) / sizeof(percentiles[0]);

    NDW_LOG("Sub-micro Underflow: %ld\n", buckets->seconds_overflow); // Overflow

    // Microseconds
    NDW_LOG("Microseconds: ");
    for (size_t i = 0; i < n; i++) {
        long slot = ndw_PercentileLookup(buckets->microseconds, NUM_MICROSECONDS, percentiles[i]);
        if (slot >= 0)
            NDW_LOG("%s<%ld> ", labels[i], slot);
        else
            NDW_LOG("%s<-> ", labels[i]);  // no data
    }
    NDW_LOG("\n");

    // Milliseconds
    NDW_LOG("Milliseconds: ");
    for (size_t i = 0; i < n; i++) {
        long slot = ndw_PercentileLookup(buckets->milliseconds, NUM_MILLISECONDS, percentiles[i]);
        if (slot >= 0)
            NDW_LOG("%s<%ld> ", labels[i], slot);
        else
            NDW_LOG("%s<-> ", labels[i]);
    }
    NDW_LOG("\n");

    // Seconds
    NDW_LOG("Seconds: ");
    for (size_t i = 0; i < n; i++) {
        long slot = ndw_PercentileLookup(buckets->seconds, NUM_SECONDS, percentiles[i]);
        if (slot >= 0)
            NDW_LOG("%s<%ld> ", labels[i], slot);
        else
            NDW_LOG("%s<-> ", labels[i]);
    }
    NDW_LOG("\n");

    NDW_LOG("Seconds Overflow: %ld\n", buckets->seconds_overflow); // Overflow
} // end method ndw_PrintLatencyBuckets

bool
ndw_atol(const CHAR_T* str, long* value)
{
    if (NULL == value) {
        NDW_LOGERR("*** FATAL ERROR: value (long*) parameter is NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    if ((NULL == str) || ('\0' == *str)) {
        return false;
    }

    CHAR_T* endptr = NULL;
    long l = strtol(str, &endptr, 10); // Base 10

    if ((endptr == str) || (!('\0' == *endptr))) {
        return false;
    }

    *value = l;
    return true;
} // end method ndw_atol

bool
ndw_atoi(const CHAR_T* str, INT_T* value)
{
    if (NULL == value) {
        NDW_LOGERR("*** FATAL ERROR: value (INT_T*) parameter is NULL!\n");
        ndw_exit(EXIT_FAILURE);
    }

    long l = *value;

    if (! ndw_atol(str, &l)) {
        return false;
    }
    else {
        *value = (INT_T) l;
        return true;
    }
} // end method ndw_atoi

static struct option ndw_program_long_options[] = {
    {"config", required_argument, 0, 'c'},
    {"maxmsgs", required_argument, 0, 'm'},
    {"bytes", required_argument, 0, 'b'},
    {"waittime", required_argument, 0, 'w'},
    {"pub", required_argument, 0, 'p'},
    {"sub", required_argument, 0, 's'},
    {"resreq", required_argument, 0, 't'},
    {"valgrind", required_argument, 0, 'v'},
    {0, 0, 0, 0}
};

static NDW_TestArgs_T ndw_test_args = {
    .num_options_given = 0,
    .num_options_valid = 0,
    .config_file = "Registry.JSON",
    .max_msgs = 6,
    .bytes_size = 512,
    .wait_time_seconds = 7,
    .resreq_time_msecs = 500,
    .is_pub = true,
    .is_sub = true,
    .run_valgrind = false
};


NDW_TestArgs_T*
ndw_ParseProgramOptions(INT_T argc, CHAR_T** argv)
{
    int opt;
    int index = -1;
    long wt;

    while ((opt = getopt_long(argc, argv, "c:m:b:w:p:s:t:v:", ndw_program_long_options, &index)) != -1) {
        printf("OPT = <%c>\n", ((char) opt));
        switch (opt) {
            case 'c':
                ++ndw_test_args.num_options_given;
                ndw_test_args.config_file = optarg;
                ++ndw_test_args.num_options_valid;
                break;
            case 'm':
                ++ndw_test_args.num_options_given;
                long total_msgs = 1;
                if (ndw_atol(optarg, &total_msgs) && (total_msgs > 0)) {
                    ndw_test_args.max_msgs = (INT_T) total_msgs;
                    ++ndw_test_args.num_options_valid;
                }
                break;
            case 'b':
                ++ndw_test_args.num_options_given;
                long message_bytes = 1;
                if (ndw_atol(optarg, &message_bytes) && (message_bytes > 0)) {
                    ndw_test_args.bytes_size = (INT_T) message_bytes;
                    ++ndw_test_args.num_options_valid;
                }
                break;
            case 'w':
                ++ndw_test_args.num_options_given;
                wt = 1;
                if (ndw_atol(optarg, &wt) && (wt > 0)) {
                    ndw_test_args.wait_time_seconds = (INT_T) wt;
                    ++ndw_test_args.num_options_valid;
                }
                break;
            case 'p':
                ++ndw_test_args.num_options_given;
                if (! NDW_ISNULLCHARPTR(optarg)) {
                    ndw_test_args.is_pub = (('Y' == *optarg) || ('y' == *optarg));
                    ++ndw_test_args.num_options_valid;
                }
                break;
            case 's':
                ++ndw_test_args.num_options_given;
                if (! NDW_ISNULLCHARPTR(optarg)) {
                    ndw_test_args.is_sub = (('Y' == *optarg) || ('y' == *optarg));
                    ++ndw_test_args.num_options_valid;
                }
                break;
            case 't':
                ++ndw_test_args.num_options_given;
                wt = 1;
                if (ndw_atol(optarg, &wt) && (wt > 0)) {
                    ndw_test_args.resreq_time_msecs = (INT_T) wt;
                    ++ndw_test_args.num_options_valid;
                }
                break;
            case 'v':
                ++ndw_test_args.num_options_given;
                if (! NDW_ISNULLCHARPTR(optarg)) {
                    ndw_test_args.run_valgrind = (('Y' == *optarg) || ('y' == *optarg));
                    ++ndw_test_args.num_options_valid;
                }
                break;
            default:
                break;
        }
    }

    printf("\nBEGIN: Program Arguments:\n");
    printf("num_options_given<%d>\nnum_options_valid<%d>\n\n-c config_file<%s>\n"
            "-m max_msgs<%d>\n-b bytes_size<%d>\n-w wait_time_seconds<%d>\n-t timeout_msec_response_req<%d>\n"
            "-p is_pub<%s>\n-s is_sub<%s>\n-v run_valgrind<%s>\n",
            ndw_test_args.num_options_given, ndw_test_args.num_options_valid,
            (NDW_ISNULLCHARPTR(ndw_test_args.config_file)) ? "??" : ndw_test_args.config_file,
            ndw_test_args.max_msgs, ndw_test_args.bytes_size,
            ndw_test_args.wait_time_seconds, ndw_test_args.resreq_time_msecs,
            ndw_test_args.is_pub ? "Y" : "n",
            ndw_test_args.is_sub ? "Y" : "n",
            ndw_test_args.run_valgrind ? "Y" : "n");
    printf("\nEND: Program Arguments:\n\n");

    if (ndw_test_args.run_valgrind) {
        printf("valgrind ");
    }

    printf("--c %s --m %d --b %d --w %d --t %d --p %s --s %s\n",
            (NDW_ISNULLCHARPTR(ndw_test_args.config_file)) ? "??" : ndw_test_args.config_file,
            ndw_test_args.max_msgs, ndw_test_args.bytes_size,
            ndw_test_args.wait_time_seconds, ndw_test_args.resreq_time_msecs,
            ndw_test_args.is_pub ? "Y" : "n",
            ndw_test_args.is_sub ? "Y" : "n");

    return &ndw_test_args;
} // ndw_ParseProgramOptions

void
ndw_PrintNVPairs(const CHAR_T* prefix, NDW_NVPairs_T* nvpairs)
{
    if ((NULL == nvpairs) || (NULL == nvpairs->names) || (NULL == nvpairs->values)) {
        return;
    }

    const CHAR_T* prefix_to_print = ((NULL == prefix) || ('\0' == *prefix)) ? "?" : prefix;

    CHAR_T* name;
    CHAR_T* value;
    for (INT_T i = 0; i < nvpairs->count; i++) {
        name = ((NULL == nvpairs->names[i]) || ('\0' == *(nvpairs->names[i]))) ? "??" : nvpairs->names[i];
        value = ((NULL == nvpairs->values[i]) || ('\0' == *(nvpairs->values[i]))) ? "??" : nvpairs->values[i];
        NDW_LOG("  -> NVPair[%s] [%d] --> {<%s> -> <%s>}\n", prefix_to_print, i, name, value);
            
    }
} // end method ndw_PrintNVPairs

const CHAR_T*
ndw_GetNVPairValue(const CHAR_T* name, NDW_NVPairs_T* nvpairs)
{
    if ((NULL == name) || ('\0' == *name) ||
        (NULL == nvpairs) || (nvpairs->count <= 0) || (NULL == nvpairs->names) || (NULL == nvpairs->values)) {
        return NULL;
    }

    CHAR_T* the_name;
    CHAR_T* the_value;
    for (INT_T i = 0; i < nvpairs->count; i++) {
        the_name = nvpairs->names[i];
        the_value = nvpairs->values[i];
        if ((NULL != name) && (0 == strcmp(name, the_name)) && (NULL != the_value) && ('\0' != *the_value)) {
            return the_value;
        }
    }

    return NULL;
} // end method ndw_GetNVPairValue

void
ndw_FreeNVPairs(NDW_NVPairs_T* nvpairs)
{
    if (NULL != nvpairs) {
        if (NULL != nvpairs->names)
            free(nvpairs->names);
        if (NULL != nvpairs->values)
            free(nvpairs->values);

        nvpairs->count = 0;
    }
} // end method ndw_FreeNVPairs


// NOTE: It modifies the string passed in.
// Caller has to free names and values only, but NOT contents of each.
INT_T
ndw_ParseNameValuePairs(CHAR_T *str, CHAR_T ***names, CHAR_T ***values)
{
    if ((NULL == str) || ('\0' == *str) || (NULL == names) || (NULL == values))
        return 0;

    // Count the number of pairs
    INT_T count = 1; // At least one pair
    for (CHAR_T *ptr = str; *ptr != '\0'; ptr++) {
        if (*ptr == '^') {
            count++;
        }
    }

    // Allocate memory for the arrays
    *names = (CHAR_T **)malloc((count + 1) * sizeof(CHAR_T *));
    *values = (CHAR_T **)malloc((count + 1) * sizeof(CHAR_T *));
    if (*names == NULL || *values == NULL) {
        free(*names);
        free(*values);
        return -1; // Out of memory
    }

    // Split the string Into name-value pairs
    CHAR_T *pair = strtok(str, "^");
    INT_T i = 0;
    while (pair != NULL && i < count) {
        // Split the pair Into name and value
        CHAR_T *colon = strchr(pair, '=');
        if (colon != NULL) {
            *colon = '\0'; // Null-terminate the name
            (*names)[i] = pair;
            (*values)[i] = colon + 1;
            i++;
        }

        pair = strtok(NULL, "^");
    }

    (*names)[i] = NULL;
    (*values)[i] = NULL;

    return i; // Return the number of pairs
} // end method ndw_ParseNameValuePairs

INT_T
ndw_ParseNVPairs(CHAR_T *str, NDW_NVPairs_T* nvpairs)
{
    if ((NULL == str) || ('\0' == *str) || (NULL == nvpairs))
        return 0;

    nvpairs->names = NULL;
    nvpairs->values = NULL;

    nvpairs->count =  ndw_ParseNameValuePairs(str, &(nvpairs->names), &(nvpairs->values));
    return nvpairs->count;
} // end method ndw_ParseNVPairs

//
// Time related functions.
//

CHAR_T*
ndw_GetPrintableCurrentTime()
{
    struct timespec ts;
    struct tm *timeinfo;
    CHAR_T* buffer = calloc(1, 256);

    clock_gettime(CLOCK_REALTIME, &ts);
    timeinfo = localtime(&ts.tv_sec);

    long ms = ts.tv_nsec / 1000000;
    long us = (ts.tv_nsec % 1000000) / 1000;
    long ns = ts.tv_nsec % 1000;

    sprintf(buffer, "%04d-%02d-%02d %02d:%02d:%02d.%03ld%03ld%03ld",
                    timeinfo->tm_year + 1900, timeinfo->tm_mon + 1, timeinfo->tm_mday,
                    timeinfo->tm_hour, timeinfo->tm_min, timeinfo->tm_sec,
                    ms, us, ns);

    return buffer;
} // end method ndw_GetPrintableCurrentTime

void
ndw_PrintCurrentTime()
{
    CHAR_T* p_time = ndw_GetPrintableCurrentTime();
    if (NULL != p_time) {
        NDW_LOG("Current Time = <%s>\n", p_time);
        free(p_time);
    }
} // end method

void
ndw_SleepMicros(long micros)
{
    if (micros > 0) {
        usleep(micros);
    }
} // end method ndw_SleepMicros

void
ndw_SleepMillis(long millis)
{
    ndw_SleepMicros(millis * 1000);
} // end method ndw_SleepMillis

void
ndw_SleepSeconds(long seconds)
{
    ndw_SleepMicros(seconds * 1000 * 1000);
} // end method ndw_SleepSeconds(long seconds)

long long
ndw_GetCurrentTimeMillis()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    long long l = (long long)(ts.tv_sec) * 1000 + (ts.tv_nsec / 1000000);
    return l;
} // end method ndw_GetCurrentTimeMillis

void
ndw_TimeDurationMillisStart(ndw_TimeDuration_T* duration, long long duration_value_millis)
{
    if (NULL != duration) {
        if (duration_value_millis <= 0)
            duration_value_millis = 1;

        duration->duration_millis = duration_value_millis;
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        duration->start_time = ndw_GetCurrentTimeMillis();
        duration->current_time = duration->start_time + 1; // What else you going to do?!
        duration->elapsed = 1;
        duration->expired = false;
    }
} // end method ndw_TimeDurationMillisStart

void
ndw_TimeDurationSecondsStart(ndw_TimeDuration_T* duration, long long duration_value_seconds)
{
    ndw_TimeDurationMillisStart(duration, duration_value_seconds * 1000);
} // end method ndw_TimeDurationSecondsStart

bool
ndw_TimeDurationStop(ndw_TimeDuration_T* duration)
{
    ndw_TimeDurationExpired(duration);
    return duration->expired;
} // method ndw_TimeDurationStop

bool
ndw_TimeDurationExpired(ndw_TimeDuration_T* duration)
{
    bool expired = false;
    if (NULL != duration) {
        duration->current_time = ndw_GetCurrentTimeMillis();
        duration->elapsed = duration->current_time - duration->start_time;
        if (duration->elapsed >= duration->duration_millis) {
            duration->expired = expired = true;
        }
    }

    return expired;
} // method ndw_TimeDurationExpired

void
ndw_TimeDurationSleepMillis(ndw_TimeDuration_T* duration)
{
    if (NULL == duration) {
        return;
    }

    long long sleep_time_millis = duration->duration_millis;
    if ((sleep_time_millis <= 0) || (duration->current_time <= 0))
        ndw_TimeDurationMillisStart(duration, sleep_time_millis = 1);

    ndw_SleepMillis(sleep_time_millis);
    bool b = ndw_TimeDurationExpired(duration);
    if (!b) {
        NDW_LOGERR("*** FATAL ERROR: ndw_TimeDurationExpired() should have failed. "
                    "sleep_time_millis<%lld> start_time<%lld> current_time<%lld> duration_millis<%lld>\n",
                    sleep_time_millis, duration->start_time, duration->current_time, duration->duration_millis);
    }
} // end method ndw_TimeDurationSleepMillis

ndw_Counter_T*
ndw_CreateCounter()
{
    return calloc(1, sizeof(ndw_Counter_T));
} // end method ndw_CreatCounter

long
ndw_CounterCurrentValue(ndw_Counter_T* counter)
{
    if (NULL == counter)
        return 0;

    long current_value = atomic_load(&(counter->counter));
    return current_value;
} // end method ndw_CounterCurrentValue

long
ndw_CounterUpdate(ndw_Counter_T* counter, long update_value)
{
    if (NULL != counter) {
        long l = atomic_fetch_add(&(counter->counter), update_value);
        return l;
    }

    return -1;
} // end method ndw_CounterUpdate

// Sleep for sleep_duration_millis and if counter is still less than check_value return false else true.
bool
ndw_CounterCheck(ndw_Counter_T* counter, long check_value, long long sleep_duration_millis)
{
    if (NULL == counter) {
        return false;
    }

    long current_value = atomic_load(&(counter->counter));

    if (sleep_duration_millis <= 0) {
        return (current_value >= check_value) ? true : false;
    }

    ndw_SleepMillis(sleep_duration_millis);
    long new_value = atomic_load(&(counter->counter));
    return (new_value >= check_value) ? true : false;
} // end method ndw_CounterCheck
 
// For Debug binary with DEBUG defined, call abort to get backtrace and dump
void ndw_exit(int status)
{
#if defined(DEBUG)
    abort();
#else
    const CHAR_T* generate_ndw_dump = getenv("NDW_DUMP_ON_ABORT");
    if (generate_ndw_dump) {
        abort();
    }
#endif

    exit(status);
}
