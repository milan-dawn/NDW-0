package ndw

/*
#include "ndw_types.h"
#include "AbstractMessaging.h"
#include "RegistryData.h"
#include "MsgHeaders.h"
#include "MsgHeader_1.h"

#include <stdint.h>
#include <stdlib.h>

// Add this typedef here so cgo knows the type
typedef void (*ndw_GOAsyncMsgHandler)(ndw_Topic_T*);

// Declare the Go handler function to be exported

// Declare the setter function
extern void ndw_SetGoMessageHandler(ndw_GOAsyncMsgHandler handler);
*/
import "C"

import (
    "errors"
    "flag"
    "fmt"
    "time"
    "strings"
    "os"
    "os/signal"
    "runtime"
    "syscall"
    "unsafe"
    "sync/atomic"
    "bytes"
    "encoding/json"
)

var verboseLevel int

var NDW_ENCODING_FORMAT_NONE    = C.NDW_ENCODING_FORMAT_NONE
var NDW_ENCODING_FORMAT_STRING  = C.NDW_ENCODING_FORMAT_STRING 
var NDW_ENCODING_FORMAT_JSON    = C.NDW_ENCODING_FORMAT_JSON
var NDW_ENCODING_FORMAT_BINARY  = C.NDW_ENCODING_FORMAT_BINARY
var NDW_ENCODING_FORMAT_XML     = C.NDW_ENCODING_FORMAT_XML
var NDW_MAX_ENCODING_FORMAT     = C.NDW_MAX_ENCODING_FORMAT

var NDW_DEFAULT_MSG_HEADER      = C.NDW_MSGHEADER_1


//
// Centralized realMain in ndw package
// This is necessary as we need to fun the main function with the LockOSThread as well as ability to invoke ndw_Shutdown
// in the same main thread that invoked NDW_Init
//
func RunNDWApp(appLogic func() int) int {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer NDW_Exit()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Run user-defined logic
	exitCode := appLogic()

	// Optional: handle signal interruption
	select {
	case sig := <-sigChan:
		NDW_LOGERR("Caught signal: %s\n", sig)
		return 1
	default:
		return exitCode
	}
}


// Atomic counters
var total_messages_published int64
var total_messages_received int64

// Topic Data
type NDW_TopicData struct {
    DomainName     string
    ConnectionName string
    TopicName      string

    ToString        string

    TotalMsgsPublished int64
    TotalMsgsReceived int64

    DomainPtr     *C.ndw_Domain_T
    ConnectionPtr *C.ndw_Connection_T
    TopicPtr      *C.ndw_Topic_T
}

var TopicList []NDW_TopicData

var topicPtrToData = make(map[*C.ndw_Topic_T]*NDW_TopicData)

//
//  BEGIN: NDW API functions.
//

type AppNDWAsyncMsgCallbackType func(topicData *NDW_TopicData, msg *NDW_ReceivedMsg)
var appAsyncMsgCallback AppNDWAsyncMsgCallbackType

//export ndwGoMessageHandler
func ndwGoMessageHandler(topic *C.ndw_Topic_T) {
    runtime.LockOSThread()
    defer runtime.UnlockOSThread()

    if topic == nil {
        NDW_LOGERR("*** ERROR: NULL topic Pointer!\n")
        return
    }

    topicData, ok := topicPtrToData[topic]
    if !ok {
        NDW_LOGERR("*** ERROR: Failed to find NDW_TopicData for %s\n", C.GoString(topic.debug_desc))
        return
    }

    topicData.TotalMsgsReceived += 1
    IncrementTotalMsgsReceived()

    msg := NDW_BuildReceivedMsg(unsafe.Pointer(topic))

    //NDW_LOG("ndwGoMessageHandler: Topic = %s, MsgSize = %d TotalMsgsReceived = %d\n", topicData.TopicName, msg.MsgSize, topicData.TotalMsgsReceived)

    if appAsyncMsgCallback != nil {
        appAsyncMsgCallback(topicData, msg)
    } else {
        NDW_LOGERR("No appAsyncMsgCallback registered.")
    }
}


func NDW_RegisterGoAsyncMsgCallback(cb AppNDWAsyncMsgCallbackType) {
    appAsyncMsgCallback = cb;
}

func NDW_ThreadInit() int32 {
    ret := C.ndw_ThreadInit()
    if ret != 0 {
        fmt.Errorf("*** ERROR: ndw_ThreadInit failed with code %d", int(ret))
    }

    return int32(ret)
}

func NDW_ThreadExit() int32 {
    ret := C.ndw_ThreadExit()
    if ret != 0 {
        fmt.Errorf("*** ERROR: ndw_ThreadExit failed with code %d", int(ret))
    }

    return int32(ret)
}


func NDW_Initialize() int32 {

    C.ndw_SetGoMessageHandler((C.ndw_GOAsyncMsgHandler)(C.ndwGoMessageHandler))

    verboseLevel = int(C.ndw_verbose)

    if ret := C.ndw_Init(); ret != 0 {
        fmt.Errorf("*** ERROR: ndw_Init failed with code %d", int(ret))
        return int32(ret)
    }

    ret := PopulateTopicData()
    if ret != 0 {
        fmt.Errorf("*** ERROR: ndw_Init failed with code %d", int(ret))
        return int32(ret)
    }

    return 0
}

// NOTE: Must use defer NDWExit() when invoking this function from main.
func NDW_Exit() {

    NDW_LOG("\nCalling ndw_Shutdown...\n")
    C.ndw_Shutdown()
    fmt.Println("\nndw_Shutdown complete.\n")
    fmt.Printf("\nTOTAL MESSAGES SENT: %d\n", GetTotalMsgsPublished())
    fmt.Printf("TOTAL MESSAGES RECEIVED: %d\n", GetTotalMsgsReceived())
}

func NDW_IsConnected(domain_name string, connection_name string) bool {
    cDomain := C.CString(domain_name)
    cConn := C.CString(connection_name)
    defer C.free(unsafe.Pointer(cDomain))
    defer C.free(unsafe.Pointer(cConn))

    ret := C.ndw_IsConnected(cDomain, cConn)
    return bool(ret)
}

func NDW_IsClosed(domain_name string, connection_name string) bool {
    cDomain := C.CString(domain_name)
    cConn := C.CString(connection_name)
    defer C.free(unsafe.Pointer(cDomain))
    defer C.free(unsafe.Pointer(cConn))

    ret := C.ndw_IsClosed(cDomain, cConn)
    return bool(ret)
}

func NDW_IsDraining(domain_name string, connection_name string) bool {
    cDomain := C.CString(domain_name)
    cConn := C.CString(connection_name)
    defer C.free(unsafe.Pointer(cDomain))
    defer C.free(unsafe.Pointer(cConn))

    ret := C.ndw_IsDraining(cDomain, cConn)
    return bool(ret)
}

func NDW_Connect(domain_name string, connection_name string) int32 {
    cDomain := C.CString(domain_name)
    cConn := C.CString(connection_name)
    defer C.free(unsafe.Pointer(cDomain))
    defer C.free(unsafe.Pointer(cConn))

    ret := C.ndw_Connect(cDomain, cConn)
    if ret != 0 {
        fmt.Errorf("*** ERROR: ndw_Connect failed with code %d", int(ret))
        return int32(ret)
    }
    return 0
}

func NDW_ConnectAll() int32 {
    for _, topic := range TopicList {
        if NDW_IsConnected(topic.DomainName, topic.ConnectionName) {
            NDW_LOGX("✅ Already Connected to to <%s, %s>\n", topic.DomainName, topic.ConnectionName)
            continue;
        }

        ret := NDW_Connect(topic.DomainName, topic.ConnectionName)
        if ret != 0 {
            NDW_LOGERR("NDW_Connect failed for Domain: %s, Connection: %s\n", topic.DomainName, topic.ConnectionName)
            return -1
        }

        NDW_LOGX("✅ Connected to to <%s, %s>\n", topic.DomainName, topic.ConnectionName)
    }
    return 0
}

func NDW_SubscribeAsync(topic_data *NDW_TopicData) int32 {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return -1
    }

    ret := C.ndw_SubscribeAsync(topic_data.TopicPtr)
    if ret != 0 {
        NDW_LOGERR("*** ERROR: ndw_SubscribeAsync failed for %s\n", C.GoString(topic_data.TopicPtr.debug_desc))
        return int32(ret)
    }

    NDW_LOGX("✅ Subscribed to %s\n", topic_data.ToString)

    return 0
}

func NDW_SubscribeAsyncAll() int32 {
    for _, topic := range TopicList {
        ret := NDW_SubscribeAsync(&topic)
        if ret != 0 {
            return int32(ret)
        }
    }

    return 0;
}

func NDW_SubscribeSynchronous(topic_data *NDW_TopicData) int32 {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return -1
    }

    ret := C.ndw_SubscribeSynchronous(topic_data.TopicPtr)
    if ret != 0 {
        NDW_LOGERR("*** ERROR: ndw_SubscribeSynchronous failed for %s\n", C.GoString(topic_data.TopicPtr.debug_desc))
        return int32(ret)
    }

    NDW_LOGX("✅ SubscribedSynchronous to to <%s, %s>\n", topic_data.ToString)

    return 0
}

func NDW_Unsubscribe(topic *C.ndw_Topic_T) int32 {
    if topic == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL!\n")
        return -1
    }

    ret := C.ndw_Unsubscribe(topic)
    if ret != 0 {
        NDW_LOGERR("*** ERROR: ndw_UnsubscribeAsync failed for %s\n", C.GoString(topic.debug_desc))
        return int32(ret)
    }

    return 0
}

func NDW_UnsubscribeAll() int32 {
    for _, topic := range TopicList {
        ret := NDW_Unsubscribe(topic.TopicPtr)
        if ret != 0 {
            return int32(ret)
        }
    }

    return 0;
}

func NDW_SynchronousPollForMsg(topic_data *NDW_TopicData, timeout_us int64, dropped_messages *int64) int32 {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return -1
    }

    timeout_ms := C.LONG_T(timeout_us)
    if timeout_ms >= 1000 {
        timeout_ms = timeout_ms / 1000
    }

    var droppedMessages C.LONG_T

    ret := C.ndw_SynchronousPollForMsg(topic_data.TopicPtr, timeout_ms, &droppedMessages)

    *dropped_messages = int64(droppedMessages);

    if ret == 1 {
        // One message obtained.
        IncrementTotalMsgsReceived()
        return int32(ret)
    } else if ret == 0 {
        // Timeout
        return 0
    }

    NDW_LOGERR("*** ERROR: C.ndw_SynchronousPollForMsg() failed for %s\n", C.GoString(topic_data.TopicPtr.debug_desc))
    return int32(ret)
}

func NDW_PollAsyncQeueue(topic_data *NDW_TopicData, timeout_us int64) int32 {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return -1
    }

    timeout := C.LONG_T(timeout_us)

    ret := C.ndw_PollAsyncQueue(topic_data.TopicPtr, timeout);

    if ret == 1 {
        // One message obtained.
        IncrementTotalMsgsReceived()
        return int32(ret)
    } else if ret == 0 {
        // Timeout
        return 0
    }

    NDW_LOGERR("*** ERROR: C.ndw_PollAsyncQueue() failed for %s\n", C.GoString(topic_data.TopicPtr.debug_desc))

    return int32(ret)
}

func NDW_CommitLastMsg(topic_data *NDW_TopicData) int32 {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return -1
    }

    ret := C.ndw_CommitLastMsg(topic_data.TopicPtr)
    if ret != 0 {
        NDW_LOGERR("*** ERROR: C.ndw_CommitLastMsg() failed for %s\n", C.GoString(topic_data.TopicPtr.debug_desc))
        return int32(ret)
    }

    return 0
}

func CreateOutMsgCxt(topic_data *NDW_TopicData, payload []byte, msg_header_id int, encoding_format int) *C.ndw_OutMsgCxt_T {
    if len(payload) == 0 {
        return nil
    }

    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return nil
    }

    // Allocate a temporary C buffer and copy the Go bytes
    cPtr := C.CBytes(payload)
    defer C.free(cPtr) // Safe to free immediately after the call below

    outCtx := C.ndw_CreateOutMsgCxt(
        topic_data.TopicPtr,
        C.INT_T(msg_header_id),
        C.INT_T(encoding_format),
        (*C.uchar)(cPtr),
        C.INT_T(len(payload)),
    )

    return outCtx
}

//func CreateOutMsgCxtForJSONMessage(topic *C.ndw_Topic_T, json_msg string, msg_header_id int, encoding_format int) *C.ndw_OutMsgCxt_T {
//   payload := []byte(json_msg)
//  return CreateOutMsgCxt(topic, payload, msg_header_id, encoding_format)
//}

func NDW_PublishMsg(topic_data *NDW_TopicData) int32 {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return -1
    }

    ret := C.ndw_PublishMsg()
    if ret != 0 {
        NDW_LOGERR("*** ERROR*** Publish Message failed on %s\n", C.GoString(topic_data.TopicPtr.debug_desc))
        return int32(ret)
    }

    topic_data.TotalMsgsPublished += 1
    IncrementTotalMsgsPublished()
    return 0
}

func NDW_GetTopicDebugString(topic_data *NDW_TopicData) string {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return ""
    }

    return C.GoString(topic_data.TopicPtr.debug_desc)
}

func NDW_GetLastReceivedMsg(topic_data *NDW_TopicData) *NDW_ReceivedMsg {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return nil
    }

    msg := NDW_BuildReceivedMsg(unsafe.Pointer(topic_data.TopicPtr))
    return msg
}

func NDW_GetMsgCommitAckCounters(topic_data *NDW_TopicData, commit_counter *int64, ack_counter *int64, failed_ack_counter *int64) {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return
    }

    *commit_counter = int64(topic_data.TopicPtr.msgs_commit_count)
    *ack_counter = int64(topic_data.TopicPtr.msgs_ack_count)
    *failed_ack_counter = int64(topic_data.TopicPtr.msgs_failed_ack_count)
}

func NDW_SetTopicSequenceNumber(topic_data *NDW_TopicData, sequence_number int64) {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return
    }

    topic_data.TopicPtr.sequence_number = C.LONG_T(sequence_number)
}

func NDW_GetTopicSequenceNumber(topic_data *NDW_TopicData) int64 {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return -1
    }

    return int64(topic_data.TopicPtr.sequence_number)
}

func NDW_IncrementTopicSequenceNumber(topic_data *NDW_TopicData) int64 {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return -1
    }

    sequence_number := int64(topic_data.TopicPtr.sequence_number)
    sequence_number += 1
    topic_data.TopicPtr.sequence_number = C.LONG_T(sequence_number)
    return sequence_number
}

func NDW_GetPubKey(topic_data *NDW_TopicData) string {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return ""
    }

    if topic_data.TopicPtr.pub_key == nil {
        return ""
    } else {
        pub_key := C.GoString(topic_data.TopicPtr.pub_key)
        return pub_key
    }
}

func NDW_GetSubKey(topic_data *NDW_TopicData) string {
    if topic_data.TopicPtr == nil {
        NDW_LOGERR("*** ERROR *** topic_data.TopicPtr is NULL for %s\n", topic_data.TopicName)
        return ""
    }

    if topic_data.TopicPtr.sub_key == nil {
        return ""
    } else {
        sub_key := C.GoString(topic_data.TopicPtr.sub_key)
        return sub_key
    }
}

//
// BEGIN: Simple functions to get Domain, Connections and Topics using names.
//

func getDomainByName(domainName string) *C.ndw_Domain_T {
    cName := C.CString(domainName)
    defer C.free(unsafe.Pointer(cName))
    domain_ptr := C.ndw_GetDomainByName(cName);
    if domain_ptr == nil {
        NDW_LOGERR("Failed to get Domain Pointer for Domain Name: %s\n", domainName)
        return nil
    }

    return domain_ptr
}

func getConnectionByName(domain *C.ndw_Domain_T, connectionName string) *C.ndw_Connection_T {
    if domain == nil {
        NDW_LOGERR("nil C.ndw_Domain_T Pointer as argument for Connection Name: %s\n", connectionName)
        return nil
    }

    cName := C.CString(connectionName)
    defer C.free(unsafe.Pointer(cName))
    conn_ptr := C.ndw_GetConnectionByNameFromDomain(domain, cName)
    if conn_ptr == nil {
        NDW_LOGERR("Failed to get Connection Pointer for Connection Name: %s\n", connectionName)
        return nil
    }

    return conn_ptr
}

func getTopicByName(conn *C.ndw_Connection_T, topicName string) *C.ndw_Topic_T {
    cName := C.CString(topicName)
    defer C.free(unsafe.Pointer(cName))
    topic_ptr := C.ndw_GetTopicByNameFromConnection(conn, cName)
    if topic_ptr == nil {
        NDW_LOGERR("Failed to get Topic Pointer for topicName: %s\n", topicName)
        return nil
    }

    return topic_ptr
}


func getTopicFromPath(path string) *NDW_TopicData {
    if path == "" {
        NDW_LOGERR("nil for path: %s\n", path)
        return nil;
    }

    tokens, num_tokens := SplitStringToTokens(path, "^")
    if num_tokens != 3 {
        NDW_LOGERR("Invalid argument: path: %s\n", path)
        return nil;
    }

    domain_name := tokens[0]
    connection_name := tokens[1]
    topic_name := tokens[2]

    if domain_name == "" {
        NDW_LOGERR("Invalid argument: Failed to find Domain Name from path: %s\n", path)
        return nil;
    }

    if connection_name == "" {
        NDW_LOGERR("Invalid argument: Failed to find Connection Name from path: %s\n", path)
        return nil;
    }

    if topic_name == "" {
        NDW_LOGERR("Invalid argument: Failed to find Topic Name from path: %s\n", path)
        return nil;
    }

    domain_ptr := getDomainByName(domain_name)
    if domain_ptr == nil {
        NDW_LOGERR("Invalid argument: Failed to find Domain Pointer from path: %s\n", path)
        return nil;
    }


    connection_ptr := getConnectionByName(domain_ptr, connection_name)
    if connection_ptr == nil {
        NDW_LOGERR("Invalid argument: Failed to find Connection Pointer from path: %s\n", path)
        return nil;
    }

    cTopic_Name := C.CString(topic_name)
    defer C.free(unsafe.Pointer(cTopic_Name))
    topic_ptr := getTopicByName(connection_ptr, topic_name)
    if topic_ptr == nil {
        NDW_LOGERR("Invalid argument: Failed to find Topic Pointer from path: %s\n", path)
        return nil;
    }

    return &NDW_TopicData {
        DomainName:     domain_name,
        ConnectionName: connection_name,
        TopicName:      topic_name,


        DomainPtr:      domain_ptr,
        ConnectionPtr:  connection_ptr,
        TopicPtr:       topic_ptr,
         
    }
}

func PopulateTopicData() int {
    NDW_LOGX("BEGIN: PopulateTopicData: len(TopicList) = %d...\n", len(TopicList))
    num_populated := 0

    for i := range TopicList {
        topic := &TopicList[i]

        topic.TotalMsgsPublished = 0
        topic.TotalMsgsReceived = 0

        topic.DomainPtr = getDomainByName(topic.DomainName)
        if topic.DomainPtr == nil {
            NDW_LOGERR("*** ERROR: TopicConfig[%d]: domain lookup failed for '%s'\n", i, topic.DomainName)
            return -1
        }

        topic.ConnectionPtr = getConnectionByName(topic.DomainPtr, topic.ConnectionName)
        if topic.ConnectionPtr == nil {
            NDW_LOGERR("*** ERROR: TopicConfig[%d]: connection lookup failed for '%s'\n", i, topic.ConnectionName)
            return -1
        }

        topic.TopicPtr = getTopicByName(topic.ConnectionPtr, topic.TopicName)
        if topic.TopicPtr == nil {
            NDW_LOGERR("*** ERROR: TopicConfig[%d]: topic lookup failed for '%s'\n", i, topic.TopicName)
            return -1
        }

        //topic.ToString = topic.DomainName + "^" + topic.ConnectionName + "^" + topic.TopicName
        topic.ToString = C.GoString(topic.TopicPtr.debug_desc)

        topicPtrToData[topic.TopicPtr] = topic;

        NDW_LOG("✅ Populated TopicConfig[%d]: %s / %s / %s\n",
            i, topic.DomainName, topic.ConnectionName, topic.TopicName)

        num_populated += 1
    }

    NDW_LOGX("END: PopulateTopicData: num_populated = %d\n", num_populated)
    return 0
}


// BEGIN: Received message Manipulation functions.

type NDW_ReceivedMsg struct {
    TopicPtr      *C.ndw_Topic_T
    TopicID       int32
    TopicName     string
    Sequence      int64
    MsgBody       []byte
    MsgSize       int32
    Header        []byte
    HeaderSize    int32
    HeaderID      uint8
    HeaderSizeVal uint8
    VendorID      uint8
    Timestamp     string
}

func NDW_BuildReceivedMsg(topic_ptr unsafe.Pointer) *NDW_ReceivedMsg {
    topic := (*C.ndw_Topic_T)(topic_ptr)

    topicID := int32(topic.topic_unique_id)
    topicName := C.GoString(topic.topic_unique_name)
    msgSize := int32(topic.last_msg_received_size)

    var msg []byte
    if topic.last_msg_received != nil && msgSize > 0 {
        msg = C.GoBytes(unsafe.Pointer(topic.last_msg_received), C.int(msgSize))
    }

    var hdr []byte
    var headerID, headerSizeVal, vendorID uint8
    if topic.last_msg_header_received != nil {
        rawHdr := C.GoBytes(unsafe.Pointer(topic.last_msg_header_received), 2)
        if len(rawHdr) >= 2 {
            hdrSize := int(rawHdr[1])
            hdr = C.GoBytes(unsafe.Pointer(topic.last_msg_header_received), C.int(hdrSize))
            if len(hdr) >= 3 {
                headerID = hdr[0]
                headerSizeVal = hdr[1]
                vendorID = hdr[2]
            }
        }
    }

    timestamp := time.Now().Format("2006-01-02 15:04:05.000")

    return &NDW_ReceivedMsg{
        TopicPtr:      topic,
        TopicID:       topicID,
        TopicName:     topicName,
        Sequence:      int64(topic.total_received_msgs),
        MsgBody:       msg,
        MsgSize:       msgSize,
        Header:        hdr,
        HeaderSize:    int32(len(hdr)),
        HeaderID:      headerID,
        HeaderSizeVal: headerSizeVal,
        VendorID:      vendorID,
        Timestamp:     timestamp,
    }
}

func NDW_PrintReceivedMsg(msg *NDW_ReceivedMsg) {
    NDW_LOGX("<<< Message Received:")
    NDW_LOG("  Sequence       : %d", msg.Sequence)
    NDW_LOG("  Topic ID       : %d", msg.TopicID)
    NDW_LOG("  Topic Name     : %s", msg.TopicName)
    NDW_LOG("  Msg Size       : %d", msg.MsgSize)
    NDW_LOG("  Header Size    : %d", msg.HeaderSize)
    NDW_LOG("  Header ID      : %d", msg.HeaderID)
    NDW_LOG("  Header SizeVal : %d", msg.HeaderSizeVal)
    NDW_LOG("  Vendor ID      : %d", msg.VendorID)
    NDW_LOG("  Timestamp      : %s", msg.Timestamp)

    if len(msg.MsgBody) > 0 {
        NDW_LOG("  Msg Body       : %s", string(msg.MsgBody))
    }
    if len(msg.Header) > 0 {
        NDW_LOG("  Header (raw)   : %v", msg.Header)
    }

    NDW_LOG("ON:  %s\n", C.GoString(msg.TopicPtr.debug_desc))
}

// END: Received message Manipulation functions.

//
// END: Simple functions to get Domain, Connections and Topics using names.
//

//
//  END: NDW API functions.
//

//
//  BEGIN: Utility functions.
//
func CreateTopicList(num_topics int) {
    TopicList = make([]NDW_TopicData, num_topics)
}

func GetTopicData(index int) *NDW_TopicData {
    return &TopicList[index]
}

func InitializeTopicData(index int, domain_name string, connection_name string, topic_name string) {
    TopicList[index].DomainName = domain_name
    TopicList[index].ConnectionName = connection_name
    TopicList[index].TopicName = topic_name
}

func CreateTestJSONMessageOfSize(topic *NDW_TopicData, message string, sequenceNumber int64, targetSize int) (string, int) {
	obj := make(map[string]interface{})

    obj["Topic"] = topic.TopicName
	obj["message"] = message
	obj["sequencer_number"] = sequenceNumber

	// Encode initial object to get current size
	jsonBytes, _ := json.Marshal(obj)
	currentSize := len(jsonBytes)

	i := 0
	for currentSize < targetSize {
		key := fmt.Sprintf("key_%d", i)
		// Create a 99-byte value string
		value := strings.Repeat("x", 99)
		obj[key] = value

		jsonBytes, _ = json.Marshal(obj)
		currentSize = len(jsonBytes)
		i++
	}

	finalJSON := string(jsonBytes)
	return finalJSON, currentSize
}


type TestJsonMessage struct {
    jsonPtr unsafe.Pointer
    success bool
}

func PrettyPrintJSON(prefix string, jsonStr string, suffix string) {
	var prettyBuf bytes.Buffer
	err := json.Indent(&prettyBuf, []byte(jsonStr), "", "  ")
	if err != nil {
		NDW_LOGERR("Failed to pretty print JSON: %v", err)
		return
	}

	NDW_LOG("%s %s %s\n", prefix, prettyBuf.String(), suffix)
}


func NDW_SleepForMessages(unit_sleep_sec int, max_expiry_time int) {
	lastReceivedCount := GetTotalMsgsReceived()
	lastHeardTime := time.Now()

	for {
		time.Sleep(time.Duration(unit_sleep_sec) * time.Second)

		currPublished := GetTotalMsgsPublished()
		currReceived := GetTotalMsgsReceived()

		NDW_LOG("💤 Published: %d | Received: %d\n", currPublished, currReceived)

		if currReceived > lastReceivedCount {
			lastReceivedCount = currReceived
			lastHeardTime = time.Now()
			continue
		}

		elapsed := time.Since(lastHeardTime).Seconds()
		if int(elapsed) >= max_expiry_time {
			NDW_LOG("⏳ No new messages in last %d seconds. Expired.\n", max_expiry_time)
			break
		}
	}
}

func IsNilOrEmpty(s *string) bool {
    return s == nil || *s == ""
}

// PrintCurrentTime prints the time in format HH:MM:SS:MS:US:NS
func PrintCurrentTime() {
    now := time.Now()
    hour := now.Hour()
    minute := now.Minute()
    second := now.Second()
    nanosec := now.Nanosecond()

    millisec := nanosec / 1_000_000
    microsec := (nanosec / 1_000) % 1_000
    nanorem := nanosec % 1_000

    NDW_LOG("%02d:%02d:%02d:%03d:%03d:%03d\n", 
        hour, minute, second, millisec, microsec, nanorem)
    fmt.Printf("%02d:%02d:%02d:%03d:%03d:%03d\n",
        hour, minute, second, millisec, microsec, nanorem)
}

func GetCurrentTimeString() string {
    now := time.Now()
    hour := now.Hour()
    minute := now.Minute()
    second := now.Second()
    nanosec := now.Nanosecond()

    millisec := nanosec / 1_000_000
    microsec := (nanosec / 1_000) % 1_000
    nanorem := nanosec % 1_000

    return fmt.Sprintf("%02d:%02d:%02d:%03d:%03d:%03d",
        hour, minute, second, millisec, microsec, nanorem)
}

func NDW_LOG(format string, args ...any) {
    ts := GetCurrentTimeString()
    fmt.Fprintf(os.Stdout, "[%s]: "+format, append([]any{ts}, args...)...)
}

func NDW_LOGX(format string, args ...any) {
    pc, file, line, ok := runtime.Caller(1)
    funcName := "unknown"
    if ok {
        fn := runtime.FuncForPC(pc)
        if fn != nil {
            funcName = fn.Name()
        }
    }

    ts := GetCurrentTimeString()

    fmt.Fprintf(os.Stdout, "[%s] %s:%d (%s): "+format+"\n",
        append([]any{ts, file, line, funcName}, args...)...)
}

func NDW_LOGERR(format string, args ...any) {
    pc, file, line, ok := runtime.Caller(1)
    funcName := "unknown"
    if ok {
        fn := runtime.FuncForPC(pc)
        if fn != nil {
            funcName = fn.Name()
        }
    }

    ts := GetCurrentTimeString()

    fmt.Fprintf(os.Stderr, "[%s] %s:%d (%s): "+format+"\n",
        append([]any{ts, file, line, funcName}, args...)...)
}


// SplitStringToTokens splits a string into tokens using the given separator.
// Example: SplitStringToTokens("one^two^three", "^") → ["one", "two", "three"]
func SplitStringToTokens(input string, sep string) ([]string, int) {
    if sep == "" {
        return []string{input}, 1 // fallback: no separator
    }
    tokens := strings.Split(input, sep)
    return tokens, len(tokens)
}

// ParseMessageArgs parses -m <count>, -b <size>, -w <wait_time_in_seconds> and -t <topics> from the given args.
// Returns (messageCount, messageSize, []NDW_TopicData, error).
func ParseMessageArgs(args []string) (int, int, int, []NDW_TopicData, error) {
    fs := flag.NewFlagSet("msgflags", flag.ContinueOnError)

    m := fs.Int("m", 1, "Message count")
    b := fs.Int("b", 1024, "Message size")
    w := fs.Int("w", 6, "Wait time in seconds")
    t := fs.String("t", "", "Topic list (pipe-separated)")

    fs.SetOutput(nil)

    if err := fs.Parse(args); err != nil {
        NDW_LOGERR("argument parsing failed: %v", err)
        return 0, 0, 0, nil, err
    }

    if *m < 1 || *b < 1 {
        err := errors.New("message count and size must be positive integers")
        NDW_LOGERR("%v", err)
        return 0, 0, 0, nil, err
    }

    var topicConfigs []NDW_TopicData

    if *t != "" {
        topicStrings, _ := SplitStringToTokens(*t, "|")

        for _, topicStr := range topicStrings {
            parts, count := SplitStringToTokens(topicStr, "^")
            if count != 3 {
                NDW_LOGERR("Note: Ingoring invalid topic format: %s", topicStr)
                continue
            }

            topicConfigs = append(topicConfigs, NDW_TopicData{
                DomainName:     parts[0],
                ConnectionName: parts[1],
                TopicName:      parts[2],
            })
        }
    }

    TopicList = topicConfigs

    return *m, *b, *w, topicConfigs, nil
}

// IncrementTotalMsgsPublished increments and returns the new value
func IncrementTotalMsgsPublished() int64 {
    return atomic.AddInt64(&total_messages_published, 1)
}

// GetTotalMsgsPublished returns the current value atomically
func GetTotalMsgsPublished() int64 {
    return atomic.LoadInt64(&total_messages_published)
}

// IncrementTotalMsgsReceived increments and returns the new value
func IncrementTotalMsgsReceived() int64 {
    return atomic.AddInt64(&total_messages_received, 1)
}

// GetTotalMsgsReceived returns the current value atomically
func GetTotalMsgsReceived() int64 {
    return atomic.LoadInt64(&total_messages_received)
}

type NDW_TimeDuration struct {
	StartTime time.Time
	EndTime   time.Time

	Hours        int
	Minutes      int
	Seconds      int
	Milliseconds int
	Microseconds int
	Nanoseconds  int
}

func NDW_StartTimeDuration() *NDW_TimeDuration {
	now := time.Now()
	return &NDW_TimeDuration{
		StartTime:    now,
		EndTime:      now,
		Hours:        0,
		Minutes:      0,
		Seconds:      0,
		Milliseconds: 0,
		Microseconds: 0,
		Nanoseconds:  0,
	}
}

func NDW_EndTimeDuration(td *NDW_TimeDuration) {
	td.EndTime = time.Now()
	diff := td.EndTime.Sub(td.StartTime)

	totalNano := diff.Nanoseconds()

	td.Hours        = int(totalNano / int64(time.Hour))
	td.Minutes      = int((totalNano % int64(time.Hour)) / int64(time.Minute))
	td.Seconds      = int((totalNano % int64(time.Minute)) / int64(time.Second))
	td.Milliseconds = int((totalNano % int64(time.Second)) / int64(time.Millisecond))
	td.Microseconds = int((totalNano % int64(time.Millisecond)) / int64(time.Microsecond))
	td.Nanoseconds  = int(totalNano % int64(time.Microsecond))
}

func NDW_PrintTimeDuration(td *NDW_TimeDuration) {
	fmt.Printf("🕒 Start Time: %s\n", td.StartTime.Format("2006-01-02 15:04:05.000000000"))
	fmt.Printf("🕒 End Time  : %s\n", td.EndTime.Format("2006-01-02 15:04:05.000000000"))
	fmt.Printf("⏱️  Duration : %02dh:%02dm:%02ds:%03dms:%03dus:%03dns\n",
		td.Hours, td.Minutes, td.Seconds,
		td.Milliseconds, td.Microseconds, td.Nanoseconds)
}


//
//  END: Utility functions.
//

