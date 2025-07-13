package main

/*
#include "ndw_types.h"
#include "AbstractMessaging.h"
#include "RegistryData.h"
#include "MsgHeaders.h"

#include <stdint.h>
#include <stdlib.h>

// Add this typedef here so cgo knows the type
typedef void (*ndw_GOAsyncMsgHandler)(ndw_Topic_T*);

// Declare the Go handler function to be exported
extern void ndwGoMessageHandler(ndw_Topic_T*);

// Declare the setter function
extern void ndw_SetGoMessageHandler(ndw_GOAsyncMsgHandler handler);
*/
import "C"

import (
    "fmt"
    "os"
    "strconv"
    "runtime"
    "time"
    "unsafe"
)

const defaultPublishCount = 6
const defaultQueueSize = 10000000
const defaultTimeoutSeconds = 7
const defaultMsgSize = 512

var (
    topicQueue      chan receivedMsg
    globalSequencer int64 = 0
    exitChan        = make(chan struct{})
    publishCount    = defaultPublishCount
    msgSizeBytes    = defaultMsgSize
    totalSent       int64 = 0
    totalReceived   int64 = 0
)

type receivedMsg struct {
    sequence      int64
    topicID       int32
    topicName     string
    msgBody       []byte
    msgSize       int32
    header        []byte
    headerSize    int32
    headerID      uint8
    headerSizeVal uint8
    vendorID      uint8
    timestamp     string
}

//export ndwGoMessageHandler
func ndwGoMessageHandler(topic *C.ndw_Topic_T) {
    runtime.LockOSThread()
    defer runtime.UnlockOSThread()

    totalReceived++

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
    topicQueue <- receivedMsg{
        sequence:      totalReceived,
        topicID:       topicID,
        topicName:     topicName,
        msgBody:       msg,
        msgSize:       msgSize,
        header:        hdr,
        headerSize:    int32(len(hdr)),
        headerID:      headerID,
        headerSizeVal: headerSizeVal,
        vendorID:      vendorID,
        timestamp:     timestamp,
    }
}

func startTopicProcessor(timeout time.Duration) {
    go func() {
        timer := time.NewTimer(timeout)
        defer timer.Stop()
        for {
            select {
            case msg := <-topicQueue:
                if msg.sequence < 100 || msg.sequence%10000 == 0 {
                    fmt.Printf("<<< Received Message[%d] [%s] Topic ID: %d, Topic Name: %s, Message Size: %d, Message Body: %s, HeaderID: %d, HeaderSize: %d, VendorID: %d\n",
                        msg.sequence, msg.timestamp, msg.topicID, msg.topicName, msg.msgSize, string(msg.msgBody), msg.headerID, msg.headerSizeVal, msg.vendorID)
                }
                timer.Reset(timeout)
            case <-timer.C:
                fmt.Println("No messages received within timeout. Exiting.")
                close(exitChan)
                return
            }
        }
    }()
}

func initNDW() error {
    C.ndw_SetGoMessageHandler((C.ndw_GOAsyncMsgHandler)(C.ndwGoMessageHandler))

    if ret := C.ndw_Init(); ret != 0 {
        return fmt.Errorf("ndw_Init failed with code %d", int(ret))
    }
    return nil
}

func shutdownNDW() {
    fmt.Println("Calling ndw_Shutdown...")
    C.ndw_Shutdown()
    fmt.Println("ndw_Shutdown complete.")
    fmt.Printf("TOTAL MESSAGES SENT: %d\n", totalSent)
    fmt.Printf("TOTAL MESSAGES RECEIVED: %d\n", totalReceived)
}

func connectNDW(domain, connection string) error {
    cDomain := C.CString(domain)
    cConn := C.CString(connection)
    defer C.free(unsafe.Pointer(cDomain))
    defer C.free(unsafe.Pointer(cConn))

    ret := C.ndw_Connect(cDomain, cConn)
    if ret != 0 {
        return fmt.Errorf("ndw_Connect failed with code %d", int(ret))
    }
    return nil
}

func subscribeAsync(topic *C.ndw_Topic_T) error {
    ret := C.ndw_SubscribeAsync(topic)
    if ret != 0 {
        return fmt.Errorf("SubscribeAsync failed: %d", int(ret))
    }
    return nil
}

func getConnectionByName(domain *C.ndw_Domain_T, name string) *C.ndw_Connection_T {
    cName := C.CString(name)
    defer C.free(unsafe.Pointer(cName))
    return C.ndw_GetConnectionByNameFromDomain(domain, cName)
}

func getTopicByName(conn *C.ndw_Connection_T, topicName string) *C.ndw_Topic_T {
    cName := C.CString(topicName)
    defer C.free(unsafe.Pointer(cName))
    return C.ndw_GetTopicByNameFromConnection(conn, cName)
}

type preparedMessage struct {
    jsonPtr unsafe.Pointer
    success bool
}

func prepareJSONMessage(topic *C.ndw_Topic_T, baseMessage string, sequencerNumber int64, targetSize, headerID, encodingFormat int32) (*preparedMessage, error) {
    cBaseMessage := C.CString(baseMessage)
    defer C.free(unsafe.Pointer(cBaseMessage))

    var cMsgSize C.INT_T
    jsonStrPtr := C.ndw_CreateTestJsonStringofNSize(
        topic,
        cBaseMessage,
        C.LONG_T(sequencerNumber),
        C.INT_T(targetSize),
        &cMsgSize,
    )
    if jsonStrPtr == nil || cMsgSize == 0 {
        return nil, fmt.Errorf("Failed to create JSON message")
    }

    jsonBytes := C.GoBytes(unsafe.Pointer(jsonStrPtr), C.int(cMsgSize))
    msgPtr := (*C.uchar)(unsafe.Pointer(&jsonBytes[0]))
    msgSize := C.int(len(jsonBytes))

    ctx := C.ndw_CreateOutMsgCxt(topic, C.INT_T(headerID), C.INT_T(encodingFormat), msgPtr, msgSize)
    if ctx == nil {
        C.free(unsafe.Pointer(jsonStrPtr))
        return nil, fmt.Errorf("ndw_CreateOutMsgCxt returned NULL")
    }

    return &preparedMessage{jsonPtr: unsafe.Pointer(jsonStrPtr), success: true}, nil
}

func publishMsg(topic *C.ndw_Topic_T) error {
    prep, err := prepareJSONMessage(topic, "ACME.Orders Message", globalSequencer, int32(msgSizeBytes), 20, 2)
    if err != nil {
        return err
    }
    globalSequencer++
    totalSent++

    ret := C.ndw_PublishMsg()

    if totalSent < 100 || totalSent%10000 == 0 {
        debugStr := C.GoString(topic.debug_desc)
        timestamp := time.Now().Format("2006-01-02 15:04:05.000")
        fmt.Printf(">>> Sent Message: [%s] %s\n", timestamp, debugStr)
    }

    C.free(prep.jsonPtr)

    if ret != 0 {
        return fmt.Errorf("ndw_PublishMsg failed: %d", int(ret))
    }
    return nil
}

func main() {
    runtime.LockOSThread()
    defer runtime.UnlockOSThread()

    domain := "DomainA"
    connection := "NATSConn1"
    topicName := "ACME.Orders"

    if val := os.Getenv("NDW_QUEUE_SIZE"); val != "" {
        if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
            topicQueue = make(chan receivedMsg, parsed)
        }
    } else {
        topicQueue = make(chan receivedMsg, defaultQueueSize)
    }

    if val := os.Getenv("NUM_MSGS"); val != "" {
        if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
            publishCount = parsed
        }
    }

    if val := os.Getenv("MSG_BYTES"); val != "" {
        if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
            msgSizeBytes = parsed
        }
    }

    if err := initNDW(); err != nil {
        fmt.Println("Init error:", err)
        return
    }
    defer shutdownNDW()

    startTopicProcessor(defaultTimeoutSeconds * time.Second)

    if err := connectNDW(domain, connection); err != nil {
        fmt.Println("Connect error:", err)
        return
    }

    cDomain := C.CString(domain)
    defer C.free(unsafe.Pointer(cDomain))
    domainPtr := C.ndw_GetDomainByName(cDomain)
    if domainPtr == nil {
        fmt.Println("Could not get domain pointer")
        return
    }

    connPtr := getConnectionByName(domainPtr, connection)
    if connPtr == nil {
        fmt.Println("Could not get connection pointer")
        return
    }

    topicPtr := getTopicByName(connPtr, topicName)
    if topicPtr == nil {
        fmt.Println("Could not get topic pointer")
        return
    }

    if err := subscribeAsync(topicPtr); err != nil {
        fmt.Println("Subscribe error:", err)
        return
    }

    for i := 0; i < publishCount; i++ {
        if err := publishMsg(topicPtr); err != nil {
            fmt.Println("Publish failed:", err)
            return
        }
    }

    fmt.Println("All messages published successfully")
    <-exitChan
}

