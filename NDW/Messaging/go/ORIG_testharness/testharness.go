package main

/*
#include "ndw_types.h"
#include "AbstractMessaging.h"
#include "RegistryData.h"
#include "MsgHeaders.h"
#include "MsgHeader_1.h"

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

// Add this typedef here so cgo knows the type
typedef void (*ndw_GOAsyncMsgHandler)(ndw_Topic_T*);

// Declare the Go handler function to be exported

// Declare the setter function

*/
import "C"

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
	"unsafe"
)

type (
	PublishFunction    func(topic *C.ndw_Topic_T) C.INT_T
	SubscribeFunction  func(topic *C.ndw_Topic_T) C.INT_T
	PollFunction       func(topic *C.ndw_Topic_T, timeoutUs C.LONG_T) C.INT_T
	PollCommitFunction func(topic *C.ndw_Topic_T) C.INT_T
)

type PublishFunctionPtrData struct {
	Function     PublishFunction
	FunctionName string
}

type SubscribeFunctionPtrData struct {
	Function     SubscribeFunction
	FunctionName string
}

type PollFunctionPtrData struct {
	Function     PollFunction
	FunctionName string
}

type PollCommitFunctionPtrData struct {
	Function     PollCommitFunction
	FunctionName string
}

type PublishType int

const (
	NATS PublishType = iota
	JS
)

type SubscribeType int

const (
	Unknown SubscribeType = iota
	NATSAsync
	NATSPoll
	JSPush
	JSPull
	QAsync
)

type AppTopic struct {
	Topic                 *C.ndw_Topic_T
	TopicNumber           int
	LogicalUniqueName     [256]byte
	Domain                [256]byte
	Connection            [256]byte
	TopicName             [256]byte
	Enabled               bool
	Disabled              bool
	PublishEnabled        bool
	SubscribeEnabled      bool
	Publish               bool
	Subscribe             bool
	PollTimeoutUs         int
	PublishType           PublishType
	SubscribeType         SubscribeType
	MsgsPublishedCount    int64
	MsgsReceivedCount     int64
	MsgsCommitCount       int64
	MsgsAckCount          int64
	MsgsFailedAckCount    int64
	FunctionPublishData   PublishFunctionPtrData
	FunctionSubscribeData SubscribeFunctionPtrData
	IsPollActivity        bool
	FunctionPollData      PollFunctionPtrData
	FunctionCommitData    PollCommitFunctionPtrData
}

type PublisherArgs struct {
	ThreadID C.pthread_t
	NumMsgs  int
	MsgSize  int
	DelayUs  uint
	RetCode  int
}

type PollSubscriberArgs struct {
	ThreadID   C.pthread_t
	TimeoutSec int
	RetCode    int
}

var (
	numTopics            int
	numPublications      int
	numSubscriptions     int
	numPollSubscriptions int

	pArgs       PublisherArgs
	sArgs       PollSubscriberArgs
	PubArgs     = &pArgs
	PollSubArgs = &sArgs

	TotalPublished     atomic.Int64
	TotalPollReceived  atomic.Int64
	TotalAsyncReceived atomic.Int64

	NumMessages  = 1
	MessageSize  = 1024

	Topics       []*AppTopic

	StartTimestamp string
	EndTimestamp   string

	PrintFrequencyModulo int64 = 1000

	startFlag int
)

var (
	startMutex = C.pthread_mutex_t{}
	startCond  = C.pthread_cond_t{}
)

func waitForStartSignal() {
	C.pthread_mutex_lock(&startMutex)
	for startFlag == 0 {
		C.pthread_cond_wait(&startCond, &startMutex)
	}
	C.pthread_mutex_unlock(&startMutex)
}

func sendStartSignal() {
	C.pthread_mutex_lock(&startMutex)
	startFlag = 1
	C.pthread_cond_broadcast(&startCond)
	C.pthread_mutex_unlock(&startMutex)
}

func defaultPublishMsgFunction(topic *C.ndw_Topic_T) C.INT_T {
	_ = topic
	return C.ndw_PublishMsg()
}

func NATS_Subscribe_Async(topic *C.ndw_Topic_T) C.INT_T {
	return C.ndw_SubscribeAsync(topic)
}

func NATS_Subscribe_Sync(topic *C.ndw_Topic_T) C.INT_T {
	return C.ndw_SubscribeSynchronous(topic)
}

func JS_subscribe_Async(topic *C.ndw_Topic_T) C.INT_T {
	return C.ndw_SubscribeAsync(topic)
}

func JS_Subscribe_Sync(topic *C.ndw_Topic_T) C.INT_T {
	return C.ndw_SubscribeSynchronous(topic)
}

func AsyncQ_Subscribe(topic *C.ndw_Topic_T) C.INT_T {
	return C.ndw_SubscribeAsync(topic)
}

func NATS_Poll(topic *C.ndw_Topic_T, timeoutUs C.LONG_T) C.INT_T {
	var droppedMessages C.LONG_T
	timeoutMs := C.LONG_T(1)
	if timeoutUs >= 1000 {
		timeoutMs = timeoutUs / 1000
	}
	return C.ndw_SynchronousPollForMsg(topic, timeoutMs, &droppedMessages)
}

func JS_Poll(topic *C.ndw_Topic_T, timeoutUs C.LONG_T) C.INT_T {
	var droppedMessages C.LONG_T
	timeoutMs := C.LONG_T(1)
	if timeoutUs >= 1000 {
		timeoutMs = timeoutUs / 1000
	}
	return C.ndw_SynchronousPollForMsg(topic, timeoutMs, &droppedMessages)
}

func AsyncQ_Poll(topic *C.ndw_Topic_T, timeoutUs C.LONG_T) C.INT_T {
	return C.ndw_PollAsyncQueue(topic, timeoutUs)
}

func setCommitCounters(topic *C.ndw_Topic_T) {
	if topic != nil {
		aTopic := (*AppTopic)(unsafe.Pointer(topic.app_opaque))
		if aTopic != nil {
			aTopic.MsgsCommitCount = int64(topic.msgs_commit_count)
			aTopic.MsgsAckCount = int64(topic.msgs_ack_count)
			aTopic.MsgsFailedAckCount = int64(topic.msgs_failed_ack_count)
		}
	}
}

func NATS_Commit(topic *C.ndw_Topic_T) C.INT_T {
	retCode := C.ndw_CommitLastMsg(topic)
	setCommitCounters(topic)
	return retCode
}

func JS_Commit(topic *C.ndw_Topic_T) C.INT_T {
	retCode := C.ndw_CommitLastMsg(topic)
	setCommitCounters(topic)
	return retCode
}

func AsyncQ_Commit(topic *C.ndw_Topic_T) C.INT_T {
	retCode := C.ndw_CommitAsyncQueuedMessge(topic)
	setCommitCounters(topic)
	return retCode
}

var (
	publishFunctionData              = PublishFunctionPtrData{Function: defaultPublishMsgFunction, FunctionName: "default_publish_msg_function"}
	natsSubscribeAsyncFunctionData   = SubscribeFunctionPtrData{Function: NATS_Subscribe_Async, FunctionName: "NATS_Subscribe_Async"}
	natsSubscribeSyncFunctionData    = SubscribeFunctionPtrData{Function: NATS_Subscribe_Sync, FunctionName: "NATS_Subscribe_Sync"}
	jsSubscribeAsyncFunctionData     = SubscribeFunctionPtrData{Function: JS_subscribe_Async, FunctionName: "JS_subscribe_Async"}
	jsSubscribeSyncFunctionData      = SubscribeFunctionPtrData{Function: JS_Subscribe_Sync, FunctionName: "JS_Subscribe_Sync"}
	asyncqSubscribeFunctionData      = SubscribeFunctionPtrData{Function: AsyncQ_Subscribe, FunctionName: "AsyncQ_Subscribe"}

	natsPollFunctionData             = PollFunctionPtrData{Function: NATS_Poll, FunctionName: "NATS_Poll"}
	jsPollFunctionData               = PollFunctionPtrData{Function: JS_Poll, FunctionName: "JS_Poll"}
	asyncqPollFunctionData           = PollFunctionPtrData{Function: AsyncQ_Poll, FunctionName: "AsyncQ_Poll"}

	natsCommitFunctionData           = PollCommitFunctionPtrData{Function: NATS_Commit, FunctionName: "NATS_Commit"}
	jsCommitFunctionData             = PollCommitFunctionPtrData{Function: JS_Commit, FunctionName: "JS_Commit"}
	asyncqCommitFunctionData         = PollCommitFunctionPtrData{Function: AsyncQ_Commit, FunctionName: "AsyncQ_Commit"}
)

func printAppTopicConfig(topic *AppTopic) {
	fmt.Println("------")
	fmt.Printf("Topic Number: %d\n", topic.TopicNumber)
	fmt.Printf("LogicalUniqueName: %s\n", C.GoString((*C.char)(unsafe.Pointer(&topic.LogicalUniqueName[0]))))
	fmt.Printf("Domain: %s\n", C.GoString((*C.char)(unsafe.Pointer(&topic.Domain[0]))))
	fmt.Printf("Connection: %s\n", C.GoString((*C.char)(unsafe.Pointer(&topic.Connection[0]))))
	fmt.Printf("TopicName: %s\n", C.GoString((*C.char)(unsafe.Pointer(&topic.TopicName[0]))))
	fmt.Printf("Enabled: %t\n", topic.Enabled)
	fmt.Printf("Disabled: %t\n", topic.Disabled)
	fmt.Printf("Publish_Enabled: %t\n", topic.PublishEnabled)
	fmt.Printf("Subscribe_Enabled: %t\n", topic.SubscribeEnabled)
	fmt.Printf("publish_function_data: <%s>\n", topic.FunctionPublishData.FunctionName)
	fmt.Printf("subscribe_function_data: <%s>\n", topic.FunctionSubscribeData.FunctionName)
	fmt.Printf("poll_function_data: <%s>\n", topic.FunctionPollData.FunctionName)
	fmt.Printf("commit_function_data: <%s>\n", topic.FunctionCommitData.FunctionName)

	switch topic.PublishType {
	case NATS:
		fmt.Println("PublishType: NATS")
	case JS:
		fmt.Println("PublishType: JS")
	}

	switch topic.SubscribeType {
	case NATSAsync:
		fmt.Println("SubscribeType: NATSAsync")
	case NATSPoll:
		fmt.Println("SubscribeType: NATSPoll")
	case JSPush:
		fmt.Println("SubscribeType: JSPush")
	case JSPull:
		fmt.Println("SubscribeType: JSPull")
	case QAsync:
		fmt.Println("SubscribeType: QAsync")
	case Unknown:
		fmt.Println("SubscribeType: Unknown")
	default:
		fmt.Println("SubscribeType: ??")
	}
	fmt.Printf("is_poll_activity: %t\n", topic.IsPollActivity)
}

func printAppTopicConfigs() {
	if Topics == nil || len(Topics) == 0 {
		return
	}

	fmt.Println()
	fmt.Println("================= BEGIN: Print App Topic Configurations ==========================")
	fmt.Println()
	for i, topic := range Topics {
		fmt.Printf("Topic %d:\n", i+1)
		printAppTopicConfig(topic)
		fmt.Println()
	}
	fmt.Println("================= END: Print App Topic Configurations ==========================")
	fmt.Println()
}

func loadAppTopicConfigs(jsonFile string) []*AppTopic {
	data, err := os.ReadFile(jsonFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file '%s': %v\n", jsonFile, err)
		return nil
	}

	var root struct {
		NDWAppConfig struct {
			Topics []map[string]interface{} `json:"Topics"`
		} `json:"NDWAppConfig"`
	}

	err = json.Unmarshal(data, &root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "*** ERROR parsing JSON:", err)
		return nil
	}

	topicCount := len(root.NDWAppConfig.Topics)
	topics := make([]*AppTopic, topicCount)

	for i, topicJson := range root.NDWAppConfig.Topics {
		t := &AppTopic{TopicNumber: i}

		copyStringField := func(dst *[256]byte, key string) {
			if v, ok := topicJson[key].(string); ok {
				copy(dst[:], v)
			}
		}

		copyStringField(&t.LogicalUniqueName, "LogicalUniqueName")
		copyStringField(&t.Domain, "Domain")
		copyStringField(&t.Connection, "Connection")
		copyStringField(&t.TopicName, "TopicName")

		t.Enabled = intFromMap(topicJson, "Enabled") != 0
		t.Disabled = !t.Enabled
		t.PublishEnabled = intFromMap(topicJson, "Publish") != 0
		t.SubscribeEnabled = intFromMap(topicJson, "Subscribe") != 0

		if val, ok := topicJson["PollTimeoutus"].(float64); ok && val >= 0 {
			t.PollTimeoutUs = int(val)
		}

		if t.PublishEnabled {
			ptype := strFromMap(topicJson, "PublishType")
			switch ptype {
			case "NATS":
				t.PublishType = NATS
				t.FunctionPublishData = publishFunctionData
			case "JS":
				t.PublishType = JS
				t.FunctionPublishData = publishFunctionData
			default:
				fmt.Fprintf(os.Stderr, "Invalid publish_type <%s>\n", ptype)
				return nil
			}
			numPublications++
		}

		if t.SubscribeEnabled {
			typeVal := strFromMap(topicJson, "SubscribeType")
			switch typeVal {
			case "NATSAsync":
				t.SubscribeType = NATSAsync
				t.FunctionSubscribeData = natsSubscribeAsyncFunctionData
				t.FunctionCommitData = natsCommitFunctionData
				t.IsPollActivity = false
			case "NATSPoll":
				numPollSubscriptions++
				t.SubscribeType = NATSPoll
				t.FunctionSubscribeData = natsSubscribeSyncFunctionData
				t.IsPollActivity = true
				t.FunctionPollData = natsPollFunctionData
				t.FunctionCommitData = natsCommitFunctionData
			case "JSPush":
				t.SubscribeType = JSPush
				t.FunctionSubscribeData = jsSubscribeAsyncFunctionData
				t.IsPollActivity = false
				t.FunctionCommitData = jsCommitFunctionData
			case "JSPull":
				numPollSubscriptions++
				t.SubscribeType = JSPull
				t.FunctionSubscribeData = jsSubscribeSyncFunctionData
				t.IsPollActivity = true
				t.FunctionPollData = jsPollFunctionData
				t.FunctionCommitData = jsCommitFunctionData
			case "QAsync":
				numPollSubscriptions++
				t.SubscribeType = QAsync
				t.FunctionSubscribeData = asyncqSubscribeFunctionData
				t.IsPollActivity = true
				t.FunctionPollData = asyncqPollFunctionData
				t.FunctionCommitData = asyncqCommitFunctionData
			default:
				fmt.Fprintf(os.Stderr, "Invalid subscribe_type <%s>\n", typeVal)
				return nil
			}
			numSubscriptions++
		}
		topics[i] = t
	}

	numTopics = topicCount
	Topics = topics
	return topics
}

func intFromMap(m map[string]interface{}, key string) int {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case float64:
			return int(val)
		}
	}
	return 0
}

func strFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func populateTopicPointers() int {
	if numTopics <= 0 {
		return -1
	}

	for i := 0; i < numTopics; i++ {
		t := Topics[i]
		domain := C.ndw_GetDomainByName((*C.char)(unsafe.Pointer(&t.Domain[0])))
		if domain == nil {
			fmt.Fprintf(os.Stderr, "Failed to get NDW_Domain_T* from Domain Name <%s>\n", C.GoString((*C.char)(unsafe.Pointer(&t.Domain[0]))))
			return -2
		}

		connection := C.ndw_GetConnectionByNameFromDomain(domain, (*C.char)(unsafe.Pointer(&t.Connection[0])))
		if connection == nil {
			fmt.Fprintf(os.Stderr, "Failed to get NDW_Connection_T* from Connection Name<%s> for Domain<%s>\n",
				C.GoString((*C.char)(unsafe.Pointer(&t.Connection[0]))), C.GoString((*C.char)(unsafe.Pointer(&t.Domain[0]))))
			return -3
		}

		topic := C.ndw_GetTopicByNameFromConnection(connection, (*C.char)(unsafe.Pointer(&t.TopicName[0])))
		if topic == nil {
			fmt.Fprintf(os.Stderr, "Failed to get ndw_Topic_T* from Topic Name <%s> for Domain<%s> and Connection<%s>\n",
				C.GoString((*C.char)(unsafe.Pointer(&t.TopicName[0]))), C.GoString((*C.char)(unsafe.Pointer(&t.Connection[0]))), C.GoString((*C.char)(unsafe.Pointer(&t.Domain[0]))))
			return -4
		}

		t.Topic = topic
		t.Topic.app_opaque = unsafe.Pointer(t)
	}

	return 0
}

func getCurrentTimestamp(label string) string {
	ts := time.Now()
	ms := ts.Nanosecond() / 1e6
	us := (ts.Nanosecond() / 1e3) % 1e3
	ns := ts.Nanosecond() % 1e3

	formatted := fmt.Sprintf("[%s] HH:MM:SS:ms:us:ns %02d:%02d:%02d:%03d:%03d:%03d",
		label, ts.Hour(), ts.Minute(), ts.Second(), ms, us, ns)
	return formatted
}

func ndwHandleAsyncMessage(topic *C.ndw_Topic_T, opaque unsafe.Pointer) C.int {
	aTopic := (*AppTopic)(topic.app_opaque)
	if aTopic == nil {
		fmt.Fprintf(os.Stderr, "*** ERROR: topic->app_opaque is NULL for <%s>\n", C.GoString(topic.debug_desc))
		return -101
	}
	aTopic.MsgsReceivedCount++

	numAsyncMsgs := atomic.AddInt64(&totalAsyncReceived, 1) - 1
	msg := (*C.char)(topic.last_msg_received)
	msgSize := int(topic.last_msg_received_size)
	var queuedMsgs C.uint64_t
	C.ndw_GetQueuedMsgCount(topic, &queuedMsgs)

	if topic.total_received_msgs%printFrequencyModulo == 0 {
		fmt.Fprintf(os.Stderr, "<<<Received ASYNC Message: TopicId<%d, %s> num_async_msgs<%d> topic->total_received_msgs<%d> queued_msgs<%d> msg_size<%d> msg<%s>\n",
			int(topic.topic_unique_id), C.GoString(topic.topic_unique_name), numAsyncMsgs,
			int(topic.total_received_msgs), uint64(queuedMsgs), msgSize, C.GoString(msg))
	}

	if aTopic.FunctionCommitData.Function == nil {
		fmt.Fprintf(os.Stderr, "*** ERROR: function_commit is NULL for <%s>\n", C.GoString(topic.debug_desc))
		return -102
	}

	commitCode := aTopic.FunctionCommitData.Function(topic)
	if commitCode != 0 {
		fmt.Fprintf(os.Stderr, "*** ERROR: function_commit failed with error code<%d> on <%s>\n",
			int(commitCode), C.GoString(topic.debug_desc))
		return commitCode
	}

	return 0
}

func threadPublisher(arg unsafe.Pointer) unsafe.Pointer {
	fmt.Printf("Publisher Thread ID<%d>\n", C.pthread_self())

	args := (*PublisherArgs)(arg)

	if numPublications <= 0 {
		args.RetCode = 0
		return arg
	}

	ndwThreadInit()

	args.RetCode = -1
	args.ThreadID = uint64(C.pthread_self())
	logX("Publish Thread ID<%d>\n", args.ThreadID)

	logX("Publisher Args: num_topics<%d> num_msgs<%d> msg_size<%d> delay_us<%d>\n",
		numTopics, args.NumMsgs, args.MsgSize, args.DelayUs)
	logX("Total Number of Active Topics to Publish to is = %d\n", numPublications)

	for i := 0; i < numTopics; i++ {
		aTopic := Topics[i]
		if aTopic.Disabled {
			continue
		}

		if aTopic.PublishEnabled {
			if aTopic.Topic == nil {
				logErr("*** ERROR: ndw_Topic_T* pointer NOT set for <%s>\n", &aTopic.TopicName)
				args.RetCode = -1
				return arg
			}

			if aTopic.FunctionPublishData.Function == nil {
				logErr("AppTopic_T has NULL function_publish for Topic<%s>\n", &aTopic.TopicName)
				args.RetCode = -1
				return arg
			}

			aTopic.Topic.sequence_number = 0
		}
	}

	waitForStartSignal()

	for i := 0; i < args.NumMsgs; i++ {
		for j := 0; j < numTopics; j++ {
			aTopic := Topics[j]
			topic := aTopic.Topic

			if aTopic.Disabled || !aTopic.PublishEnabled || topic.pub_key == nil {
				continue
			}

			topic.sequence_number++
			data, dataSize := ndwCreateTestJsonString(topic, "Sample Message", topic.sequence_number, args.MsgSize)
			cxt := ndwCreateOutMsgCxt(topic, C.NDW_MSGHEADER_1, C.NDW_ENCODING_FORMAT_JSON, (*C.uchar)(unsafe.Pointer(&data[0])), C.int(dataSize))

			if cxt == nil {
				logErr("Failed to create Output Message Context!\n")
				continue
			}

			publishCode := aTopic.FunctionPublishData.Function(topic)
			if publishCode != 0 {
				logErr("*** ERROR: ndw_PublishMsg() failed! sequence_number<%d> error code <%d>\n",
					topic.sequence_number, publishCode)
				continue
			}

			if topic.sequence_number%printFrequencyModulo == 0 {
				log(">>> Published Message: sequence_number<%d> data <%s> data_size <%d>\n",
					topic.sequence_number, string(data), dataSize)
			}

			aTopic.MsgsPublishedCount++
			atomic.AddInt64(&totalPublished, 1)
			if args.DelayUs > 0 {
				time.Sleep(time.Duration(args.DelayUs) * time.Microsecond)
			}
		}
	}

	ndwThreadExit()
	args.RetCode = 0
	return arg
}

func subscribe() C.INT_T {
	logX("BEGIN: Subscribing...\n")
	for i := 0; i < numTopics; i++ {
		aTopic := Topics[i]
		topic := aTopic.Topic

		if aTopic.Disabled || !aTopic.SubscribeEnabled || topic.sub_key == nil {
			continue
		}

		if aTopic.FunctionSubscribeData.Function == nil {
			logX("*** ERROR: subscribe_data.function NOT set for <%s %s, %s>\n",
				&aTopic.Domain, &aTopic.Connection, &aTopic.TopicName)
			return -1
		}

		logX("Subscribing to <%s %s, %s> using function<%s>\n",
			&aTopic.Domain, &aTopic.Connection, &aTopic.TopicName, &aTopic.FunctionSubscribeData.FunctionName)

		retCode := aTopic.FunctionSubscribeData.Function(topic)
		if retCode != 0 {
			logErr("*** ERROR: Subscription failed with return_code<%d> for %s\n", retCode, C.GoString(topic.debug_desc))
			return retCode
		}
	}
	logX("END: Subscribing...\n")
	return 0
}

func threadSubscriberPoll(arg unsafe.Pointer) unsafe.Pointer {
	fmt.Printf("Subscriber Poll Thread ID<%d>\n", C.pthread_self())

	args := (*PollSubscriberArgs)(arg)
	if args.TimeoutSec <= 0 {
		args.TimeoutSec = 6
	}

	if numPollSubscriptions <= 0 {
		logErr("NOTE: There are no polling subscriptions enabled\n")
		args.RetCode = 0
		return arg
	}

	nPollSubscriptions := 0
	for i := 0; i < numTopics; i++ {
		aTopic := Topics[i]
		topic := aTopic.Topic
		if aTopic.Disabled || !aTopic.SubscribeEnabled || topic.sub_key == nil {
			continue
		}

		if topic == nil || aTopic.FunctionSubscribeData.Function == nil ||
			aTopic.FunctionPollData.Function == nil || aTopic.FunctionCommitData.Function == nil {
			logErr("*** ERROR: Missing function or topic for <%s>\n", &aTopic.TopicName)
			args.RetCode = -1
			return arg
		}

		nPollSubscriptions++
	}

	if nPollSubscriptions == 0 {
		logErr("*** WARNING: No Subscriptions to poll: nPollSubscriptions<%d> numPollSubscriptions<%d>\n",
			nPollSubscriptions, numPollSubscriptions)
		args.RetCode = 0
		return arg
	}

	ndwThreadInit()

	args.ThreadID = uint64(C.pthread_self())
	logX("Subscribe Thread ID<%d>\n", args.ThreadID)
	args.RetCode = -1

	logX("Subscriber thread waiting for start signal...\n")
	waitForStartSignal()

	var idleStartTime time.Time

	for {
		atLeastOneReceived := false
		now := time.Now()

		for i := 0; i < numTopics; i++ {
			aTopic := Topics[i]
			if aTopic.Disabled || !aTopic.IsPollActivity {
				continue
			}

			topic := aTopic.Topic
			numItems := aTopic.FunctionPollData.Function(topic, C.LONG_T(aTopic.PollTimeoutUs))

			if numItems == 1 {
				aTopic.MsgsReceivedCount++
				atLeastOneReceived = true
				atomic.AddInt64(&totalPollReceived, 1)
				msg := C.GoString((*C.char)(unsafe.Pointer(topic.last_msg_received)))
				msgSize := int(topic.last_msg_received_size)

				if topic.total_received_msgs%printFrequencyModulo == 0 {
					logX("<<< Received POLL Message: [%s, %d] msg<%s> msg_size<%d> ON %s\n",
						&aTopic.TopicName, topic.total_received_msgs, msg, msgSize, C.GoString(topic.debug_desc))
				}

				commitCode := aTopic.FunctionCommitData.Function(topic)
				if commitCode != 0 {
					logX("*** ERROR: Failed to commit message! commit_code<%d> msg<%s> msg_size<%d> on %s\n",
						commitCode, msg, msgSize, C.GoString(topic.debug_desc))
					goto onPollExit
				}
			} else if numItems < 0 {
				logErr("Subscriber encountered error polling for Async Messages on %s\n",
					C.GoString(topic.debug_desc))
				goto onPollExit
			}
		}

		if atLeastOneReceived {
			idleStartTime = time.Time{}
		} else {
			if idleStartTime.IsZero() {
				idleStartTime = now
			} else if now.Sub(idleStartTime).Seconds() > float64(args.TimeoutSec) {
				logX("Subscriber timeout: no message received on any topic in %d seconds.\n", args.TimeoutSec)
				break
			}
		}
	}

	args.RetCode = 0

onPollExit:
	ndwThreadExit()
	return arg
}

func printStatistics() {
	fmt.Fprintln(os.Stdout, "\n---------------- BEGIN: STATISTICS ---------------------")
	for i := 0; i < numTopics; i++ {
		aTopic := Topics[i]
		if aTopic.Disabled {
			continue
		}
		fmt.Fprintf(os.Stdout, "[%d] <%s> ==> msg_published_count<%d> msg_received_count<%d> commits<%d> acks<%d> failed_acks<%d>\n",
			aTopic.TopicNumber, aTopic.TopicName,
			aTopic.MsgsPublishedCount, aTopic.MsgsReceivedCount,
			aTopic.MsgsCommitCount, aTopic.MsgsAckCount, aTopic.MsgsFailedAckCount)
	}
	if startTimestamp != nil {
		fmt.Fprintf(os.Stdout, "Start Time %s\n", C.GoString(startTimestamp))
	}
	if endTimestamp != nil {
		fmt.Fprintf(os.Stdout, "End Time   %s\n", C.GoString(endTimestamp))
	}
	fmt.Fprintf(os.Stdout, "\nTOTALS: Published<%d> AsyncReceived<%d> PollReceived<%d>\n",
		atomic.LoadInt64(&totalPublished), atomic.LoadInt64(&totalAsyncReceived), atomic.LoadInt64(&totalPollReceived))
	fmt.Fprintln(os.Stdout, "\n---------------- END: STATISTICS ---------------------\n")
}

func testInitiateConnections() C.INT_T {
	for i := 0; i < numTopics; i++ {
		t := Topics[i]
		logX("Trying to Connect to <%s, %s>\n", &t.Domain, &t.Connection)
		retCode := C.ndw_Connect((*C.char)(unsafe.Pointer(&t.Domain[0])), (*C.char)(unsafe.Pointer(&t.Connection[0])))
		if retCode != 0 {
			logErr("*** ERROR: Failed to Connect to <%s, %s>\n", &t.Domain, &t.Connection)
			return retCode
		}
	}
	return 0
}

func setPrintFrequency(maxMsgsPerTopic int) {
	switch {
	case maxMsgsPerTopic <= 100:
		printFrequencyModulo = 1
	case maxMsgsPerTopic <= 1000:
		printFrequencyModulo = 100
	case maxMsgsPerTopic <= 10000:
		printFrequencyModulo = 1000
	case maxMsgsPerTopic <= 100000:
		printFrequencyModulo = 10000
	case maxMsgsPerTopic <= 1000000:
		printFrequencyModulo = 100000
	case maxMsgsPerTopic <= 3000000:
		printFrequencyModulo = 300000
	default:
		printFrequencyModulo = 1000000
	}

	fmt.Printf("print_frequency_modulo<%d>\n", printFrequencyModulo)
}


func testInitialize(argc C.int, argv **C.char) C.int {
	fmt.Printf("Main Thread ID<%d>\n", C.pthread_self())

	programOptions := C.ndw_ParseProgramOptions(argc, argv)
	if programOptions == nil {
		fmt.Fprintf(os.Stderr, "*** ERROR: test_initialize(): Failed to parse program options!\n")
		return -1
	}

	if programOptions.max_msgs <= 0 {
		programOptions.max_msgs = 1
	}
	if programOptions.bytes_size <= 0 {
		programOptions.bytes_size = 1024
	}
	if programOptions.wait_time_seconds <= 0 {
		programOptions.wait_time_seconds = 6
	}

	setPrintFrequency(int(programOptions.max_msgs))

	appConfigFile := C.getenv(C.CString(APP_CONFIG_TOPIC_FILE_ENV))
	if appConfigFile == nil {
		fmt.Fprintf(os.Stderr, "*** ERROR: test_initialize(): NULL app_json_config_file specified\n")
		return -2
	}

	loadAppTopicConfigs(appConfigFile)
	if numTopics <= 0 {
		fmt.Fprintf(os.Stderr, "*** ERROR: test_initialize(): Invalid number of topics<%d> in App JSON Config File <%s>\n", numTopics, C.GoString(appConfigFile))
		return -3
	}
	if numPublications <= 0 && numSubscriptions <= 0 {
		fmt.Fprintf(os.Stderr, "*** ERROR: test_initialize(): There are NO Publications or Subscriptions enabled in <%s>\n", C.GoString(appConfigFile))
		return -4
	}

	printAppTopicConfigs()

	if C.ndw_Init() != 0 {
		fmt.Fprintf(os.Stderr, "*** ERROR: ndw_Init() failed!\n")
		return -5
	}
	if populateTopicPointers() != 0 {
		logErr("*** ERROR: test_initialize(): Failed to find Topic Pointers!\n")
		C.ndw_Shutdown()
		return -6
	}

	logX("==> test_initialize() completed. num_topics<%d> num_publications<%d> num_subscriptions<%d> num_poll_subscriptions<%d>\n", numTopics, numPublications, numSubscriptions, numPollSubscriptions)

	if testInitiateConnections() != 0 {
		logErr("*** ERROR: Failed to initialze Connections!\n")
		goto shutdown
	}
	if subscribe() != 0 {
		logErr("*** ERROR: Failed to subscribe!\n")
		goto shutdown
	}

	pubArg.NumMsgs = int(programOptions.max_msgs)
	pubArg.MsgSize = int(programOptions.bytes_size)
	pubArg.DelayUs = 0
	pubArg.RetCode = -1

	fmt.Printf("Publisher Thread Args: num_msgs<%d> msg_size<%d> delay_us<%d>\n", pubArg.NumMsgs, pubArg.MsgSize, pubArg.DelayUs)

	pollSubArgs.TimeoutSec = int(programOptions.wait_time_seconds)
	pollSubArgs.RetCode = -1
	fmt.Printf("Subscriber Thread Args: timeout_sec<%d>\n", pollSubArgs.TimeoutSec)

	var pubThread, subThread C.pthread_t
	if numPublications > 0 {
		//C.pthread_create(&pubThread, nil, (*[0]byte)(C.thread_publisher), unsafe.Pointer(pubArg))
		C.pthread_create(&pubThread, nil, (*[0]byte)(threadPublisher), unsafe.Pointer(pubArg))
	}
	if numPollSubscriptions > 0 {
		//C.pthread_create(&subThread, nil, (*[0]byte)(C.thread_subscriber_poll), unsafe.Pointer(&pollSubArgs))
		C.pthread_create(&subThread, nil, (*[0]byte)(threadSubscriberPoll), unsafe.Pointer(&pollSubArgs))
	}
	time.Sleep(1 * time.Second)

	startTimestamp = getCurrentTimestamp("Start Time => ")
	sendStartSignal()

	if numPublications > 0 {
		C.pthread_join(pubThread, nil)
	}
	if numPollSubscriptions > 0 {
		C.pthread_join(subThread, nil)
	}

	endTimestamp = getCurrentTimestamp("End Time => ")

	sleepWaitTime := int(programOptions.wait_time_seconds)
	for {
		published := atomic.LoadInt64(&totalPublished)
		prevReceived := atomic.LoadInt64(&totalPollReceived) + atomic.LoadInt64(&totalAsyncReceived)
		logX("---> main(): Before Sleep<%d>: total_published<%d> prev_received<%d>\n", sleepWaitTime, published, prevReceived)
		time.Sleep(time.Duration(sleepWaitTime) * time.Second)
		published = atomic.LoadInt64(&totalPublished)
		received := atomic.LoadInt64(&totalPollReceived) + atomic.LoadInt64(&totalAsyncReceived)
		logX("---> main(): After Sleep<%d> total_published<%d> prev_received<%d> received<%d>\n", sleepWaitTime, published, prevReceived, received)
		if prevReceived == received {
			break
		} else {
			prevReceived = received
		}
	}
	logX("main(): At end: total received<%d>\n", atomic.LoadInt64(&totalPollReceived)+atomic.LoadInt64(&totalAsyncReceived))

shutdown:
	C.ndw_Shutdown()
	printStatistics()
	C.free(unsafe.Pointer(startTimestamp))
	C.free(unsafe.Pointer(endTimestamp))
	C.free(unsafe.Pointer(Topics))
	return 0
}

