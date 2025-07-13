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

// Declare the Go handler function to be exported

// Declare the setter function

*/
import "C"

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
	"runtime"
    "encoding/json"
    "ndw/ndw"
)

type (
	PublishFunction    func(topicData *ndw.NDW_TopicData) int32
	SubscribeFunction  func(topicData *ndw.NDW_TopicData) int32
	PollFunction       func(topicData *ndw.NDW_TopicData, timeout_us int64) int32
	PollCommitFunction func(topicData *ndw.NDW_TopicData) int32
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

type AppTopic struct {
	Topic                 *ndw.NDW_TopicData
	TopicNumber           int
	LogicalUniqueName     string
	Domain                string
	Connection            string
	TopicName             string
	Enabled               bool
	Disabled              bool
	PublishEnabled        bool
	SubscribeEnabled      bool
	Publish               bool
	Subscribe             bool
	PollTimeoutus         int
	PublishType           string
	SubscribeType         string
	FunctionPublishData   PublishFunctionPtrData
	FunctionSubscribeData SubscribeFunctionPtrData
	IsPollActivity        bool
	FunctionPollData      PollFunctionPtrData
	FunctionCommitData    PollCommitFunctionPtrData
    ToString              string

	MsgsPublishedCount    int64
	MsgsReceivedCount     int64
	MsgsCommitCount       int64
	MsgsAckCount          int64
	MsgsFailedAckCount    int64
}

var appTopicsMap = make(map[string]*AppTopic)

type PublisherArgs struct {
    ThreadID uint64
	NumMsgs  int
	MsgSize  int
	delay_us  uint
	RetCode  int
}

var pubArg PublisherArgs

type PollSubscriberArgs struct {
    ThreadID uint64
	TimeoutSec int
	RetCode    int
}

var pollSubArgs PollSubscriberArgs

var (
    max_msgs             int
    bytes_size           int
    wait_time_seconds    int

	numTopics            int
	numPublications      int
	numSubscriptions     int
	numPollSubscriptions int

	pArgs       PublisherArgs
	sArgs       PollSubscriberArgs
	PubArgs     = &pArgs
	PollSubArgs = &sArgs

	TotalPublished     int64
	TotalPollReceived  int64
	TotalAsyncReceived int64

	NumMessages  = 1
	MessageSize  = 1024

	Topics       []*AppTopic

	StartTimestamp string
	EndTimestamp   string

	PrintFrequencyModulo int64 = 1000

	startFlag int
)


func defaultPublishMsgFunction(topicData *ndw.NDW_TopicData) int32 {
    return ndw.NDW_PublishMsg(topicData)
}

func NATS_Subscribe_Async(topicData *ndw.NDW_TopicData) int32 {
	return ndw.NDW_SubscribeAsync(topicData)
}

func NATS_Subscribe_Sync(topicData *ndw.NDW_TopicData) int32 {
	return ndw.NDW_SubscribeSynchronous(topicData)
}

func JS_subscribe_Async(topicData *ndw.NDW_TopicData) int32 {
	return ndw.NDW_SubscribeAsync(topicData)
}

func JS_Subscribe_Sync(topicData *ndw.NDW_TopicData) int32 {
	return ndw.NDW_SubscribeSynchronous(topicData)
}

func AsyncQ_Subscribe(topicData *ndw.NDW_TopicData) int32 {
	return ndw.NDW_SubscribeAsync(topicData)
}

func NATS_Poll(topicData *ndw.NDW_TopicData, timeout_us int64) int32 {
    var droppedMessages int64
	return ndw.NDW_SynchronousPollForMsg(topicData, timeout_us, &droppedMessages)
}

func JS_Poll(topicData *ndw.NDW_TopicData, timeout_us int64) int32 {
    var droppedMessages int64
	return ndw.NDW_SynchronousPollForMsg(topicData, timeout_us, &droppedMessages)
}


func AsyncQ_Poll(topicData *ndw.NDW_TopicData, timeout_us int64) int32 {
	return int32(ndw.NDW_PollAsyncQeueue(topicData, timeout_us))
}

func setCommitCounters(topicData *ndw.NDW_TopicData)  {
    app_topic, exists := appTopicsMap[topicData.TopicName]
    if !exists {
        ndw.NDW_LOGERR("*** ERROR: Failed to get AppTopic structure from TopicName<%s>\n", topicData.TopicName)
    } else {
        ndw.NDW_GetMsgCommitAckCounters(topicData,
            &app_topic.MsgsCommitCount, &app_topic.MsgsAckCount, &app_topic.MsgsFailedAckCount)
    }
}

func NATS_Commit(topicData *ndw.NDW_TopicData) int32 {
	retCode := ndw.NDW_CommitLastMsg(topicData)
	setCommitCounters(topicData)
	return retCode
}

func JS_Commit(topicData *ndw.NDW_TopicData) int32 {
	retCode := ndw.NDW_CommitLastMsg(topicData)
	setCommitCounters(topicData)
	return retCode
}

func AsyncQ_Commit(topicData *ndw.NDW_TopicData) int32 {
	retCode := ndw.NDW_CommitLastMsg(topicData)
	setCommitCounters(topicData)
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
	fmt.Printf("LogicalUniqueName: %s\n", topic.LogicalUniqueName)
	fmt.Printf("Domain: %s\n", topic.Domain)
	fmt.Printf("Connection: %s\n", topic.Connection)
	fmt.Printf("TopicName: %s\n", topic.TopicName)
	fmt.Printf("Enabled: %t\n", topic.Enabled)
	fmt.Printf("Disabled: %t\n", topic.Disabled)
	fmt.Printf("Publish: %t\n", topic.Publish)
	fmt.Printf("PublishEnabled: %t\n", topic.PublishEnabled)
	fmt.Printf("SubscribeEnabled: %t\n", topic.SubscribeEnabled)
	fmt.Printf("publish_function_data: <%s>\n", topic.FunctionPublishData.FunctionName)
	fmt.Printf("subscribe_function_data: <%s>\n", topic.FunctionSubscribeData.FunctionName)
	fmt.Printf("poll_function_data: <%s>\n", topic.FunctionPollData.FunctionName)
	fmt.Printf("commit_function_data: <%s>\n", topic.FunctionCommitData.FunctionName)
	fmt.Printf("PollTimeoutus: <%d>\n", topic.PollTimeoutus);

    switch topic.PublishType {
        case "NATS", "JS":
            fmt.Printf("PublishType: <%s>\n", topic.PublishType);
        default:
            fmt.Printf("PublishType: <%s>\n", "*** Invalid Value ***");
            
    }

	switch topic.SubscribeType {
        case "NATSAsync", "NATSPoll", "JSPush", "JSPull", "QAsync":
            fmt.Printf("SubscribeType: <%s>\n", topic.SubscribeType)
        default:
            fmt.Printf("SubscribeType: <%s>\n", "*** Invalid Value ***");
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
            AppName string `json:"AppName"`
			Topics []AppTopic `json:"Topics"`
		} `json:"NDWAppConfig"`
	}

	err = json.Unmarshal(data, &root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "*** ERROR parsing JSON:", err)
		return nil
	}


	topicCount := len(root.NDWAppConfig.Topics)
    ndw.NDW_LOGX("topicCount = %d\n", topicCount);
	topics := make([]*AppTopic, topicCount)
    ndw.CreateTopicList(topicCount)

	for i := range root.NDWAppConfig.Topics {
        topics[i] = &root.NDWAppConfig.Topics[i]
		t := topics[i]
        t.ToString = "<" + t.Domain + ", " + t.Connection + ", " + t.TopicName + ">"
        appTopicsMap[t.TopicName] = t
        t.Topic = ndw.GetTopicData(i)

        t.PublishEnabled = t.Publish
        t.SubscribeEnabled = t.Subscribe

        ndw.InitializeTopicData(i, t.Domain, t.Connection, t.TopicName)

		if t.PublishEnabled {
			switch t.PublishType {
			case "NATS":
				t.FunctionPublishData = publishFunctionData
			case "JS":
				t.FunctionPublishData = publishFunctionData
			default:
				fmt.Fprintf(os.Stderr, "Invalid PublishType <%s>\n", t.PublishType)
				return nil
			}
			numPublications++
		}

		if t.SubscribeEnabled {
			switch t.SubscribeType {
			case "NATSAsync":
				t.FunctionSubscribeData = natsSubscribeAsyncFunctionData
				t.FunctionCommitData = natsCommitFunctionData
				t.IsPollActivity = false
			case "NATSPoll":
				numPollSubscriptions++
				t.FunctionSubscribeData = natsSubscribeSyncFunctionData
				t.IsPollActivity = true
				t.FunctionPollData = natsPollFunctionData
				t.FunctionCommitData = natsCommitFunctionData
			case "JSPush":
				t.FunctionSubscribeData = jsSubscribeAsyncFunctionData
				t.IsPollActivity = false
				t.FunctionCommitData = jsCommitFunctionData
			case "JSPull":
				numPollSubscriptions++
				t.FunctionSubscribeData = jsSubscribeSyncFunctionData
				t.IsPollActivity = true
				t.FunctionPollData = jsPollFunctionData
				t.FunctionCommitData = jsCommitFunctionData
			case "QAsync":
				numPollSubscriptions++
				t.FunctionSubscribeData = asyncqSubscribeFunctionData
				t.IsPollActivity = true
				t.FunctionPollData = asyncqPollFunctionData
				t.FunctionCommitData = asyncqCommitFunctionData
			default:
				fmt.Fprintf(os.Stderr, "Invalid SubscribeType <%s>\n", t.SubscribeType)
				return nil
			}
			numSubscriptions++
		}
	}

	numTopics = topicCount
	return topics
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

func ndw_AsyncMessageHandler(topicData *ndw.NDW_TopicData, msg *ndw.NDW_ReceivedMsg) {

	atomic.AddInt64(&TotalAsyncReceived, 1)

    if int64(topicData.TotalMsgsReceived)%PrintFrequencyModulo == 0 {
        ndw.NDW_LOGX("✅ Message received on topic %s (%s.%s) [%d]\n",
            topicData.TopicName, topicData.DomainName, topicData.ConnectionName, topicData.TotalMsgsReceived)
        ndw.NDW_LOGX("📦 Payload (%d bytes): %s\n", msg.MsgSize, string(msg.MsgBody))
        ndw.NDW_PrintReceivedMsg(msg);
    }

    ret := ndw.NDW_CommitLastMsg(topicData)
    if (0 != ret) {
        ndw.NDW_LOGERR("*** ERROR*** Failed to CommitLastMsg!\n")
    }

    setCommitCounters(topicData)
    app_topic, exists := appTopicsMap[topicData.TopicName]
    if !exists {
        ndw.NDW_LOGERR("*** ERROR: Failed to get AppTopic structure from TopicName<%s>\n", topicData.TopicName)
    } else {
        app_topic.MsgsReceivedCount++
    }
}

func threadPublisher(start chan struct{}, args *PublisherArgs) *PublisherArgs {
    runtime.LockOSThread()
    defer runtime.UnlockOSThread()

    args.ThreadID = uint64(C.pthread_self())

	if numPublications <= 0 {
		args.RetCode = 0
		return args
    }

	ndw.NDW_LOGX("[Go:ThreadID<%d>]: Publisher Args: num_topics<%d> num_msgs<%d> msg_size<%d> delay_us<%d>\n",
		args.ThreadID, numTopics, args.NumMsgs, args.MsgSize, args.delay_us)
	ndw.NDW_LOGX("Total Number of Active Topics to Publish to is = %d\n", numPublications)

	for i := 0; i < numTopics; i++ {
		aTopic := Topics[i]
		if aTopic.Disabled {
			continue
		}

		if aTopic.PublishEnabled {
			if aTopic.Topic.TopicPtr == nil {
				ndw.NDW_LOGERR("*** ERROR: ndw_Topic_T* pointer NOT set for <%s>\n", aTopic.TopicName)
				args.RetCode = -1
				return args
			}

			if aTopic.FunctionPublishData.Function == nil {
				ndw.NDW_LOGERR("AppTopic_T has NULL function_publish for Topic<%s>\n", aTopic.TopicName)
				args.RetCode = -1
				return args
			}
		}

        ndw.NDW_SetTopicSequenceNumber(aTopic.Topic, 0)
	}

    ndw.NDW_LOGX("Publisher waiting for signal...\n");
    <-start
    ndw.NDW_LOGX("Publisher started...\n");

	args.RetCode = -1

	ndw.NDW_ThreadInit()

    for i := 0; i < args.NumMsgs; i++ {
        for j := 0; j < numTopics; j++ {
            aTopic := Topics[j]
            topic_data := aTopic.Topic
            pub_key := ndw.NDW_GetPubKey(topic_data)
            if aTopic.Disabled || !aTopic.PublishEnabled || len(pub_key) == 0 {
                continue
            }

            //
            sequence_number := ndw.NDW_IncrementTopicSequenceNumber(topic_data)
            json_msg, _ := ndw.CreateTestJSONMessageOfSize(topic_data,
                                    "Sample Message", sequence_number, args.MsgSize)
            payload := []byte(json_msg)
            dataSize := len(payload)
            cxt := ndw.CreateOutMsgCxt(topic_data, payload,
                                        ndw.NDW_DEFAULT_MSG_HEADER, ndw.NDW_ENCODING_FORMAT_JSON)

            if cxt == nil {
                ndw.NDW_LOGERR("Failed to create Output Message Context for %s\n", topic_data.ToString)
                continue
            }

            publishCode := aTopic.FunctionPublishData.Function(topic_data)
            if publishCode != 0 {
                ndw.NDW_LOGERR("*** ERROR: ndw_PublishMsg() failed! sequence_number<%d> error code <%d> for %s\n",
                    sequence_number, publishCode, topic_data.ToString)
                continue
            }

            if int64(sequence_number - 1)%PrintFrequencyModulo == 0 {
                ndw.NDW_LOG(">>> Published Message: sequence_number<%d> data <%s> data_size <%d> on %s\n",
                            sequence_number, json_msg, dataSize, topic_data.ToString)
            }
            //

            aTopic.MsgsPublishedCount++
            atomic.AddInt64(&TotalPublished, 1)
            if args.delay_us > 0 {
                time.Sleep(time.Duration(args.delay_us) * time.Microsecond)
            }
        }
    }

	ndw.NDW_ThreadExit()
	args.RetCode = 0
    return args
}

func subscribe() int32 {

    ndw.NDW_LOGX("BEGIN: Subscribing...\n")

    for i := 0; i < numTopics; i++ {
        aTopic := Topics[i]
        topic_data := aTopic.Topic

        sub_key := ndw.NDW_GetSubKey(topic_data)
        if aTopic.Disabled || !aTopic.SubscribeEnabled || len(sub_key) == 0 {
            continue
        }

      if aTopic.FunctionSubscribeData.Function == nil {
            ndw.NDW_LOGX("*** ERROR: subscribe_data.function NOT set for <%s %s, %s>\n",
                &aTopic.Domain, &aTopic.Connection, &aTopic.TopicName)
            return -1
        }

        ndw.NDW_LOGX("Subscribing to <%s %s, %s> using function<%s>\n",
                aTopic.Domain, aTopic.Connection, aTopic.TopicName, aTopic.FunctionSubscribeData.FunctionName)

        retCode := aTopic.FunctionSubscribeData.Function(topic_data)
        if retCode != 0 {
            ndw.NDW_LOGERR("*** ERROR: Subscription failed with return_code<%d> for %s\n",
                            retCode, topic_data.ToString)
            return int32(retCode)
        }
    }

    ndw.NDW_LOGX("END: Subscribing...\n")

    return 0
}

func threadSubscriberPoll(start chan struct{}, args *PollSubscriberArgs) *PollSubscriberArgs {
    runtime.LockOSThread()
    defer runtime.UnlockOSThread()

    args.ThreadID = uint64(C.pthread_self())

    if args.TimeoutSec <= 0 {
        args.TimeoutSec = 6
    }

    if numPollSubscriptions <= 0 {
        ndw.NDW_LOGERR("NOTE: There are no polling subscriptions enabled\n")
        args.RetCode = 0
        return args
    }

   nPollSubscriptions := 0
   for i := 0; i < numTopics; i++ {
        aTopic := Topics[i]
        topic_data := aTopic.Topic
        sub_key := ndw.NDW_GetSubKey(topic_data)
        if aTopic.Disabled || !aTopic.SubscribeEnabled || len(sub_key) == 0 {
            continue
        }

        if aTopic.FunctionSubscribeData.Function == nil ||
            aTopic.FunctionPollData.Function == nil || aTopic.FunctionCommitData.Function == nil {
            ndw.NDW_LOGERR("*** ERROR: Missing function or topic for <%s>\n", &aTopic.TopicName)
            args.RetCode = -1
            return args
        }

        nPollSubscriptions++
    }

   if nPollSubscriptions == 0 {
        ndw.NDW_LOGERR("*** WARNING: No Subscriptions to poll: nPollSubscriptions<%d> numPollSubscriptions<%d>\n",
            nPollSubscriptions, numPollSubscriptions)
        args.RetCode = 0
        return args
    }

    ndw.NDW_ThreadInit()

	ndw.NDW_LOGX("Subscribe Thread ID<%d>\n", args.ThreadID)
	args.RetCode = -1

	ndw.NDW_LOGX("[Go:ThreadID<%d>]: Subscriber thread waiting for start signal...\n", args.ThreadID)
    <-start
	ndw.NDW_LOGX("[Go:ThreadID<%d>]: Subscriber thread got start signal...\n", args.ThreadID)

	var idleStartTime time.Time

	for {
		atLeastOneReceived := false
		now := time.Now()

		for i := 0; i < numTopics; i++ {
			aTopic := Topics[i]
			if aTopic.Disabled || !aTopic.IsPollActivity {
				continue
			}

			topic_data := aTopic.Topic
			numItems := aTopic.FunctionPollData.Function(topic_data, int64(aTopic.PollTimeoutus))

			if numItems == 1 {
				aTopic.MsgsReceivedCount++
				atLeastOneReceived = true
				atomic.AddInt64(&TotalPollReceived, 1)

                msg := ndw.NDW_GetLastReceivedMsg(topic_data)

				if int64(msg.Sequence)%PrintFrequencyModulo == 0 {
					ndw.NDW_LOGX("<<< Received POLL Message: [%s, %d] msg<%s> msg_size<%d> ON %s\n",
						aTopic.TopicName, msg.Sequence, string(msg.MsgBody), msg.MsgSize, topic_data.ToString)
                    //ndw.NDW_PrintReceivedMsg(msg)
				}

				commitCode := aTopic.FunctionCommitData.Function(topic_data)
				if commitCode != 0 {
					ndw.NDW_LOGX("*** ERROR: Failed to commit message! commit_code<%d> msg<%s> msg_size<%d> on %s\n",
						commitCode, string(msg.MsgBody), msg.MsgSize, topic_data.ToString)
					goto onPollExit
				}
			} else if numItems < 0 {
				ndw.NDW_LOGERR("Subscriber encountered error polling for Async Messages on %s\n", topic_data.ToString)
				goto onPollExit
			}
		}

		if atLeastOneReceived {
			idleStartTime = time.Time{}
		} else {
			if idleStartTime.IsZero() {
				idleStartTime = now
			} else if now.Sub(idleStartTime).Seconds() > float64(args.TimeoutSec) {
				ndw.NDW_LOGX("Subscriber timeout: no message received on any topic in %d seconds.\n", args.TimeoutSec)
				break
			}
		}
	}

	args.RetCode = 0

onPollExit:
	ndw.NDW_ThreadExit()

    return args
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
    fmt.Fprintf(os.Stdout, "Start Time %s\n", StartTimestamp)
    fmt.Fprintf(os.Stdout, "End Time   %s\n", EndTimestamp)
	fmt.Fprintf(os.Stdout, "\nTOTALS: Published<%d> AsyncReceived<%d> PollReceived<%d>\n",
		atomic.LoadInt64(&TotalPublished), atomic.LoadInt64(&TotalAsyncReceived), atomic.LoadInt64(&TotalPollReceived))
	fmt.Fprintln(os.Stdout, "\n---------------- END: STATISTICS ---------------------\n")
}

func setPrintFrequency(maxMsgsPerTopic int) {
	switch {
	case maxMsgsPerTopic <= 100:
		PrintFrequencyModulo = 1
	case maxMsgsPerTopic <= 1000:
		PrintFrequencyModulo = 100
	case maxMsgsPerTopic <= 10000:
		PrintFrequencyModulo = 1000
	case maxMsgsPerTopic <= 100000:
		PrintFrequencyModulo = 10000
	case maxMsgsPerTopic <= 1000000:
		PrintFrequencyModulo = 100000
	case maxMsgsPerTopic <= 3000000:
		PrintFrequencyModulo = 300000
	default:
		PrintFrequencyModulo = 1000000
	}

	fmt.Printf("print_frequency_modulo<%d>\n", PrintFrequencyModulo)
}

func testInitialize() int {
	fmt.Printf("Main Thread ID<%d>\n", C.pthread_self())
    args := os.Args

    ndw.NDW_LOG("Arg[0]: <%s>\n", args[0])  // the program name
    for i, arg := range args[1:] {
        ndw.NDW_LOG("Arg[%d]: %s\n", i+1, arg)
    }

    // Note Args[0] is executable name and Args[1] is "pubsub", "syncsub", "jspush", "jspoll" or "asyncq".
    // Hence we parse from Args[2:] onwards.
	max_msgs, bytes_size, wait_time_seconds, _, err := ndw.ParseMessageArgs(os.Args[2:])
	if err != nil {
	    ndw.NDW_LOGERR("*** ERROR: test_initialize(): Failed to parse program options!\n")
		return -1
	}
    ndw.NDW_LOG("max_msgs<%d> bytes_size<%d> wait_time_seconds<%d>\n", max_msgs, bytes_size, wait_time_seconds);

	if max_msgs <= 0 {
		max_msgs = 1
	}
	if bytes_size <= 0 {
		bytes_size = 1024
	}

	if wait_time_seconds <= 0 {
		wait_time_seconds = 6
	}

    ndw.NDW_LOG("max_msgs<%d> bytes_size<%d> wait_time_seconds<%d>\n", max_msgs, bytes_size, wait_time_seconds);

	sleepWaitTime := wait_time_seconds

	setPrintFrequency(max_msgs)
    ndw.NDW_LOGX("PrintFrequencyModulo = %d\n", PrintFrequencyModulo)

	appConfigFile := os.Getenv("NDW_APP_TOPIC_FILE")
	if appConfigFile == "" {
		fmt.Fprintf(os.Stderr, "*** ERROR: test_initialize(): NDW_APP_TOPIC_FILE not found from environment\n")
		return -2
	}

	Topics = loadAppTopicConfigs(appConfigFile)
	if numTopics <= 0 {
		fmt.Fprintf(os.Stderr, "*** ERROR: test_initialize(): Invalid number of topics<%d> in App JSON Config File <%s>\n", numTopics, appConfigFile)
		return -3
	}

	printAppTopicConfigs()

	if numPublications <= 0 && numSubscriptions <= 0 {
		fmt.Fprintf(os.Stderr, "*** ERROR: test_initialize(): There are NO Publications or Subscriptions enabled in <%s>\n", appConfigFile)
		return -4
	}

    ndw.NDW_RegisterGoAsyncMsgCallback(ndw_AsyncMessageHandler);

    start_channel := make(chan struct{})

	if ndw.NDW_Initialize() != 0 {
		fmt.Fprintf(os.Stderr, "*** ERROR: ndw_Init() failed!\n")
		return -5
	}

    if 0 != ndw.NDW_ConnectAll() {
        ndw.NDW_LOGERR("** ERROR: Failed to make Connections!\n")
        close(start_channel)
		goto shutdown
    }

	ndw.NDW_LOGX("==> test_initialize() completed. num_topics<%d> num_publications<%d> num_subscriptions<%d> num_poll_subscriptions<%d>\n", numTopics, numPublications, numSubscriptions, numPollSubscriptions)

	if subscribe() != 0 {
		ndw.NDW_LOGERR("*** ERROR: Failed to subscribe!\n")
        close(start_channel)
		goto shutdown
	}

	pubArg.NumMsgs = max_msgs
	pubArg.MsgSize = bytes_size
	pubArg.delay_us = 0
	pubArg.RetCode = -1

	fmt.Printf("Publisher Thread Args: NumMsgs<%d> MsgSize<%d> delay_us<%d>\n", pubArg.NumMsgs, pubArg.MsgSize, pubArg.delay_us)

	pollSubArgs.TimeoutSec = 1;
	pollSubArgs.RetCode = -1
	fmt.Printf("Subscriber Thread Args: timeout_sec<%d>\n", pollSubArgs.TimeoutSec)

	time.Sleep(1 * time.Second)

	StartTimestamp = getCurrentTimestamp("Start Time => ")

	if numPublications > 0 {
        go threadPublisher(start_channel, &pubArg)
	}

	if numPollSubscriptions > 0 {
        go threadSubscriberPoll(start_channel, &pollSubArgs)
	}

    close(start_channel)

	for {
		published := atomic.LoadInt64(&TotalPublished)
		prevReceived := atomic.LoadInt64(&TotalPollReceived) + atomic.LoadInt64(&TotalAsyncReceived)

		ndw.NDW_LOGX("---> main(): Before Sleep<%d>: total_published<%d> prev_received<%d>\n",
                        sleepWaitTime, published, prevReceived)

		time.Sleep(time.Duration(sleepWaitTime) * time.Second)

		published = atomic.LoadInt64(&TotalPublished)
		received := atomic.LoadInt64(&TotalPollReceived) + atomic.LoadInt64(&TotalAsyncReceived)

		ndw.NDW_LOGX("---> main(): After Sleep<%d> total_published<%d> prev_received<%d> received<%d>\n",
                        sleepWaitTime, published, prevReceived, received)
		if prevReceived == received {
			break
		} else {
			prevReceived = received
		}
	}

	EndTimestamp =   getCurrentTimestamp("End Time =>   ")
    printStatistics()

	ndw.NDW_LOGX("main(): At end: total received<%d>\n", atomic.LoadInt64(&TotalPollReceived)+atomic.LoadInt64(&TotalAsyncReceived))

shutdown:
	return 0
}

func main() {
    os.Exit(ndw.RunNDWApp(testInitialize))
}


