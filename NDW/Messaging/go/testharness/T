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
	PublishFunction        func(topic *C.ndw_Topic_T) C.INT_T
	SubscribeFunction      func(topic *C.ndw_Topic_T) C.INT_T
	PollFunction           func(topic *C.ndw_Topic_T, timeoutUs C.LONG_T) C.INT_T
	PollCommitFunction     func(topic *C.ndw_Topic_T) C.INT_T
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
	ThreadID  C.pthread_t
	NumMsgs   int
	MsgSize   int
	DelayUs   uint
	RetCode   int
}

type PollSubscriberArgs struct {
	ThreadID    C.pthread_t
	TimeoutSec  int
	RetCode     int
}

var (
	TotalPublished     atomic.Int64
	TotalReceived      atomic.Int64
	TotalAsyncReceived atomic.Int64
	TotalPollReceived  atomic.Int64

	NumMessages        = 1
	MessageSize        = 1024
	PollTimeoutSec     = 6
	PublisherDelayUs   = 10
	PollDelayUs        = 10

	IsAsyncMessageHandlingEnabled = false

	PubArgs      = &PublisherArgs{}
	PollSubArgs  = &PollSubscriberArgs{}
)

