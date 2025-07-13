package main


import (
    "fmt"
    "os"
    "ndw/ndw"
)

var total_msgs_to_publish = 0
var approximate_send_msg_bytes = 0

const print_frequency int64 = 100000

func ndw_AsyncMessageHandler(topicData *ndw.NDW_TopicData, msg *ndw.NDW_ReceivedMsg) {

    if topicData.TotalMsgsReceived < 100 || topicData.TotalMsgsReceived%print_frequency == 0 {
        fmt.Printf("✅ Message received on topic %s (%s.%s)\n",
            topicData.TopicName, topicData.DomainName, topicData.ConnectionName)
        fmt.Printf("📦 Payload (%d bytes): %s\n", msg.MsgSize, string(msg.MsgBody))
        ndw.NDW_PrintReceivedMsg(msg);
    }

    ret := ndw.NDW_CommitLastMsg(topicData)
    if (0 != ret) {
        fmt.Printf("*** ERROR*** Failed to CommitLastMsg!\n")
    }
}

func publishMessages() int {

    var sequence_number int64 = 0
    for i := 0; i < total_msgs_to_publish; i++ {
        for j := range ndw.TopicList {
            topic := &ndw.TopicList[j]

            sequence_number = topic.TotalMsgsPublished + 1
            json_msg, _ := ndw.CreateTestJSONMessageOfSize(
                            topic, "Sample Message from GO: ", sequence_number, approximate_send_msg_bytes)

            payload := []byte(json_msg)
            cxt := ndw.CreateOutMsgCxt(topic, payload, ndw.NDW_DEFAULT_MSG_HEADER, ndw.NDW_ENCODING_FORMAT_JSON)
            if nil == cxt {
                ndw.NDW_LOGERR("Failed to create message for %s\n", topic.ToString)
                return -1
            }

            if 0 != ndw.NDW_PublishMsg(topic) {
                ndw.NDW_LOGERR("Failed to publish message on %s\n", topic.ToString)
                return -1
            }

            if topic.TotalMsgsPublished < 100 || topic.TotalMsgsPublished%print_frequency == 0 {
                ndw.NDW_LOGX(">>> Publishing Message: <%s>\n", json_msg)
            }
        }
    }

    return 0
}


func appMain() int {

    ndw.NDW_LOGX("Arguments...\n")
    args := os.Args
    fmt.Println("Executable:", args[0])  // the program name
    for i, arg := range args[1:] {
        ndw.NDW_LOG("Arg[%d]: %s\n", i+1, arg)
    }

    ndw.NDW_LOGX("\nPrintCurrentTime()...\n")
    ndw.PrintCurrentTime()

    
    fmt.Printf("\nSplitStringToTokens()...\n")
    topics := []string{"DomainA^NatsConn1^TopicA", "DomainA", "DomainA^",
                            "DomainA^NATSConn1", "DomainA^NatsConn1^"}
    for i := 0; i < len(topics); i++ {
        tokens, num_tokens := ndw.SplitStringToTokens(topics[i], "^")
        fmt.Printf("%s ==>", topics[i])
        for j := 0; j < num_tokens; j++ {
            fmt.Printf(" [%d]: %s", j, tokens[j])
        }
    fmt.Printf("\n")
    }
    fmt.Printf("\n")

    fmt.Printf("\nParsing Program Arguments()...\n")

        msgCount, msgSize, _, topicData, err := ndw.ParseMessageArgs(os.Args[1:])
    if err != nil {
        fmt.Println("❌ Error:", err)
        return 1
    }

    total_msgs_to_publish = msgCount
    approximate_send_msg_bytes = msgSize

    fmt.Printf("✅ Total Messages to Publish: %d\n", total_msgs_to_publish)
    fmt.Printf("✅ Approximate Message Size:  %d\n", approximate_send_msg_bytes)

    fmt.Printf("✅ Number of Topics Parsed: %d\n", len(topicData))
    for i, cfg := range topicData {
        fmt.Printf("  [%d] Domain: <%s> | Connection: <%s> | Topic: <%s>\n",
            i+1, cfg.DomainName, cfg.ConnectionName, cfg.TopicName)
    }

    ndw.NDW_RegisterGoAsyncMsgCallback(ndw_AsyncMessageHandler);

    if 0 != ndw.NDW_Initialize() {
        return 1
    }

    if 0 != ndw.NDW_ConnectAll() {
        ndw.NDW_LOGERR("** ERROR: Failed to make Connections!\n")
        return 1
    }

    if 0 != ndw.NDW_SubscribeAsyncAll() {
        ndw.NDW_LOGERR("** ERROR: Failed to SubscribeAsyncAll()!\n")
        return 1
    }

    time_duration := ndw.NDW_StartTimeDuration()

    if 0 != publishMessages() {
        ndw.NDW_LOGERR("** ERROR: Failed to Publish Messages!\n")
        return 1
    }

    ndw.NDW_SleepForMessages(2, 6)
    ndw.NDW_EndTimeDuration(time_duration)

    fmt.Printf("\n======================\n\n")

    ndw.NDW_PrintTimeDuration(time_duration)
    fmt.Printf("✅ Total Messages Published: %d\n", ndw.GetTotalMsgsPublished())
    fmt.Printf("✅ Approximate Message Size:  %d\n", ndw.GetTotalMsgsReceived())
    for i := range ndw.TopicList {
        topic := &ndw.TopicList[i]
        fmt.Printf("Topic Name: %s Total Messages Published: %d Total Messages Received: %d\n",
                    topic.TopicName, topic.TotalMsgsPublished, topic.TotalMsgsReceived)
    }
    fmt.Printf("\n");

    return 0 
}

func main() {
    os.Exit(ndw.RunNDWApp(appMain))
}

