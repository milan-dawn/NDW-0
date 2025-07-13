package main


import (
    "fmt"
    "os"
    "ndw/ndw"
)

func ndw_AsyncMessageHandler(topicData *ndw.NDW_TopicData, msg *ndw.NDW_ReceivedMsg) {
	fmt.Printf("✅ Message received on topic %s (%s.%s)\n",
		topicData.TopicName, topicData.DomainName, topicData.ConnectionName)
	fmt.Printf("📦 Payload (%d bytes): %s\n", msg.MsgSize, string(msg.MsgBody))
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

    fmt.Printf("✅ Message Count: %d\n", msgCount)
    fmt.Printf("✅ Message Size:  %d\n", msgSize)

    fmt.Println("✅ Number of Topics Parsed: %d", len(topicData))
    for i, cfg := range topicData {
        fmt.Printf("  [%d] Domain: <%s> | Connection: <%s> | Topic: <%s>\n",
            i+1, cfg.DomainName, cfg.ConnectionName, cfg.TopicName)
    }

    ndw.NDW_RegisterGoAsyncMsgCallback(ndw_AsyncMessageHandler);

    if 0 != ndw.NDW_Initialize() {
        return 1
    }

    fmt.Printf("\n======================\n\n")

    return 0 
}

func main() {
    os.Exit(ndw.RunNDWApp(appMain))
}

