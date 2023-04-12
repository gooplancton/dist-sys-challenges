package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/exp/slices"
)

type BroadcastJob struct {
  Destination string
  Value int64
}

type TopologyMsgBody struct {
  Topology map[string][]string `json:"topology"`
}

type BroadcastMsgBody struct {
  Message int64 `json:"message"`
}

func broadcaster(n *maelstrom.Node, jobs chan *BroadcastJob){
  for broadcastJob := range jobs {
    waitAckChan := make(chan bool)
    msgBody := make(map[string]interface{})
    msgBody["type"] = "broadcast"
    msgBody["message"] = broadcastJob.Value

    n.RPC(broadcastJob.Destination, msgBody, func(res maelstrom.Message) error {

      var resBody map[string]interface{}
      err := json.Unmarshal(res.Body, &resBody)
      ok := err == nil && resBody["type"] == "broadcast_ok"
      waitAckChan <- ok
      close(waitAckChan)
      
      return nil
    })

    select {
    case ok := <- waitAckChan:
      if !ok {
        jobs <- broadcastJob
      }
    case <- time.After(50*time.Millisecond):
      fmt.Fprintln(os.Stderr, "resending value")
      jobs <- broadcastJob
    }
  }
}

func main() {
  n := maelstrom.NewNode()
  neighbors := []string{}
  messagesMutex := sync.RWMutex{}
  messages := []int64{}
  jobs := make(chan *BroadcastJob, 100)

  for w := 0; w < 10; w++ {
    go broadcaster(n, jobs)
  }

  n.Handle("topology", func(msg maelstrom.Message) error {
    var topologyMsgBody TopologyMsgBody
    if err := json.Unmarshal(msg.Body, &topologyMsgBody); err != nil {
      return err
    }

    neighbors = topologyMsgBody.Topology[n.ID()]
    res := make(map[string]interface{})
    res["type"] = "topology_ok"
    return n.Reply(msg, res) 
  })

  n.Handle("read", func(msg maelstrom.Message) error {
    messagesMutex.RLock()
    res := make(map[string]interface{})
    res["type"] = "read_ok"
    res["messages"] = messages
    err := n.Reply(msg, res)

    messagesMutex.RUnlock()
    return err
  })

  n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
    return nil
  })

  n.Handle("broadcast", func(msg maelstrom.Message) error {
    var broadcastMsgBody BroadcastMsgBody
    if err := json.Unmarshal(msg.Body, &broadcastMsgBody); err != nil {
      return err
    }

    res := make(map[string]interface{})
    res["type"] = "broadcast_ok"

    messagesMutex.Lock()
    if slices.Contains(messages, broadcastMsgBody.Message) {
      messagesMutex.Unlock()
      return n.Reply(msg, res)
    }
    messages = append(messages, broadcastMsgBody.Message)
    messagesMutex.Unlock()

    for _, neighbor := range neighbors {
      jobs <- &BroadcastJob{
        Value: broadcastMsgBody.Message,
        Destination: neighbor,
      }
    }

    return n.Reply(msg, res)
  })

  if err := n.Run(); err != nil {
    log.Fatal(err)
  } 
}

