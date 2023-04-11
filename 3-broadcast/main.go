package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/exp/slices"
)

type BroadcastJob struct {
  Destination string
  Value int64
  SkipIds []string
}

type TopologyMsgBody struct {
  Topology map[string][]string `json:"topology"`
}

type BroadcastMsgBody struct {
  Message int64 `json:"message"`
  SkipIds []string `json:"skip_ids,omitempty"` 
}

func broadcaster(n *maelstrom.Node, jobs chan *BroadcastJob){
  for broadcastJob := range jobs {
    msgBody := make(map[string]interface{})
    msgBody["type"] = "broadcast"
    msgBody["message"] = broadcastJob.Value
    msgBody["skip_ids"] = broadcastJob.SkipIds

    n.Send(broadcastJob.Destination, msgBody)
  }
}

func main() {
  n := maelstrom.NewNode()
  neighbors := []string{}
  messagesMutex := sync.RWMutex{}
  messages := []int64{}
  jobs := make(chan *BroadcastJob, 10)

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

    messagesMutex.Lock()
    if !slices.Contains(messages, broadcastMsgBody.Message) {
      messages = append(messages, broadcastMsgBody.Message)
    }
    messagesMutex.Unlock()

    skipIds := append(broadcastMsgBody.SkipIds, n.ID())
    for _, neighbor := range neighbors {
      if !slices.Contains(skipIds, neighbor) {
        jobs <- &BroadcastJob{
          Value: broadcastMsgBody.Message,
          Destination: neighbor,
          SkipIds: skipIds,
        }
      }
    }

    res := make(map[string]interface{})
    res["type"] = "broadcast_ok"
    return n.Reply(msg, res)
  })

  if err := n.Run(); err != nil {
    log.Fatal(err)
  } 
}

