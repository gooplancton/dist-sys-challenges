package main

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/tidwall/gjson"
	"golang.org/x/exp/slices"
  "github.com/samber/lo"
)


func main() {
	n := maelstrom.NewNode()
	values := []float64{}
  checkpoints := make(map[string]int)

	n.Handle("topology", func(msg maelstrom.Message) error {
		var msgBody map[string]interface{}
		json.Unmarshal(msg.Body, &msgBody)
    topologyPath := fmt.Sprintf("topology.%s", n.ID())
    neighbours := gjson.GetBytes(msg.Body, topologyPath).Array()
    for _, neighbourId := range(neighbours) {
      checkpoints[neighbourId.Str] = 0
    }
		msgBody["type"] = "topology_ok"
		delete(msgBody, "topology")

		return n.Reply(msg, msgBody)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var msgBody map[string]interface{}
		json.Unmarshal(msg.Body, &msgBody)
    value := gjson.GetBytes(msg.Body, "message").Float()
    _skipIds := gjson.GetBytes(msg.Body, "skip_ids").Array() 
    var skipIds []string = lo.Map(_skipIds, func(r gjson.Result, _ int) string {
      return r.Str
    })
		values = append(values, value)

    for neighbourId := range checkpoints {
      if slices.Contains(skipIds, neighbourId) {
        continue
      }
      
      broadcastBody := make(map[string]interface{})
      broadcastBody["skip_ids"] = append(skipIds, n.ID())
      broadcastBody["type"] = "broadcast"
      broadcastBody["message"] = value
      go n.Send(neighbourId, broadcastBody)
    }
    
		delete(msgBody, "message")
		msgBody["type"] = "broadcast_ok"

		return n.Reply(msg, msgBody)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var msgBody map[string]interface{}
		json.Unmarshal(msg.Body, &msgBody)

		msgBody["type"] = "read_ok"
		msgBody["messages"] = values

		return n.Reply(msg, msgBody)
	})

  n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
    return nil
  })

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

