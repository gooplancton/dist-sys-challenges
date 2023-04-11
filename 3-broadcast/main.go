package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"golang.org/x/exp/slices"
)

func BroadcastValue(n *maelstrom.Node, neighbourId string, value int64, skipIds []string) bool {
	broadcastBody := make(map[string]interface{})
	broadcastBody["type"] = "broadcast"
	broadcastBody["message"] = value
	replyChan := make(chan *maelstrom.Message, 1)
	n.RPC(neighbourId, broadcastBody, func(msg maelstrom.Message) error {
		replyChan <- &msg
		close(replyChan)

		return nil
	})

	select {
	case reply := <-replyChan:
		msgReplyType := gjson.GetBytes(reply.Body, "type").Str
		return msgReplyType == "broadcast_ok"

	case <- time.After(1*time.Second/2):
		return false	
	}
}

type ResendJob struct {
	NeighbourId string
	Value       int64
	SkipIds     []string
}

func main() {
	n := maelstrom.NewNode()
	values := []int64{}
	neighbours := []string{}

	n.Handle("topology", func(msg maelstrom.Message) error {
		var msgBody map[string]interface{}
		json.Unmarshal(msg.Body, &msgBody)
		topologyPath := fmt.Sprintf("topology.%s", n.ID())
		_neighbours := gjson.GetBytes(msg.Body, topologyPath).Array()
		neighbours = lo.Map(_neighbours, func(res gjson.Result, _ int) string {
			return res.Str
		})
		msgBody["type"] = "topology_ok"
		delete(msgBody, "topology")

		return n.Reply(msg, msgBody)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var msgBody map[string]interface{}
		json.Unmarshal(msg.Body, &msgBody)
		value := gjson.GetBytes(msg.Body, "message").Int()
		_skipIds := gjson.GetBytes(msg.Body, "skip_ids").Array()
		var skipIds []string = lo.Map(_skipIds, func(r gjson.Result, _ int) string {
			return r.Str
		})

		if !slices.Contains(values, value) {
			values = append(values, value)

			for _, neighbourId := range neighbours {
				if slices.Contains(skipIds, neighbourId) {
					continue
				}

				skipIds = append(skipIds, n.ID())
				ok := BroadcastValue(n, neighbourId, value, skipIds)
				for !ok {
					ok = BroadcastValue(n, neighbourId, value, skipIds)
				}
			}
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
