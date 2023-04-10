package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"golang.org/x/exp/slices"
)

func BroadcastValue(n *maelstrom.Node, neighbourId string, value float64, skipIds []string) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broadcastBody := make(map[string]interface{})
	broadcastBody["skip_ids"] = append(skipIds, n.ID())
	broadcastBody["type"] = "broadcast"
	broadcastBody["message"] = value
	msgReply, err := n.SyncRPC(ctx, neighbourId, broadcastBody)
	msgReplyType := gjson.GetBytes(msgReply.Body, "type").Str

	return err != nil && msgReplyType != "broadcast_ok"
}

type ResendJob struct {
	NeighbourId string
	Value float64
	SkipIds []string
}

func main() {
	n := maelstrom.NewNode()
	values := []float64{}
	neighbours := []string{}
	resendChan := make(chan *ResendJob, 100)

	go func() {
		for resendJob := range resendChan {
			if ok := BroadcastValue(n, resendJob.NeighbourId, resendJob.Value, resendJob.SkipIds); !ok {
				time.Sleep(1)
				resendChan <- resendJob
			}
		}
	}()

	n.Handle("topology", func(msg maelstrom.Message) error {
		var msgBody map[string]interface{}
		json.Unmarshal(msg.Body, &msgBody)
		topologyPath := fmt.Sprintf("topology.%s", n.ID())
		_neighbours := gjson.GetBytes(msg.Body, topologyPath).Array()
		neighbours = lo.Map(_neighbours, func (res gjson.Result, _ int) string {
			return res.Str
		}) 
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

		for _, neighbourId := range neighbours {
			if slices.Contains(skipIds, neighbourId) {
				continue
			}

			go func(nodeId string) {
				if ok := BroadcastValue(n, nodeId, value, skipIds); !ok {
					time.Sleep(1)
					resendChan <- &ResendJob{NeighbourId: nodeId, Value: value, SkipIds: skipIds}
				}
			}(neighbourId)
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
