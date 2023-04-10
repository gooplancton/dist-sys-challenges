package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/syncmap"
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

func main() {
	n := maelstrom.NewNode()
	values := []float64{}
	notAcknowledged := syncmap.Map{}

	// go func(){
	//   for {
	//     for neighbourId, vals := range notAcknowledged {
	//       for i, val := range vals {
	//         go func(neighbourId string, val float64, i int) {
	//           if ok := BroadcastValue(n, neighbourId, val, []string{n.ID()}); ok {
	//             notAcknowledged[neighbourId] = slices.Delete(notAcknowledged[neighbourId], i, i+1)
	//           }
	//         }(neighbourId, val, i)
	//       }
	//     }

	//     time.Sleep(2)
	//   }
	// }()

	n.Handle("topology", func(msg maelstrom.Message) error {
		var msgBody map[string]interface{}
		json.Unmarshal(msg.Body, &msgBody)
		topologyPath := fmt.Sprintf("topology.%s", n.ID())
		neighbours := gjson.GetBytes(msg.Body, topologyPath).Array()
		for _, neighbourId := range neighbours {
			notAcknowledged.Store(neighbourId.Str, []float64{})
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

		notAcknowledged.Range(func(neighbourId, missedValues any) bool {
			_neighbourId := neighbourId.(string)
			if slices.Contains(skipIds, _neighbourId) {
				return true
			}

			go func(nodeId string) {
				if ok := BroadcastValue(n, nodeId, value, skipIds); !ok {
					_missedValues := missedValues.([]float64)
					notAcknowledged.Store(nodeId, append(_missedValues, value))
				}
			}(_neighbourId)

			return true
		})

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
