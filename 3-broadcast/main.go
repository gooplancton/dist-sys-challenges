package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	values := []float64{}

	n.Handle("topology", func(msg maelstrom.Message) error {
		msgBody := make(map[string]interface{})
		msgBody["type"] = "topology_ok"

		return n.Reply(msg, msgBody)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var msgBody map[string]interface{}
		json.Unmarshal(msg.Body, &msgBody)
		value, ok := msgBody["message"].(float64)
		if !ok {
			return errors.New(fmt.Sprintf("invalid message field: %s", msgBody["message"]))
		}

		values = append(values, value)
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

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
