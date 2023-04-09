package main

import (
	"fmt"
	"log"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)


func main() {
  n := maelstrom.NewNode()
  n.Handle("generate", func(msg maelstrom.Message) error {
    base, err := uuid.NewUUID()
    if err != nil {
      return err
    }

    fullId := fmt.Sprintf("%s-%s", n.ID(), base)
    body := make(map[string]string)

    body["type"] = "generate_ok" 
    body["id"] = fullId

    return n.Reply(msg, body)
  })

  if err := n.Run(); err != nil {
    log.Fatal(err)
  }
}
