package example

import (
	"fmt"
	"log"

	"github.com/oliverkra/gqm/client"
)

type PubOptions struct {
	GrpcAddr string
	Queue    string
	Count    int
}

func RunPub(opts PubOptions) error {
	c := client.New(opts.GrpcAddr)

	log.Println("publishing to queue:", opts.Queue, "with", opts.Count, "messages")
	for i := 0; opts.Count == -1 || i < opts.Count; i++ {
		if err := c.Publish(opts.Queue, []byte(fmt.Sprintf("msg-%d", i+1))); err != nil {
			return err
		}
	}

	return nil
}
