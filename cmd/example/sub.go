package example

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/oliverkra/gqm/client"
)

type SubOptions struct {
	GrpcAddr string
	Queue    string
	Group    string
}

func RunSub(opts SubOptions) error {
	c := client.New(opts.GrpcAddr)

	if err := c.SubscribeGroup(opts.Queue, opts.Group, func(msg *client.SubscribeResponse) {
		log.Println("SUB Payload:", string(msg.GetMessage().Data))
	}); err != nil {
		return err
	}
	log.Println("subscribed to queue:", opts.Queue, ", group:", opts.Group)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(ch)
	<-ch
	return nil
}
