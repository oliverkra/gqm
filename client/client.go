package client

import (
	"context"
	"encoding/json"

	"github.com/oliverkra/gqm/pb"
	"google.golang.org/grpc"
)

type SubscribeResponse = pb.SubscribeResponse

type SubscriptionHandler func(*SubscribeResponse)

func New(address string) *Client {
	cnn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	return &Client{pb.NewQueueServiceClient(cnn)}
}

type Client struct {
	c pb.QueueServiceClient
}

func (c *Client) Publish(topic string, payload []byte) error {
	_, err := c.c.Publish(context.Background(), &pb.PublishRequest{
		Topic:   topic,
		Payload: payload,
	})
	return err
}

func (c *Client) PublishJSON(topic string, v interface{}) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Publish(topic, payload)
}

func (c *Client) Subscribe(topic string, h SubscriptionHandler) error {
	sc, err := c.c.Subscribe(context.Background())
	if err != nil {
		return err
	}
	if err := sc.Send(&pb.SubscribeRequest{
		Command: &pb.SubscribeRequest_Subscribe{
			Subscribe: &pb.SubscribeRequest_SubscribeCommand{
				Topic: topic,
			},
		},
	}); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-sc.Context().Done():
				return
			default:
				msg, err := sc.Recv()
				if err != nil {
					return
				}
				h(msg)
			}
		}
	}()
	return nil
}
