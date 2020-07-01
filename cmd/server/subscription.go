package server

import (
	"github.com/oliverkra/gqm/pb"
	"github.com/oliverkra/gqm/queue"
)

type subscription struct {
	q   queue.Queue
	srv pb.QueueService_SubscribeServer
}

func (s *subscription) Start() uint64 { return 0 }

func (s *subscription) Send(msg queue.Message) error {
	return s.srv.Send(&pb.SubscribeResponse{
		Response: &pb.SubscribeResponse_Message{
			Message: &pb.Message{
				Uuid:     msg.UUID(),
				Sequence: msg.Sequence(),
				Data:     msg.Data(),
			},
		},
	})
}
