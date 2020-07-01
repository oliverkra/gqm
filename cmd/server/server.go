package server

import (
	"context"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oliverkra/gqm/pb"
	"github.com/oliverkra/gqm/queue"
)

type Options struct {
	GrpcAddr string
}

func Run(ctx context.Context, opts Options) error {
	ln, err := net.Listen("tcp4", opts.GrpcAddr)
	if err != nil {
		return err
	}

	log.Println("listening on:", opts.GrpcAddr)
	return newServer().Serve(ln)
}

func newServer() *grpc.Server {
	queues := map[string]queue.Queue{}
	s := grpc.NewServer()
	pb.RegisterQueueServiceServer(s, &server{queues, &sync.Mutex{}})
	return s
}

var _ pb.QueueServiceServer = &server{}

type server struct {
	queues map[string]queue.Queue
	mu     *sync.Mutex
}

func (s *server) getTopic(name string) queue.Queue {
	s.mu.Lock()
	defer s.mu.Unlock()

	q, has := s.queues[name]
	if !has {
		q = queue.New(name)
		s.queues[name] = q
		// return nil, status.Error(codes.NotFound, "topic not exist")
	}
	return q
}

func (s *server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	q := s.getTopic(req.Topic)
	msg := q.Publish(req.Payload)
	return &pb.PublishResponse{Uuid: msg.UUID()}, nil
}

func (s *server) Subscribe(srv pb.QueueService_SubscribeServer) error {
	log.Println("client connecting...")
	first, err := srv.Recv()
	if err != nil {
		return err
	}
	command, ok := first.GetCommand().(*pb.SubscribeRequest_Subscribe)
	if !ok {
		return status.Error(codes.InvalidArgument, "wrong first command sended")
	}

	q := s.getTopic(command.Subscribe.Topic)
	log.Println("client connected")
	return (&subscription{q, srv}).run()
}

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

func (s *subscription) run() error {
	s.q.Subscribe(s)
	defer func() {
		log.Println("client disconnecting...")
		s.q.Unsubscribe(s)
		log.Println("client disconnected")
	}()

	for {
		select {
		case <-s.srv.Context().Done():
			return nil

		default:
			command, err := s.srv.Recv()
			if err != nil {
				return err
			}

			switch cmd := command.GetCommand().(type) {
			case *pb.SubscribeRequest_Close:
				log.Println("Close command", cmd)
				return nil
			default:
				log.Println("Unexpected command")
			}
		}
	}
}
