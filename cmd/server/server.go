package server

import (
	"context"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oliverkra/gqm/pb"
	"github.com/oliverkra/gqm/queue"
	"github.com/sirupsen/logrus"
)

type Options struct {
	GrpcAddr string
}

func Run(ctx context.Context, opts Options) error {
	ln, err := net.Listen("tcp4", opts.GrpcAddr)
	if err != nil {
		return err
	}

	logrus.Println("listening on:", opts.GrpcAddr)
	return newServer().Serve(ln)
}

func newServer() *grpc.Server {
	s := grpc.NewServer()
	pb.RegisterQueueServiceServer(s, &server{
		queues: map[string]*serverQueue{},
		mu:     &sync.Mutex{},
	})
	return s
}

var _ pb.QueueServiceServer = &server{}

type server struct {
	queues map[string]*serverQueue
	mu     *sync.Mutex
}

func (s *server) getTopic(name string) *serverQueue {
	s.mu.Lock()
	defer s.mu.Unlock()

	q, has := s.queues[name]
	if !has {
		q = &serverQueue{
			queue:  queue.New(name),
			groups: map[string]*groupSubscription{},
			mu:     &sync.Mutex{},
		}
		s.queues[name] = q
	}
	return q
}

func (s *server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	q := s.getTopic(req.Topic)
	msg := q.queue.Publish(req.Payload)
	return &pb.PublishResponse{Uuid: msg.UUID()}, nil
}

func (s *server) Subscribe(srv pb.QueueService_SubscribeServer) (err error) {
	logrus.Println("client connecting...")
	defer func() {
		if err != nil {
			logrus.WithError(err).Println("client disconnected with error")
		} else {
			logrus.Println("client disconnected")
		}
	}()

	first, err := srv.Recv()
	if err != nil {
		return err
	}
	command, ok := first.GetCommand().(*pb.SubscribeRequest_Subscribe)
	if !ok {
		return status.Error(codes.InvalidArgument, "wrong first command sended")
	}

	q := s.getTopic(command.Subscribe.Topic)
	sub := &subscription{q.queue, srv}

	if command.Subscribe.Group == "" {
		q.queue.Subscribe(sub)
		logrus.Println("client connected")
		defer func() {
			logrus.Println("client disconnecting...")
			q.queue.Unsubscribe(sub)
			logrus.Println("client disconnected")
		}()
	} else {
		g := q.getGroup(command.Subscribe.Group)
		if !q.queue.IsSubscribed(g) {
			q.queue.Subscribe(g)
		}

		g.Subscribe(sub)
		logrus.Println("client connected")
		defer func() {
			logrus.Println("client disconnecting...")
			g.Unsubscribe(sub)
			logrus.Println("client disconnected")
		}()
	}

	for {
		select {
		case <-srv.Context().Done():
			return nil

		default:
			command, err := srv.Recv()
			if err != nil {
				return err
			}

			switch cmd := command.GetCommand().(type) {
			case *pb.SubscribeRequest_Close:
				return nil
			default:
				logrus.Println("Unexpected command:", cmd)
			}
		}
	}
}

type serverQueue struct {
	queue  queue.Queue
	groups map[string]*groupSubscription
	mu     *sync.Mutex
}

func (sq *serverQueue) getGroup(name string) *groupSubscription {
	sq.mu.Lock()
	defer sq.mu.Unlock()

	g, has := sq.groups[name]
	if !has {
		g = &groupSubscription{
			subscribers:     map[queue.Subscription]struct{}{},
			subscribersList: []queue.Subscription{},
			mu:              &sync.Mutex{},
		}
		sq.groups[name] = g
	}
	return g
}
