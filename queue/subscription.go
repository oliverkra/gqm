package queue

import (
	"context"
	"log"
	"sync"
)

type Subscription interface {
	Start() uint64
	Send(Message) error
}

func newSubscriptionControl(q *queue, s Subscription) *subscriptionControl {
	ctx, stopCtx := context.WithCancel(context.Background())
	return &subscriptionControl{
		q:        q,
		s:        s,
		mu:       &sync.Mutex{},
		running:  false,
		notified: make(chan struct{}, 1),
		stopped:  make(chan struct{}, 1),
		ctx:      ctx,
		stopCtx:  stopCtx,
	}
}

type subscriptionControl struct {
	q *queue
	s Subscription

	notified chan struct{}
	stopped  chan struct{}
	mu       *sync.Mutex
	running  bool
	ctx      context.Context
	stopCtx  func()
}

func (sc *subscriptionControl) notify() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !sc.running {
		sc.notified <- struct{}{}
	}
}

func (sc *subscriptionControl) setRunning(v bool) {
	sc.mu.Lock()
	sc.running = v
	sc.mu.Unlock()
}

func (sc *subscriptionControl) run() {
	var (
		last *message
		seq  uint64 = sc.s.Start()
	)
	defer func() {
		sc.stopped <- struct{}{}
	}()

	go sc.notify()

	for {
		select {
		case <-sc.ctx.Done():
			return

		case <-sc.notified:
			sc.setRunning(true)

			for cur := sc.q.getNextMessage(last, seq); cur != nil; cur = cur.next {
				select {
				case <-sc.ctx.Done():
					goto stop

				default:
					if err := sc.s.Send(cur); err != nil {
						log.Println(err)
					}
					seq = cur.sequence
					last = cur
				}
			}

		stop:
			sc.setRunning(false)
		}
	}
}

func (sc *subscriptionControl) stop() {
	sc.stopCtx()
	<-sc.stopped
}
