package queue

import (
	"sync"
)

type Subscription interface {
	Start() uint64
	Send(Message)
}

func newSubscriptionControl(q *queue, s Subscription) *subscriptionControl {
	return &subscriptionControl{
		q:        q,
		s:        s,
		mu:       &sync.Mutex{},
		running:  false,
		notified: make(chan struct{}, 1),
	}
}

type subscriptionControl struct {
	q *queue
	s Subscription

	notified chan struct{}
	mu       *sync.Mutex
	running  bool
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

	go sc.notify()

	for {
		select {
		case <-sc.notified:
			sc.setRunning(true)

			for cur := sc.q.getNextMessage(last, seq); cur != nil; cur = cur.next {
				go sc.s.Send(cur)
				seq = cur.sequence
				last = cur
			}

			sc.setRunning(false)
		}
	}
}
