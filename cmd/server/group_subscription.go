package server

import (
	"sync"

	"github.com/oliverkra/gqm/queue"
)

type groupSubscription struct {
	subscribers     map[queue.Subscription]struct{}
	subscribersList []queue.Subscription
	next            int
	mu              *sync.Mutex
}

func (gs *groupSubscription) Subscribe(s queue.Subscription) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if _, has := gs.subscribers[s]; !has {
		gs.subscribers[s] = struct{}{}
		gs.subscribersList = append(gs.subscribersList, s)
	}
}

func (gs *groupSubscription) Unsubscribe(s queue.Subscription) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if _, has := gs.subscribers[s]; has {
		delete(gs.subscribers, s)
		gs.subscribersList = []queue.Subscription{}
		for s := range gs.subscribers {
			gs.subscribersList = append(gs.subscribersList, s)
		}
	}
}

func (gs *groupSubscription) Start() uint64 { return 0 }

func (gs *groupSubscription) Send(msg queue.Message) error {
	gs.mu.Lock()
	s := gs.subscribersList[gs.next]
	gs.next = (gs.next + 1) % len(gs.subscribersList)
	gs.mu.Unlock()
	return s.Send(msg)
}
