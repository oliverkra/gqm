package queue

import (
	"sync"
	"time"

	"github.com/paulbellamy/ratecounter"
)

type Queue interface {
	Name() string
	Stats() Stats

	Publish(data []byte) Message
	Subscribe(Subscription)
}

func New(name string) Queue {
	return &queue{
		name: name,
		stats: &stats{
			pubPerSecond: ratecounter.NewRateCounter(time.Second),
		},
		messages:      &linkedMessages{mu: &sync.Mutex{}},
		subscriptions: map[Subscription]*subscriptionControl{},
		muPub:         &sync.Mutex{},
		muSub:         &sync.Mutex{},
	}
}

type queue struct {
	name          string
	stats         *stats
	messages      *linkedMessages
	subscriptions map[Subscription]*subscriptionControl
	muPub         *sync.Mutex
	muSub         *sync.Mutex
}

func (q *queue) Name() string { return q.name }
func (q *queue) Stats() Stats { return q.stats }

func (q *queue) Publish(data []byte) Message {
	q.muPub.Lock()
	msg := q.messages.append(newMessage(q.messages.length+1, data))
	q.stats.messagesCount = q.messages.length
	q.stats.pubPerSecond.Incr(1)
	q.muPub.Unlock()

	for _, sc := range q.subscriptions {
		go sc.notify()
	}

	return msg
}

func (q *queue) Subscribe(s Subscription) {
	q.muSub.Lock()
	sc := newSubscriptionControl(q, s)
	q.subscriptions[s] = sc
	q.stats.subscriptionsCount++
	go sc.run()
	q.muSub.Unlock()
}

func (q *queue) getNextMessage(from *message, seq uint64) *message {
	q.muPub.Lock()
	defer q.muPub.Unlock()

	if from != nil && from.sequence == seq {
		return from.next
	}

	if q.messages.length > 0 {
		if seq == 0 {
			return q.messages.first
		}
		c := q.messages.first
		for c != nil && c.sequence <= seq {
			c = c.next
		}
		return c
	}
	return nil
}
