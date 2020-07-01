package queue

import (
	"github.com/paulbellamy/ratecounter"
)

type Stats interface {
	PublishPerSecond() int64
	MessagesCount() uint64
	SubscriptionsCount() int
}

type stats struct {
	pubPerSecond       *ratecounter.RateCounter
	messagesCount      uint64
	subscriptionsCount int
}

func (qs *stats) PublishPerSecond() int64 { return qs.pubPerSecond.Rate() }
func (qs *stats) MessagesCount() uint64   { return qs.messagesCount }
func (qs *stats) SubscriptionsCount() int { return qs.subscriptionsCount }
