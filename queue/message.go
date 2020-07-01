package queue

import (
	"sync"

	"github.com/google/uuid"
)

type Message interface {
	UUID() string
	Sequence() uint64
	Data() []byte
}

func newMessage(sequence uint64, data []byte) *message {
	return &message{
		uuid:     uuid.New().String(),
		sequence: sequence,
		data:     data,
	}
}

type message struct {
	uuid     string
	sequence uint64
	data     []byte

	next *message
}

func (m *message) UUID() string     { return m.uuid }
func (m *message) Sequence() uint64 { return m.sequence }
func (m *message) Data() []byte     { return m.data }

type linkedMessages struct {
	first, last *message
	length      uint64
	mu          *sync.Mutex
}

func (lm *linkedMessages) append(m *message) *message {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.first == nil {
		lm.first = m
	} else {
		lm.last.next = m
	}
	lm.last = m
	lm.length++
	return m
}
