package queue

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

var ErrDuplicate = errors.New("queue: message with such name already exists")

type Message struct {
	Id string

	// An unique name for the message.
	Name string

	// Delay specifies the duration the queue must wait
	// before executing the message.
	Delay time.Duration

	Args []interface{}
	Body string

	ReservationId string

	// The number of times the message has been reserved or released.
	ReservedCount int

	values map[string]interface{}
}

func NewMessage(args ...interface{}) *Message {
	return &Message{
		Args: args,
	}
}

func (m *Message) String() string {
	return fmt.Sprintf("Message<Id=%q Name=%q>", m.Id, m.Name)
}

// SetDelayName sets delay and unique message name using the args.
func (m *Message) SetDelayName(delay time.Duration, args ...interface{}) {
	m.Name = argsName(append(args, timeSlot(delay)))
	m.Delay = delay
	// Some random delay to better distribute the load.
	m.Delay += time.Duration(rand.Intn(5)+1) * time.Second
}

func (m *Message) MarshalArgs() (string, error) {
	return encodeArgs(m.Args)
}

func (m *Message) SetValue(name string, value interface{}) {
	if m.values == nil {
		m.values = make(map[string]interface{})
	}
	m.values[name] = value
}

func (m *Message) Value(name string) interface{} {
	return m.values[name]
}

func timeSlot(resolution time.Duration) int64 {
	resolutionInSeconds := int64(resolution / time.Second)
	if resolutionInSeconds <= 0 {
		return 0
	}
	return time.Now().Unix() / resolutionInSeconds
}

func argsName(args []interface{}) string {
	b, _ := msgpack.Marshal(args...)
	return string(b)
}
