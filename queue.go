package nosrabbitmq

import (
	"context"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type Identifier int64

type Queue interface {
	Start(context.Context) error
	Close() error
	GetIdentifier() Identifier
	SetIdentifier(Identifier)
	SetSignalChan(chan<- Signal)
}

type SignalLevel uint8

const (
	DebugSignalLevel   SignalLevel = 1
	InfoSignalLevel    SignalLevel = 2
	WarnSignalLevel    SignalLevel = 3
	ErrorSignalLevel   SignalLevel = 4
	FailureSignalLevel SignalLevel = 5
)

type Signal struct {
	Level      SignalLevel
	Identifier Identifier
	Message    string
	Error      error
}

type QueueFactory func() (Queue, error)

type MessageQueueConfig struct {
	Exchange ExchangeConfig
	Queue    QueueConfig
	Consumer ConsumerConfig

	RoutingKey   string
	NoWait       bool
	Table        amqp091.Table
	BatchSize    int
	BatchTimeout time.Duration
	HandlerFuncs []HandlerFunc
}

func (b MessageQueueConfig) Bind(channel *amqp091.Channel) error {
	return channel.QueueBind(b.Queue.Name, b.RoutingKey, b.Exchange.Name, b.NoWait, b.Table)
}

func (b MessageQueueConfig) GetBatchSize() int {
	if b.BatchSize <= 0 {
		return 1
	}
	return b.BatchSize
}

func (b MessageQueueConfig) GetBatchTimeout() time.Duration {
	if b.BatchTimeout <= 0 {
		return 5 * time.Second
	}
	return b.BatchTimeout
}

type QueueConfig struct {
	// Name is the name of exchange. Separate the namespace by using "."
	//
	// Example: $service_name.$feature.$task
	Name string
	// Durable
	//
	// true: Queue definition writes into disk. After RabbitMQ restart, exchange will remain.
	//
	// false: Queue definition writes into memory. After RabbitMQ restart, exchange will disappear.
	//
	// !NOTE: This only define how the exchange definition being stored. Doesn't mean the produced messages will be durable.
	// Message duration should be configured when publishing message.
	Durable bool
	// AutoDelete
	//
	// true: When there has no consumer to the queue, queue will be deleted right away.
	//
	// false: The queue will remain unless manually deletion
	AutoDelete bool
	// Exclusive
	//
	// true: Only the connection which declared can be the only one to access to this queue
	//
	// false: Every connections can access to this queue as long as the queue is declared by them.
	Exclusive bool
	// NoWait
	//
	// true: Not waiting for the result of exchange declaration
	//
	// false(suggested): waiting for the result of exchange declaration. Knowing the declaration result immediately.
	NoWait bool
	Args   amqp091.Table
}

func (q QueueConfig) Declare(channel *amqp091.Channel) (amqp091.Queue, error) {
	return channel.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, q.Args)
}

type ExchangeConfig struct {
	// Name is the name of exchange. Separate the namespace by using "."
	//
	// Example: $project_name.$module_name.$feature_or_entity.$type
	Name string
	// Type annotates the type of exchange (fanout, direct, topic, headers).
	Type ExchangeType
	// Durable
	//
	// true: Exchange definition writes into disk. After RabbitMQ restart, exchange will remain.
	//
	// false: Exchange definition writes into memory. After RabbitMQ restart, exchange will disappear.
	//
	// !NOTE: This only define how the exchange definition being stored. Doesn't mean the produced messages will be durable.
	Durable bool
	// AutoDelete
	//
	// true: When there has no queue biding to the exchange, exchange will be deleted right away.
	//
	// false: The exchange will remain unless manually deletion.
	AutoDelete bool
	// Internal
	//
	// true: Client cannot produce messages to this exchange. This exchange only receive messages from other exchange (Exchange-to-Exchange Binding)
	//
	// false(suggested): Normal exchange. Client can product message to this exchange.
	Internal bool
	// NoWait
	//
	// true: Not waiting for the result of exchange declaration
	//
	// false(suggested): waiting for the result of exchange declaration. Knowing the declaration result immediately.
	NoWait bool
	Args   amqp091.Table
}

func (e ExchangeConfig) Declare(channel *amqp091.Channel) error {
	if !e.Type.IsValid() {
		return nil
	}
	return channel.ExchangeDeclare(e.Name, string(e.Type), e.Durable, e.AutoDelete, e.Internal, e.NoWait, e.Args)
}

type ConsumerConfig struct {
	Name      string
	AutoAck   bool
	Exclusive bool
	NoWait    bool
	Args      amqp091.Table
}

func (c ConsumerConfig) Consume(ctx context.Context, channel *amqp091.Channel, queue amqp091.Queue) (<-chan amqp091.Delivery, error) {
	return channel.ConsumeWithContext(ctx, queue.Name, c.Name, c.AutoAck, c.Exclusive, false, c.NoWait, c.Args)
}
