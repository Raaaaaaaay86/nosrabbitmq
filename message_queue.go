package nosrabbitmq

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/xerrors"
)

type MessageQueue struct {
	identifier Identifier
	connection *amqp091.Connection
	ch         *amqp091.Channel
	config     MessageQueueConfig
	handlers   []HandlerFunc

	deliveries <-chan amqp091.Delivery
	ctx        context.Context
	cancel     context.CancelFunc
	isClosed   atomic.Bool
	wg         sync.WaitGroup
}

func NewMessageQueue(connection *amqp091.Connection, config MessageQueueConfig, handlers ...HandlerFunc) *MessageQueue {
	return &MessageQueue{
		config:     config,
		connection: connection,
		handlers:   handlers,
	}
}

func (m *MessageQueue) Start(ctx context.Context) error {
	m.ctx, m.cancel = context.WithCancel(ctx)

	ch, err := m.connection.Channel()
	if err != nil {
		return xerrors.Errorf("failed to open channel: %w", err)
	}
	m.ch = ch

	if err := m.config.Exchange.Declare(m.ch); err != nil {
		return xerrors.Errorf("failed to declare exchange: %w", err)
	}

	queue, err := m.config.Queue.Declare(m.ch)
	if err != nil {
		return xerrors.Errorf("failed to declare queue: %w", err)
	}

	if err := m.config.Bind(m.ch); err != nil {
		return xerrors.Errorf("failed to bind queue (%s) to exchange (%s): %w", m.config.Queue.Name, m.config.Exchange.Name, err)
	}

	deliveries, err := m.config.Consumer.Consume(m.ctx, m.ch, queue)
	if err != nil {
		return xerrors.Errorf("failed to receive deliveries: %w", err)
	}

	m.deliveries = deliveries

	m.wg.Add(1)
	go m.listen()

	return nil
}

func (m *MessageQueue) listen() {
	defer m.wg.Done()

	batchSize := m.config.GetBatchSize()
	batchTimeout := m.config.GetBatchTimeout()

	buffer := make([]*amqp091.Delivery, 0, m.config.GetBatchSize())
	ticker := time.NewTicker(m.config.GetBatchTimeout())
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}

		deliveries := make([]*amqp091.Delivery, len(buffer))
		copy(deliveries, buffer)

		c := NewContext(m.ctx, deliveries, m.handlers)
		c.Next()

		buffer = buffer[:0]
		ticker.Reset(batchTimeout)
	}

	for {
		select {
		case <-m.ctx.Done():
			flush()
			return
		case d, ok := <-m.deliveries:
			if !ok {
				flush()
				return
			}

			dCopy := d
			buffer = append(buffer, &dCopy)

			if len(buffer) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (m *MessageQueue) Close() error {
	if m.isClosed.Swap(true) {
		return nil
	}

	if m.cancel != nil {
		m.cancel()
	}

	m.wg.Wait()

	if m.ch != nil {
		return m.ch.Close()
	}

	return nil
}

func (m *MessageQueue) GetIdentifier() Identifier {
	return m.identifier
}

func (m *MessageQueue) SetIdentifier(identifier Identifier) {
	m.identifier = identifier
}
