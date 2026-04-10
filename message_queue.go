package nosrabbitmq

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/xerrors"
)

type MessageQueue struct {
	identifier Identifier
	signalChan chan<- Signal
	connection *Connection
	ch         *amqp091.Channel
	config     MessageQueueConfig
	handlers   []HandlerFunc

	deliveries <-chan amqp091.Delivery
	ctx        context.Context
	cancel     context.CancelFunc
	isClosed   atomic.Bool
	wg         sync.WaitGroup
}

func NewMessageQueue(connection *Connection, config MessageQueueConfig, handlers ...HandlerFunc) *MessageQueue {
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

	for {
		select {
		case <-m.ctx.Done():
			return
		case d, ok := <-m.deliveries:
			if !ok {
				// Delivery channel closed unexpectedly
				m.sendSignal(FailureSignalLevel, "delivery channel closed", nil)
				return
			}

			m.processDelivery(&d)
		}
	}
}

func (m *MessageQueue) processDelivery(d *amqp091.Delivery) {
	c := NewContext(m.ctx, d, m.handlers)
	c.Next()

	if !m.config.Consumer.AutoAck && c.GetError() == nil {
		_ = d.Ack(false)
	}
}

func (m *MessageQueue) sendSignal(level SignalLevel, message string, err error) {
	if m.signalChan != nil {
		m.signalChan <- Signal{
			Level:      level,
			Identifier: m.identifier,
			Message:    message,
			Error:      err,
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

func (m *MessageQueue) SetSignalChan(ch chan<- Signal) {
	m.signalChan = ch
}
