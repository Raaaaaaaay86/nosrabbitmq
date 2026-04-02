package nosrabbitmq

import (
	"context"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/xerrors"
)

type BatchMessageQueue struct {
	MessageQueue
	batchHandlers []BatchHandlerFunc
}

func NewBatchMessageQueue(connection *amqp091.Connection, config MessageQueueConfig, handlers ...BatchHandlerFunc) *BatchMessageQueue {
	return &BatchMessageQueue{
		MessageQueue: MessageQueue{
			connection: connection,
			config:     config,
		},
		batchHandlers: handlers,
	}
}

func (m *BatchMessageQueue) Start(ctx context.Context) error {
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
	go m.listenBatch()

	return nil
}

func (m *BatchMessageQueue) listenBatch() {
	defer m.wg.Done()

	batchSize := m.config.GetBatchSize()
	batchTimeout := m.config.GetBatchTimeout()

	buffer := make([]*amqp091.Delivery, 0, batchSize)
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}

		deliveries := make([]*amqp091.Delivery, len(buffer))
		copy(deliveries, buffer)

		m.processBatch(deliveries)

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
				m.sendSignal(FailureSignalLevel, "delivery channel closed", nil)
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

func (m *BatchMessageQueue) processBatch(deliveries []*amqp091.Delivery) {
	c := NewBatchContext(m.ctx, deliveries, m.batchHandlers)
	c.Next()

	if !m.config.Consumer.AutoAck && c.GetError() == nil {
		for _, d := range deliveries {
			_ = d.Ack(false)
		}
	}
}
