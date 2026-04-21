package nosrabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
)

type BatchMessageQueue struct {
	MessageQueue
	batchHandlers []BatchHandlerFunc
}

func NewBatchMessageQueue(connection *Connection, config MessageQueueConfig, handlers ...BatchHandlerFunc) *BatchMessageQueue {
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
	defer func() {
		if r := recover(); r != nil {
			slog.Error("batch message queue listening panic", "identifier", m.identifier, "recover", r)
		} else {
			slog.Info("batch message queue listening exited", "identifier", m.identifier)
		}
	}()
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
	ctx := context.Background()

	if m.tracerProvider != nil {
		tctx, span := m.withTracedContext(ctx, deliveries)
		defer span.End()

		ctx = tctx
	}

	c := NewBatchContext(ctx, deliveries, m.batchHandlers)
	c.Next()

	if !m.config.Consumer.AutoAck && c.GetError() == nil {
		for _, d := range deliveries {
			_ = d.Ack(false)
		}
	}
}

func (m *BatchMessageQueue) withTracedContext(ctx context.Context, ds []*amqp091.Delivery) (context.Context, trace.Span) {
	tctx, span := m.tracerProvider.Tracer(TRACER_NAME).Start(ctx, fmt.Sprintf("rabbit_mq.batch.%s", m.config.Consumer.Name))

	deliveries := otelDeliveries{
		deliveries: ds,
	}

	span.SetAttributes(deliveries.GetBatchConsumeAttributes(m.config.Queue.Name)...)

	return tctx, span
}
