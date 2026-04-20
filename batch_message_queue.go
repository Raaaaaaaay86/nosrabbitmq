package nosrabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
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
		tctx, span := m.withTracedContext(ctx)
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

func (m *BatchMessageQueue) withTracedContext(ctx context.Context) (context.Context, trace.Span) {
	tctx, span := m.tracerProvider.Tracer(TRACE_NAME).Start(ctx, fmt.Sprintf("rabbit_mq.batch.%s", m.config.Consumer.Name))

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemRabbitMQ,
		semconv.MessagingRabbitMQDestinationRoutingKey(m.config.RoutingKey),
		attribute.String("messaging.destination.name", m.getTraceDetinationName()),
		attribute.String("messaging.operation.type", "receive"),

		attribute.String("routing_key", m.config.RoutingKey),

		attribute.String("exchange.name", m.config.Exchange.Name),
		attribute.String("exchange.type", string(m.config.Exchange.Type)),
		attribute.Bool("exchange.durable", m.config.Exchange.Durable),
		attribute.Bool("exchange.auto_delete", m.config.Exchange.AutoDelete),
		attribute.Bool("exchange.internal", m.config.Exchange.Internal),
		attribute.Bool("exchange.no_wait", m.config.Exchange.NoWait),

		attribute.String("queue.name", m.config.Queue.Name),
		attribute.Bool("queue.durable", m.config.Queue.Durable),
		attribute.Bool("queue.auto_delete", m.config.Queue.AutoDelete),
		attribute.Bool("queue.exclusive", m.config.Queue.Exclusive),
		attribute.Bool("queue.no_wait", m.config.Queue.NoWait),

		attribute.String("consumer.name", m.config.Consumer.Name),
		attribute.Bool("consumer.auto_ack", m.config.Consumer.AutoAck),
		attribute.Bool("consumer.exclusive", m.config.Consumer.Exclusive),
		attribute.Bool("consumer.no_wait", m.config.Consumer.NoWait),

		attribute.String("app.consume_mode", "batch"),
	}

	span.SetAttributes(attrs...)

	return tctx, span
}
