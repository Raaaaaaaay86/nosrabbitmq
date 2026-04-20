package nosrabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
)

type MessageQueue struct {
	identifier     Identifier
	signalChan     chan<- Signal
	connection     *Connection
	ch             *amqp091.Channel
	config         MessageQueueConfig
	handlers       []HandlerFunc
	tracerProvider trace.TracerProvider

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
	defer func() {
		if r := recover(); r != nil {
			slog.Error("message queue listening panic", "identifier", m.identifier, "recover", r)
		} else {
			slog.Info("message queue listening exited", "identifier", m.identifier)
		}
	}()
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
	ctx := context.Background()

	if m.tracerProvider != nil {
		tctx, span := m.withTracedContext(ctx, d)
		defer span.End()

		ctx = tctx
	}

	c := NewContext(ctx, d, m.handlers)
	c.Next()

	if !m.config.Consumer.AutoAck && c.GetError() == nil {
		_ = d.Ack(false)
	}
}

func (m *MessageQueue) withTracedContext(ctx context.Context, d *amqp091.Delivery) (context.Context, trace.Span) {
	tctx, span := m.tracerProvider.Tracer("").Start(ctx, fmt.Sprintf("rabbit_mq.%s", m.config.Consumer.Name))

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemRabbitMQ,
		semconv.MessagingRabbitMQDestinationRoutingKey(m.config.RoutingKey),
		semconv.MessagingRabbitMQMessageDeliveryTag(int(d.DeliveryTag)),
		attribute.Int("messaging.message.body.size", len(d.Body)),

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

		attribute.String("app.consume_mode", "single"),
	}

	span.SetAttributes(attrs...)

	return tctx, span
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

func (m *MessageQueue) SetTracerProvider(tracerProvider trace.TracerProvider) {
	m.tracerProvider = tracerProvider
}

func (m *MessageQueue) SetSignalChan(ch chan<- Signal) {
	m.signalChan = ch
}
