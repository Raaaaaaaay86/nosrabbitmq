package nosrabbitmq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
)

type Publisher interface {
	Publish(ctx context.Context, exchange string, routingKey string, msg amqp091.Publishing) error
	Start(ctx context.Context)
}

type publishRequest struct {
	ctx        context.Context
	exchange   string
	routingKey string
	msg        amqp091.Publishing
	errCh      chan error
	span       trace.Span
}

type Producer struct {
	connection *Connection
	options    ProducerOptions
	requests   chan publishRequest
	ctx        context.Context
}

func NewProducer(connection *Connection, opts ...ProducerOption) *Producer {
	var options ProducerOptions
	for _, opt := range opts {
		opt(&options)
	}

	return &Producer{
		connection: connection,
		options:    options,
		requests:   make(chan publishRequest, options.bufferSize),
	}
}

func (p *Producer) Start(ctx context.Context) {
	p.ctx = ctx
	go p.loop(ctx)
}

func (p *Producer) Publish(ctx context.Context, exchange string, routingKey string, msg amqp091.Publishing) error {
	tctx, span := p.withTracedContext(ctx, exchange, routingKey, msg)

	if p.ctx == nil {
		return xerrors.New("producer is not started")
	}

	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	req := publishRequest{
		ctx:        tctx,
		exchange:   exchange,
		routingKey: routingKey,
		msg:        msg,
		errCh:      make(chan error, 1),
		span:       span,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.requests <- req:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return p.ctx.Err()
	case err := <-req.errCh:
		return err
	}
}

func (m Producer) withTracedContext(ctx context.Context, exchange string, routingKey string, msg amqp091.Publishing) (context.Context, trace.Span) {
	tctx, span := m.options.tracerProvider.Tracer(TRACE_NAME).Start(ctx, m.getTraceName(exchange, routingKey))

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemRabbitMQ,
		semconv.MessagingRabbitMQDestinationRoutingKey(routingKey),
		attribute.String("messaging.operation.type", "send"),
		attribute.Int("messaging.message.body.size", len(msg.Body)),

		attribute.String("exchange.name", exchange),
	}

	span.SetAttributes(attrs...)

	return tctx, span
}

func (m Producer) getTraceName(exchange string, routingKey string) string {
	sb := strings.Builder{}
	sb.WriteString("rabbitmq.publish")

	if exchange != "" {
		sb.WriteString(fmt.Sprintf(".%s", exchange))
	}
	if routingKey != "" {
		sb.WriteString(fmt.Sprintf(".%s", routingKey))
	}

	return sb.String()
}

func (p *Producer) loop(ctx context.Context) {
	var ch *amqp091.Channel

	ensureChannel := func() error {
		if ch != nil && !ch.IsClosed() {
			return nil
		}
		newCh, err := p.connection.Channel()
		if err != nil {
			return xerrors.Errorf("failed to open channel: %w", err)
		}
		ch = newCh
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			if ch != nil && !ch.IsClosed() {
				_ = ch.Close()
			}
			return
		case req := <-p.requests:
			if req.ctx.Err() != nil {
				req.errCh <- nil
				req.span.End()
				continue
			}

			if err := ensureChannel(); err != nil {
				req.errCh <- err
				req.span.End()
				continue
			}
			err := ch.PublishWithContext(req.ctx, req.exchange, req.routingKey, false, false, req.msg)
			if err != nil {
				ch = nil
			}

			req.errCh <- err
			req.span.End()
		}
	}
}
