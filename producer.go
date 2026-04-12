package nosrabbitmq

import (
	"context"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/xerrors"
)

type Publisher interface {
	Publish(ctx context.Context, exchange string, routingKey string, msg amqp091.Publishing) error
}

type publishRequest struct {
	ctx        context.Context
	exchange   string
	routingKey string
	msg        amqp091.Publishing
	errCh      chan error
}

type Producer struct {
	connection *Connection
	requests   chan publishRequest
	ctx        context.Context
}

func NewProducer(connection *Connection, bufferSize int) *Producer {
	return &Producer{
		connection: connection,
		requests:   make(chan publishRequest, bufferSize),
	}
}

func (p *Producer) Start(ctx context.Context) {
	p.ctx = ctx
	go p.loop(ctx)
}

func (p *Producer) Publish(ctx context.Context, exchange string, routingKey string, msg amqp091.Publishing) error {
	if p.ctx == nil {
		return xerrors.New("producer is not started")
	}

	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	req := publishRequest{
		ctx:        ctx,
		exchange:   exchange,
		routingKey: routingKey,
		msg:        msg,
		errCh:      make(chan error, 1),
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
				continue
			}

			if err := ensureChannel(); err != nil {
				req.errCh <- err
				continue
			}
			err := ch.PublishWithContext(req.ctx, req.exchange, req.routingKey, false, false, req.msg)
			if err != nil {
				ch = nil
			}

			req.errCh <- err
		}
	}
}
