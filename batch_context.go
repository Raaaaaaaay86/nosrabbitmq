package nosrabbitmq

import (
	"context"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

var _ context.Context = (*BatchContext)(nil)

type BatchHandlerFunc func(*BatchContext)

type BatchContext struct {
	ctx        context.Context
	index      int8
	aborted    bool
	err        error
	handlers   []BatchHandlerFunc
	Deliveries []*amqp091.Delivery
}

func NewBatchContext(ctx context.Context, deliveries []*amqp091.Delivery, handlers []BatchHandlerFunc) *BatchContext {
	return &BatchContext{
		ctx:        ctx,
		index:      -1,
		Deliveries: deliveries,
		handlers:   handlers,
	}
}

func (c *BatchContext) Next() {
	c.index++
	for c.index < int8(len(c.handlers)) {
		if c.aborted {
			return
		}
		c.handlers[c.index](c)
		c.index++
	}
}

func (c *BatchContext) Abort(err error) {
	c.aborted = true
	c.err = err
}

func (c *BatchContext) IsAborted() bool {
	return c.aborted
}

func (c *BatchContext) GetError() error {
	return c.err
}

func (c *BatchContext) Deadline() (time.Time, bool) {
	return c.ctx.Deadline()
}

func (c *BatchContext) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *BatchContext) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.ctx.Err()
}

func (c *BatchContext) Value(key any) any {
	return c.ctx.Value(key)
}
