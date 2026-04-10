package nosrabbitmq

import (
	"context"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

var _ context.Context = (*Context)(nil)

type HandlerFunc func(*Context)

type Context struct {
	ctx      context.Context
	index    int8
	aborted  bool
	err      error
	handlers []HandlerFunc
	Delivery *amqp091.Delivery
}

func NewContext(ctx context.Context, delivery *amqp091.Delivery, handlers []HandlerFunc) *Context {
	return &Context{
		ctx:      ctx,
		index:    -1,
		Delivery: delivery,
		handlers: handlers,
	}
}

func (c *Context) Next() {
	c.index++
	for c.index < int8(len(c.handlers)) {
		if c.aborted {
			return
		}
		c.handlers[c.index](c)
		c.index++
	}
}

func (c *Context) Abort(err error) {
	c.aborted = true
	c.err = err
}

func (c *Context) IsAborted() bool {
	return c.aborted
}

func (c *Context) GetError() error {
	return c.err
}

func (c *Context) Deadline() (time.Time, bool) {
	return c.ctx.Deadline()
}

func (c *Context) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *Context) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.ctx.Err()
}

func (c *Context) Value(key any) any {
	return c.ctx.Value(key)
}
