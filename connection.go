package nosrabbitmq

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/xerrors"
)

// Connection wraps amqp091.Connection with automatic reconnection.
// It is intended to be used as a singleton shared across multiple queues.
type Connection struct {
	url  string
	conn *amqp091.Connection
	mu   sync.RWMutex
}

func NewConnection(url string) *Connection {
	return &Connection{url: url}
}

// Connect dials RabbitMQ and starts the background reconnect loop.
// The loop runs until ctx is cancelled.
func (c *Connection) Connect(ctx context.Context) error {
	conn, err := amqp091.Dial(c.url)
	if err != nil {
		return xerrors.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	go c.reconnectLoop(ctx)
	return nil
}

// Channel returns a new AMQP channel from the current connection.
// Returns an error if the connection is not yet ready or still reconnecting.
func (c *Connection) Channel() (*amqp091.Channel, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil || conn.IsClosed() {
		return nil, xerrors.New("RabbitMQ connection is not ready")
	}

	return conn.Channel()
}

// Close closes the underlying AMQP connection.
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn.Close()
	}
	return nil
}

func (c *Connection) reconnectLoop(ctx context.Context) {
	for {
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		closeCh := conn.NotifyClose(make(chan *amqp091.Error, 1))

		select {
		case <-ctx.Done():
			return
		case amqpErr, ok := <-closeCh:
			if !ok {
				// Connection was closed gracefully (e.g. via Close()).
				return
			}
			slog.Warn("RabbitMQ connection lost, reconnecting", "error", amqpErr)
		}

		backoff := time.Second
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			newConn, err := amqp091.Dial(c.url)
			if err != nil {
				slog.Warn("RabbitMQ reconnect failed, retrying", "error", err, "backoff", backoff)
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}

			c.mu.Lock()
			c.conn = newConn
			c.mu.Unlock()

			slog.Info("RabbitMQ reconnected successfully")
			break
		}
	}
}
