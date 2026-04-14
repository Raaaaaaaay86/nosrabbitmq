package nosrabbitmq

import (
	"fmt"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func TestManager(t *testing.T) {
	conn := NewConnection("amqp://root:root@localhost:5672/")

	if err := conn.Connect(t.Context()); err != nil {
		t.Fatal(err)
		return
	}

	factory := func() (Queue, error) {
		return NewMessageQueue(conn, MessageQueueConfig{
				Exchange: ExchangeConfig{
					Name: "test.exchange", Type: FANOUT,
					Durable:    true,
					AutoDelete: false,
				},
				Queue: QueueConfig{
					Name:       "test.queue",
					Durable:    true,
					AutoDelete: false,
					Exclusive:  true,
				},
				Consumer: ConsumerConfig{
					Name:      "test.consumer",
					AutoAck:   false,
					Exclusive: true,
				},
			}, handle),
			nil
	}

	manager := NewManager()

	if _, err := manager.Add(factory); err != nil {
		t.Fatal(err)
		return
	}

	if err := manager.Start(t.Context()); err != nil {
		t.Fatal(err)
		return
	}

	producer := NewProducer(conn, 2048)
	producer.Start(t.Context())

	for {
		time.Sleep(time.Second)

		if err := producer.Publish(t.Context(), "test.exchange", "", amqp091.Publishing{
			Body: []byte("77777"),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func handle(ctx *Context) {
	fmt.Println("#", string(ctx.Delivery.Body))
}
