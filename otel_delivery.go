package nosrabbitmq

import (
	"strings"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

type otelDelivery struct {
	delivery *amqp091.Delivery
}

func (o otelDelivery) GetSingleConsumeAttributes(queueName string) []attribute.KeyValue {
	attrs := o.GetBasicAttributes(queueName)

	attrs = append(attrs,
		attribute.String("app.consume_mode", "single"),
		attribute.Int("messaging.message.body.size", len(o.delivery.Body)),
	)

	return attrs
}

func (o otelDelivery) GetBasicAttributes(queueName string) []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.MessagingSystemRabbitMQ,
		semconv.MessagingRabbitMQDestinationRoutingKey(o.delivery.RoutingKey),
		semconv.MessagingRabbitMQMessageDeliveryTag(int(o.delivery.DeliveryTag)),
		attribute.String("messaging.destination.name", o.getDetinationName(queueName)),
		attribute.String("messaging.operation.type", "receive"),
	}
}

func (o otelDelivery) getDetinationName(queueName string) string {
	parts := []string{}
	if o.delivery.Exchange != "" {
		parts = append(parts, o.delivery.Exchange)
	}

	if o.delivery.RoutingKey != "" {
		parts = append(parts, o.delivery.RoutingKey)
	}

	if queueName != "" && queueName != o.delivery.RoutingKey {
		parts = append(parts, queueName)
	}

	return strings.Join(parts, ":") + " receive"
}

type otelDeliveries struct {
	deliveries []*amqp091.Delivery
}

func (o otelDeliveries) GetBatchConsumeAttributes(queueName string) []attribute.KeyValue {
	var delivery otelDelivery

	attrs := delivery.GetBasicAttributes(queueName)

	attrs = append(attrs,
		attribute.String("app.consume_mode", "batch"),
		attribute.Int("messaging.message.body.size", o.GetBatchSize()),
	)

	return attrs
}

func (o otelDeliveries) GetBatchSize() int {
	total := 0

	for _, delivery := range o.deliveries {
		total += len(delivery.Body)
	}

	return total
}
