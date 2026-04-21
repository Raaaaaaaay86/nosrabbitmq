package nosrabbitmq

import (
	"strings"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

func getSingleConsumeAttributes(queueName string, delivery *amqp091.Delivery) []attribute.KeyValue {
	attrs := getBasicAttributes(queueName)

	attrs = append(attrs,
		semconv.MessagingRabbitMQDestinationRoutingKey(delivery.RoutingKey),
		semconv.MessagingRabbitMQMessageDeliveryTag(int(delivery.DeliveryTag)),
		attribute.String("messaging.destination.name", getDetinationName(queueName, delivery)),
		attribute.String("app.consume_mode", "single"),
		attribute.Int("messaging.message.body.size", len(delivery.Body)),
	)

	return attrs
}

func getBasicAttributes(queueName string) []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.MessagingSystemRabbitMQ,
		attribute.String("messaging.operation.type", "receive"),
	}
}

func getDetinationName(queueName string, delivery *amqp091.Delivery) string {
	parts := []string{}
	if delivery.Exchange != "" {
		parts = append(parts, delivery.Exchange)
	}

	if delivery.RoutingKey != "" {
		parts = append(parts, delivery.RoutingKey)
	}

	if queueName != "" && queueName != delivery.RoutingKey {
		parts = append(parts, queueName)
	}

	return strings.Join(parts, ":")
}

func getBatchConsumeAttributes(queueName string, deliveries []*amqp091.Delivery) []attribute.KeyValue {
	attrs := getBasicAttributes(queueName)

	attrs = append(attrs,
		attribute.String("app.consume_mode", "batch"),
		attribute.Int("messaging.batch.message_count", len(deliveries)),
		attribute.Int("messaging.batch.body.size", getBatchSize(deliveries)),
	)

	return attrs
}

func getBatchSize(deliveries []*amqp091.Delivery) int {
	total := 0

	for _, delivery := range deliveries {
		total += len(delivery.Body)
	}

	return total
}
