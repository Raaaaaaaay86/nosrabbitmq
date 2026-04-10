package nosrabbitmq

type Protocol string

const (
	AMQP  Protocol = "amqp"
	AMQPS Protocol = "amqps"
)

type ExchangeType string

const (
	DIRECT  ExchangeType = "direct"
	FANOUT  ExchangeType = "fanout"
	TOPIC   ExchangeType = "topic"
	HEADERS ExchangeType = "headers"
)

func (e ExchangeType) IsValid() bool {
	switch e {
	case DIRECT, FANOUT, TOPIC, HEADERS:
		return true
	default:
		return false
	}
}
