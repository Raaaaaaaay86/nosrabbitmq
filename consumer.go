package nosrabbitmq

import "context"

type Consumer interface {
	Start(context.Context)
	Stop(context.Context) <-chan struct{}
	SetSignalChan(chan Signal)
	GetIdentifier() Identifier
	SetIdentifier(Identifier)
	GetConnectionInfo() ConnectionInfo
	Close() error
}

type ConsumerFactory func() (Consumer, error)

type Identifier int

type ConnectionInfo struct {
	Username string
	Password string
	Protocol Protocol
	Host     string
	Port     string
}

type consumerState struct {
	factory  ConsumerFactory
	consumer Consumer
}

type SignalLevel uint8

const (
	DebugSignalLevel   SignalLevel = 1
	InfoSignalLevel    SignalLevel = 2
	WarnSignalLevel    SignalLevel = 3
	ErrorSignalLevel   SignalLevel = 4
	FailureSignalLevel SignalLevel = 5
)

type Signal struct {
	Level      SignalLevel
	Identifier Identifier
	Message    string
	Error      error
}
