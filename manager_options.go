package nosrabbitmq

import "go.opentelemetry.io/otel/trace"

type ManagerOptions struct {
	tracerProvider trace.TracerProvider
}

type ManagerOption func(*ManagerOptions) error

func WithTracerProvider(tracerProvider trace.TracerProvider) ManagerOption {
	return func(mo *ManagerOptions) error {
		mo.tracerProvider = tracerProvider
		return nil
	}
}
