package nosrabbitmq

import "go.opentelemetry.io/otel/trace"

type ProducerOptions struct {
	bufferSize     int
	tracerProvider trace.TracerProvider
}

type ProducerOption func(opt *ProducerOptions) error

func WithBufferSize(size int) ProducerOption {
	return func(opt *ProducerOptions) error {
		opt.bufferSize = size

		return nil
	}
}

func WithProducerTracerProvider(tracerProvider trace.TracerProvider) ProducerOption {
	return func(opt *ProducerOptions) error {
		opt.tracerProvider = tracerProvider

		return nil
	}
}
