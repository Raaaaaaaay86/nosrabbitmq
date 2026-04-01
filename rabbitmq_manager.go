package nosrabbitmq

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/xerrors"
)

var _ IRabbitMQManager = (*RabbitMQManager)(nil)

type IRabbitMQManager interface {
	RegisterConsumer(ctx context.Context, factory ...ConsumerFactory) error
	Start() error
	Stop(ctx context.Context) <-chan struct{}
	Close() error
}

type RabbitMQManager struct {
	factories  []ConsumerFactory
	consumers  map[Identifier]*consumerState
	signalChan chan Signal

	nextID    atomic.Uint64
	isStarted atomic.Bool
	mu        sync.RWMutex
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

type RabbitMQConfig struct {
	Username string
	Password string
	Protocol Protocol
	Host     string
	Port     string
}

func NewRabbitMQManager() *RabbitMQManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &RabbitMQManager{
		ctx:        ctx,
		cancel:     cancel,
		signalChan: make(chan Signal, 100),
		consumers:  make(map[Identifier]*consumerState),
	}
}

func (c *RabbitMQManager) RegisterConsumer(ctx context.Context, factories ...ConsumerFactory) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, factory := range factories {
		c.factories = append(c.factories, factory)
		if c.isStarted.Load() {
			if err := c.startConsumer(ctx, factory); err != nil {
				return xerrors.Errorf("failed to start registered consumer: %w", err)
			}
		}
	}

	return nil
}

func (r *RabbitMQManager) Start() error {
	r.mu.Lock()
	if r.isStarted.Swap(true) {
		r.mu.Unlock()
		return nil
	}

	for i, factory := range r.factories {
		if err := r.startConsumer(r.ctx, factory); err != nil {
			return xerrors.Errorf("failed to start consumer at index %d", i)
		}
	}
	r.mu.Unlock()

	r.wg.Add(1)

	go r.runManagementLoop()

	return nil
}

func (r *RabbitMQManager) startConsumer(ctx context.Context, factory ConsumerFactory) error {
	consumer, err := factory()
	if err != nil {
		return xerrors.Errorf("failed to build consumer from factory: %w", err)
	}

	identifier := consumer.GetIdentifier()
	if identifier == 0 {
		identifier = Identifier(r.nextID.Add(1))
		consumer.SetIdentifier(identifier)
	}

	consumer.SetSignalChan(r.signalChan)
	consumer.Start(ctx)

	state := &consumerState{
		factory:  factory,
		consumer: consumer,
	}

	r.consumers[identifier] = state

	return nil
}

func (r *RabbitMQManager) Stop(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})

	if !r.isStarted.Swap(false) {
		close(done)
		return done
	}

	// Stop management loop
	r.cancel()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Debug("Stop goroutine recovered", "value", r)
			}
			close(done)
		}()

		r.mu.RLock()
		var stopChans []<-chan struct{}
		for _, state := range r.consumers {
			stopChans = append(stopChans, state.consumer.Stop(ctx))
		}
		r.mu.RUnlock()

		// Wait for all consumers to stop
		for _, ch := range stopChans {
			if ch != nil {
				<-ch
			}
		}

		// Wait for management loop to exit
		r.wg.Wait()
	}()

	return done
}

func (r *RabbitMQManager) runManagementLoop() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("runManagementLoop recovered", "value", r)
		}
	}()
	defer r.wg.Done()

	for {
		select {
		case <-r.ctx.Done():
			return
		case sig, ok := <-r.signalChan:
			if !ok {
				return
			}
			if sig.Level == FailureSignalLevel {
				r.handleFailure(sig)
			}
		}
	}
}

func (r *RabbitMQManager) handleFailure(sig Signal) {
	r.mu.Lock()
	state, ok := r.consumers[sig.Identifier]
	if !ok {
		r.mu.Unlock()
		return
	}
	oldConsumer := state.consumer
	factory := state.factory
	r.mu.Unlock()

	// Stop and Close the old consumer without holding the lock
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	<-oldConsumer.Stop(stopCtx)
	_ = oldConsumer.Close()

	// Restart using the same factory
	newConsumer, err := factory()
	if err != nil {
		go func(err error) {
			r.signalChan <- Signal{
				Level:      FailureSignalLevel,
				Identifier: sig.Identifier,
				Error:      xerrors.Errorf("failed to re-build consumer: %w", err),
			}
		}(err)
		return
	}

	newConsumer.SetIdentifier(sig.Identifier) // Reuse the same identifier
	newConsumer.SetSignalChan(r.signalChan)
	newConsumer.Start(r.ctx)

	r.mu.Lock()
	defer r.mu.Unlock()

	// In case the factory internally set a different identifier, respect it
	newIdentifier := newConsumer.GetIdentifier()
	if newIdentifier != sig.Identifier {
		delete(r.consumers, sig.Identifier)
	}

	state.consumer = newConsumer
	r.consumers[newIdentifier] = state
}

func (r *RabbitMQManager) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cancel()

	var errs []error
	for _, state := range r.consumers {
		if err := state.consumer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return xerrors.Errorf("errors closing consumers: %w", errors.Join(errs...))
	}

	return nil
}
