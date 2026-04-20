package nosrabbitmq

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type queueState struct {
	factory QueueFactory
	queue   Queue
}

type Manager struct {
	queues     map[Identifier]*queueState
	signalChan chan Signal // Internal signal channel

	// ExternalSignalChan is used to forward non-failure signals (logs) to the caller.
	ExternalSignalChan chan Signal
	options            ManagerOptions

	nextID    atomic.Int64
	isStarted atomic.Bool
	mu        sync.RWMutex
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewManager(opts ...ManagerOption) (*Manager, error) {
	var options ManagerOptions
	for _, opt := range opts {
		opt(&options)
	}

	manager := &Manager{
		options:            options,
		queues:             make(map[Identifier]*queueState),
		signalChan:         make(chan Signal, 100),
		ExternalSignalChan: make(chan Signal, 100),
	}

	return manager, nil
}

// Add adds a Queue factory to the manager. If the queue's Identifier is 0,
// it will be automatically assigned. Returns the Identifier.
func (m *Manager) Add(factory QueueFactory) (Identifier, error) {
	queue, err := factory()
	if err != nil {
		return 0, xerrors.Errorf("failed to create queue from factory: %w", err)
	}

	id := queue.GetIdentifier()
	if id == 0 {
		id = Identifier(m.nextID.Add(1))
		queue.SetIdentifier(id)
	}

	if m.options.tracerProvider != nil {
		queue.SetTracerProvider(m.options.tracerProvider)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.queues[id] = &queueState{
		factory: factory,
		queue:   queue,
	}

	return id, nil
}

// Start concurrently starts all registered Queues.
// It also starts the background management loop for self-healing and signal forwarding.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.isStarted.Swap(true) {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	m.ctx, m.cancel = context.WithCancel(ctx)

	g, _ := errgroup.WithContext(m.ctx)

	m.mu.RLock()
	for _, state := range m.queues {
		s := state // capture for closure
		g.Go(func() error {
			s.queue.SetSignalChan(m.signalChan)
			return s.queue.Start(m.ctx)
		})
	}
	m.mu.RUnlock()

	if err := g.Wait(); err != nil {
		m.cancel()
		m.isStarted.Store(false)
		return xerrors.Errorf("failed to start queues: %w", err)
	}

	m.wg.Add(1)
	go m.runManagementLoop()

	return nil
}

// Stop stops all queues and the management loop.
func (m *Manager) Stop() error {
	if !m.isStarted.Swap(false) {
		return nil
	}

	if m.cancel != nil {
		m.cancel()
	}

	m.mu.RLock()
	var errs []error
	for _, state := range m.queues {
		if err := state.queue.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	m.mu.RUnlock()

	m.wg.Wait()

	if len(errs) > 0 {
		return xerrors.Errorf("errors stopping queues: %w", errors.Join(errs...))
	}

	return nil
}

func (m *Manager) runManagementLoop() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case sig, ok := <-m.signalChan:
			if !ok {
				return
			}

			if sig.Level == FailureSignalLevel {
				slog.Warn("Queue failure detected, attempting self-healing", "identifier", sig.Identifier, "error", sig.Error)
				m.handleFailure(sig)
			} else {
				// Forward log signals to external channel
				select {
				case m.ExternalSignalChan <- sig:
				default:
					// Drop signal if external channel is full to prevent deadlocks
				}
			}
		}
	}
}

func (m *Manager) handleFailure(sig Signal) {
	m.mu.Lock()
	state, ok := m.queues[sig.Identifier]
	if !ok {
		m.mu.Unlock()
		return
	}
	oldQueue := state.queue
	factory := state.factory
	m.mu.Unlock()

	// Cleanup old queue
	_ = oldQueue.Close()

	// Small delay before restart
	time.Sleep(time.Second)

	// Re-create using factory
	newQueue, err := factory()
	if err != nil {
		slog.Error("failed to rebuild queue during self-healing", "identifier", sig.Identifier, "error", err)
		// Send failure signal back to retry later
		go func() {
			m.signalChan <- sig
		}()
		return
	}

	newQueue.SetIdentifier(sig.Identifier)
	newQueue.SetSignalChan(m.signalChan)

	if err := newQueue.Start(m.ctx); err != nil {
		slog.Error("failed to restart queue during self-healing", "identifier", sig.Identifier, "error", err)
		_ = newQueue.Close()
		go func() {
			m.signalChan <- sig
		}()
		return
	}

	m.mu.Lock()
	state.queue = newQueue
	m.mu.Unlock()

	slog.Info("Queue self-healing completed", "identifier", sig.Identifier)
}

func (m *Manager) GetQueue(id Identifier) (Queue, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.queues[id]
	if !ok {
		return nil, false
	}
	return state.queue, true
}
