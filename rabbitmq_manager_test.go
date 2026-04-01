package nosrabbitmq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ManagerTestSuite struct {
	suite.Suite
}

func TestManagerTestSuite(t *testing.T) {
	suite.Run(t, new(ManagerTestSuite))
}

func (s *ManagerTestSuite) TestIdentifierAssignment() {
	mgr := NewRabbitMQManager()
	ctx := context.Background()

	m1 := NewMockConsumer(s.T())
	m1.EXPECT().GetIdentifier().Return(Identifier(0)).Once()
	m1.EXPECT().SetIdentifier(Identifier(1)).Return().Once()
	m1.EXPECT().SetSignalChan(mock.Anything).Return().Once()
	m1.EXPECT().Start(mock.Anything).Return().Once()

	m2 := NewMockConsumer(s.T())
	m2.EXPECT().GetIdentifier().Return(Identifier(0)).Once()
	m2.EXPECT().SetIdentifier(Identifier(2)).Return().Once()
	m2.EXPECT().SetSignalChan(mock.Anything).Return().Once()
	m2.EXPECT().Start(mock.Anything).Return().Once()

	f1 := func() (Consumer, error) { return m1, nil }
	f2 := func() (Consumer, error) { return m2, nil }

	_ = mgr.RegisterConsumer(ctx, f1, f2)
	_ = mgr.Start()

	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	s.Len(mgr.consumers, 2)
	s.Contains(mgr.consumers, Identifier(1))
	s.Contains(mgr.consumers, Identifier(2))
}

func (s *ManagerTestSuite) TestRegisterAfterStart() {
	mgr := NewRabbitMQManager()
	ctx := context.Background()
	_ = mgr.Start()

	m1 := NewMockConsumer(s.T())
	m1.EXPECT().GetIdentifier().Return(Identifier(0)).Once()
	m1.EXPECT().SetIdentifier(Identifier(1)).Return().Once()
	m1.EXPECT().SetSignalChan(mock.Anything).Return().Once()
	m1.EXPECT().Start(mock.Anything).Return().Once()

	f1 := func() (Consumer, error) { return m1, nil }

	err := mgr.RegisterConsumer(ctx, f1)
	s.NoError(err)

	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	s.Contains(mgr.consumers, Identifier(1))
}

func (s *ManagerTestSuite) TestHandleFailure() {
	mgr := NewRabbitMQManager()
	ctx := context.Background()
	_ = mgr.Start()

	m1 := NewMockConsumer(s.T())
	stopChan := make(chan struct{})
	close(stopChan)

	m1.EXPECT().GetIdentifier().Return(Identifier(1)).Maybe()
	m1.EXPECT().SetIdentifier(Identifier(1)).Return().Maybe()
	m1.EXPECT().SetSignalChan(mock.Anything).Return().Maybe()
	m1.EXPECT().Start(mock.Anything).Return().Maybe()
	m1.EXPECT().Stop(mock.Anything).Return((<-chan struct{})(stopChan)).Once()
	m1.EXPECT().Close().Return(nil).Once()

	m2 := NewMockConsumer(s.T())
	m2.EXPECT().GetIdentifier().Return(Identifier(1)).Maybe()
	m2.EXPECT().SetIdentifier(Identifier(1)).Return().Once()
	m2.EXPECT().SetSignalChan(mock.Anything).Return().Once()
	m2.EXPECT().Start(mock.Anything).Return().Once()

	factoryCount := 0
	f := func() (Consumer, error) {
		factoryCount++
		if factoryCount == 1 {
			return m1, nil
		}
		return m2, nil
	}

	_ = mgr.RegisterConsumer(ctx, f)

	mgr.signalChan <- Signal{
		Level:      FailureSignalLevel,
		Identifier: 1,
	}

	time.Sleep(1200 * time.Millisecond)

	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	s.Equal(m2, mgr.consumers[1].consumer)
}

func (s *ManagerTestSuite) TestStop() {
	mgr := NewRabbitMQManager()
	ctx := context.Background()
	_ = mgr.Start()

	m1 := NewMockConsumer(s.T())
	stopChan := make(chan struct{})
	close(stopChan)

	m1.EXPECT().GetIdentifier().Return(Identifier(1)).Maybe()
	m1.EXPECT().SetIdentifier(Identifier(1)).Return().Maybe()
	m1.EXPECT().SetSignalChan(mock.Anything).Return().Maybe()
	m1.EXPECT().Start(mock.Anything).Return().Maybe()
	m1.EXPECT().Stop(mock.Anything).Return((<-chan struct{})(stopChan)).Once()

	_ = mgr.RegisterConsumer(ctx, func() (Consumer, error) { return m1, nil })

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := mgr.Stop(stopCtx)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		s.T().Fatal("Manager Stop timed out")
	}

	s.False(mgr.isStarted.Load())
}

func (s *ManagerTestSuite) TestClose() {
	mgr := NewRabbitMQManager()
	ctx := context.Background()

	m1 := NewMockConsumer(s.T())
	m1.EXPECT().GetIdentifier().Return(Identifier(1)).Maybe()
	m1.EXPECT().SetIdentifier(Identifier(1)).Return().Maybe()
	m1.EXPECT().SetSignalChan(mock.Anything).Return().Maybe()
	m1.EXPECT().Start(mock.Anything).Return().Maybe()
	m1.EXPECT().Close().Return(nil).Once()

	_ = mgr.RegisterConsumer(ctx, func() (Consumer, error) { return m1, nil })
	_ = mgr.Start()

	err := mgr.Close()
	s.NoError(err)
	s.ErrorIs(mgr.ctx.Err(), context.Canceled)
}
