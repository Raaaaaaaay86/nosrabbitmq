# nosrabbitmq

`nosrabbitmq` is a RabbitMQ client library for the nos ecosystem. It provides a `Manager` that owns the lifecycle of multiple message queues, automatic self-healing on connection loss, a `Producer` for publishing messages, and a gin-style middleware chain (`Context` / `BatchContext`) for consumer handlers.

## Installation

```bash
go get github.com/raaaaaaaay86/nosrabbitmq
```

## Architecture

```
Connection (singleton, auto-reconnect)
    │
    ├── Producer          → publishes messages
    │
    └── Manager
            ├── MessageQueue      (single-message consumer)
            └── BatchMessageQueue (batch consumer)
```

## Quick Start

### 1. Create a Connection

`Connection` is a singleton. Create it once and share it across producers and queues.

```go
conn := nosrabbitmq.NewConnection("amqp://guest:guest@localhost:5672/")
if err := conn.Connect(context.Background()); err != nil {
    panic(err)
}
defer conn.Close()
```

### 2. Publish Messages

```go
producer := nosrabbitmq.NewProducer(conn, 1024 /* buffer size */)
producer.Start(ctx)

err := producer.Publish(ctx, "my.exchange", "routing.key", amqp091.Publishing{
    ContentType: "application/json",
    Body:        []byte(`{"event":"user.created","id":42}`),
})
```

### 3. Consume Single Messages

Define a handler function and wrap it in a `QueueFactory`.

```go
func handleMessage(ctx *nosrabbitmq.Context) {
    var payload MyEvent
    if err := json.Unmarshal(ctx.Delivery.Body, &payload); err != nil {
        ctx.Abort(err)
        return
    }
    if err := processEvent(payload); err != nil {
        ctx.Abort(err)
        return
    }
    ctx.Next()
}

factory := func() (nosrabbitmq.Queue, error) {
    return nosrabbitmq.NewMessageQueue(conn, nosrabbitmq.MessageQueueConfig{
        Exchange: nosrabbitmq.ExchangeConfig{
            Name:    "my.exchange",
            Type:    nosrabbitmq.FANOUT,
            Durable: true,
        },
        Queue: nosrabbitmq.QueueConfig{
            Name:    "my.service.queue",
            Durable: true,
        },
        Consumer: nosrabbitmq.ConsumerConfig{
            Name:    "my-consumer",
            AutoAck: false,
        },
    }, handleMessage), nil
}
```

### 4. Consume Batch Messages

```go
func handleBatch(ctx *nosrabbitmq.BatchContext) {
    for _, d := range ctx.Deliveries {
        fmt.Println("received:", string(d.Body))
    }
    ctx.Next()
}

factory := func() (nosrabbitmq.Queue, error) {
    return nosrabbitmq.NewBatchMessageQueue(conn, nosrabbitmq.MessageQueueConfig{
        Exchange: nosrabbitmq.ExchangeConfig{
            Name:    "my.exchange",
            Type:    nosrabbitmq.DIRECT,
            Durable: true,
        },
        Queue: nosrabbitmq.QueueConfig{
            Name:    "my.batch.queue",
            Durable: true,
        },
        Consumer: nosrabbitmq.ConsumerConfig{AutoAck: false},
        RoutingKey:   "batch.key",
        BatchSize:    50,
        BatchTimeout: 3 * time.Second,
    }, handleBatch), nil
}
```

### 5. Register Queues and Start the Manager

```go
manager := nosrabbitmq.NewManager()

if _, err := manager.Add(factory); err != nil {
    panic(err)
}
if err := manager.Start(ctx); err != nil {
    panic(err)
}
defer manager.Stop()
```

## Middleware Chain

Handlers are executed in registration order, exactly like a gin middleware chain. Call `ctx.Abort(err)` to stop the chain and prevent further handlers (including auto-ack) from running.

```go
func loggingMiddleware(ctx *nosrabbitmq.Context) {
    slog.Info("received message", "body", string(ctx.Delivery.Body))
    ctx.Next()
}

func businessLogicHandler(ctx *nosrabbitmq.Context) {
    // process the message
    ctx.Next()
}

nosrabbitmq.NewMessageQueue(conn, config, loggingMiddleware, businessLogicHandler)
```

## Self-Healing

The `Manager` watches for `FailureSignalLevel` signals from each queue. When a queue's delivery channel closes unexpectedly, the manager tears it down, waits one second, re-creates it via its factory, and restarts it automatically.

## Real-World Example (noschat)

```go
// component/rabbitmq_app.go

func NewChatroomMessageQueueFactory(
    cfg config.Kubernetes,
    conn AppRabbitMQConnection,
    handlers ...nosrabbitmq.HandlerFunc,
) nosrabbitmq.QueueFactory {
    return func() (nosrabbitmq.Queue, error) {
        return nosrabbitmq.NewMessageQueue(conn, nosrabbitmq.MessageQueueConfig{
            Exchange: nosrabbitmq.ExchangeConfig{
                Name:    "chatroom.exchange",
                Type:    nosrabbitmq.FANOUT,
                Durable: true,
            },
            Queue: nosrabbitmq.QueueConfig{
                Name:      "chatroom.broadcasting." + cfg.Metadata.Name,
                Durable:   true,
                Exclusive: true,
            },
            Consumer: nosrabbitmq.ConsumerConfig{
                Name:      "chatroom-consumer-" + cfg.Metadata.Name,
                AutoAck:   false,
                Exclusive: true,
            },
        }, handlers...), nil
    }
}

// domain/chatroom/infra/rabbitmq/chatroom_consumer.go

func (c *ChatroomConsumer) ConsumeMessage(ctx *nosrabbitmq.Context) {
    var message entity.Message
    if err := json.Unmarshal(ctx.Delivery.Body, &message); err != nil {
        ctx.Abort(err)
        return
    }
    if _, err := c.saveMessage.Execute(ctx, usecase.SaveMessageCommand{Message: message}); err != nil {
        ctx.Abort(err)
        return
    }
    c.hub.Broadcast(message.RoomId, payload)
    ctx.Next()
}
```

## API Reference

### `Connection`

| Method               | Description                                 |
|----------------------|---------------------------------------------|
| `NewConnection(url)` | Create a new connection (not yet connected) |
| `Connect(ctx)`       | Dial RabbitMQ and start auto-reconnect loop |
| `Channel()`          | Open a new AMQP channel                     |
| `Close()`            | Close the underlying connection             |

### `Manager`

| Method           | Description                                       |
|------------------|---------------------------------------------------|
| `NewManager()`   | Create a new manager                              |
| `Add(factory)`   | Register a `QueueFactory`, returns `Identifier`   |
| `Start(ctx)`     | Start all registered queues concurrently          |
| `Stop()`         | Stop all queues gracefully                        |
| `GetQueue(id)`   | Retrieve a running queue by its `Identifier`      |

### `MessageQueueConfig`

| Field          | Type              | Description                                                   |
|----------------|-------------------|---------------------------------------------------------------|
| `Exchange`     | `ExchangeConfig`  | Exchange name, type, durability                               |
| `Queue`        | `QueueConfig`     | Queue name, durability, exclusivity                           |
| `Consumer`     | `ConsumerConfig`  | Consumer name, auto-ack                                       |
| `RoutingKey`   | `string`          | Binding routing key                                           |
| `BatchSize`    | `int`             | Batch flush size (`BatchMessageQueue` only, default: 1)       |
| `BatchTimeout` | `time.Duration`   | Batch flush interval (default: 5s)                            |

### Exchange Types

| Constant  | Value      |
|-----------|------------|
| `DIRECT`  | `"direct"` |
| `FANOUT`  | `"fanout"` |
| `TOPIC`   | `"topic"`  |
| `HEADERS` | `"headers"`|
