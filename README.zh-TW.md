# nosrabbitmq

`nosrabbitmq` 是 nos 生態系的 RabbitMQ 客戶端套件。提供 `Manager` 統一管理多個訊息佇列的生命週期、連線斷線時自動自癒、用於發布訊息的 `Producer`，以及類似 gin 的 middleware chain（`Context` / `BatchContext`）供消費者 handler 使用。

## 安裝

```bash
go get github.com/raaaaaaaay86/nosrabbitmq
```

## 架構

```
Connection（singleton，自動重連）
    │
    ├── Producer          → 發布訊息
    │
    └── Manager
            ├── MessageQueue      （單筆訊息消費者）
            └── BatchMessageQueue （批次訊息消費者）
```

## 快速開始

### 1. 建立連線

`Connection` 是一個 singleton，建立一次後共用於 Producer 與 Queue。

```go
conn := nosrabbitmq.NewConnection("amqp://guest:guest@localhost:5672/")
if err := conn.Connect(context.Background()); err != nil {
    panic(err)
}
defer conn.Close()
```

### 2. 發布訊息

```go
producer := nosrabbitmq.NewProducer(conn, 1024 /* buffer size */)
producer.Start(ctx)

err := producer.Publish(ctx, "my.exchange", "routing.key", amqp091.Publishing{
    ContentType: "application/json",
    Body:        []byte(`{"event":"user.created","id":42}`),
})
```

### 3. 消費單筆訊息

定義 handler function，並用 `QueueFactory` 包裝。

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

### 4. 批次消費訊息

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
        BatchSize:    50,              // 累積 50 筆後 flush
        BatchTimeout: 3 * time.Second, // 即使未滿也每 3s flush 一次
    }, handleBatch), nil
}
```

### 5. 註冊佇列並啟動 Manager

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

Handler 的執行順序與註冊順序相同，與 gin middleware chain 行為一致。呼叫 `ctx.Abort(err)` 可中止 chain，阻止後續 handler（包含 auto-ack）執行。

```go
func loggingMiddleware(ctx *nosrabbitmq.Context) {
    slog.Info("received message", "body", string(ctx.Delivery.Body))
    ctx.Next()
}

func businessLogicHandler(ctx *nosrabbitmq.Context) {
    // 處理訊息
    ctx.Next()
}

nosrabbitmq.NewMessageQueue(conn, config, loggingMiddleware, businessLogicHandler)
```

## 自動自癒

`Manager` 監控每個佇列發出的 `FailureSignalLevel` 信號。當佇列的 delivery channel 意外關閉時，Manager 會自動將其關閉、等待一秒、透過 factory 重新建立並重啟，無需任何人工介入。

## 實際範例（noschat）

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

## API 說明

### `Connection`

| 方法                 | 說明                              |
|----------------------|-----------------------------------|
| `NewConnection(url)` | 建立連線物件（尚未連線）          |
| `Connect(ctx)`       | 連線至 RabbitMQ 並啟動自動重連   |
| `Channel()`          | 開啟新的 AMQP channel             |
| `Close()`            | 關閉連線                          |

### `Manager`

| 方法             | 說明                                          |
|------------------|-----------------------------------------------|
| `NewManager()`   | 建立新的 Manager                              |
| `Add(factory)`   | 註冊 `QueueFactory`，回傳 `Identifier`        |
| `Start(ctx)`     | 並行啟動所有已註冊的佇列                      |
| `Stop()`         | 優雅停止所有佇列                              |
| `GetQueue(id)`   | 透過 `Identifier` 取得正在運行的佇列          |

### `MessageQueueConfig`

| 欄位           | 型別              | 說明                                                     |
|----------------|-------------------|----------------------------------------------------------|
| `Exchange`     | `ExchangeConfig`  | Exchange 名稱、類型、持久性                              |
| `Queue`        | `QueueConfig`     | Queue 名稱、持久性、獨占性                               |
| `Consumer`     | `ConsumerConfig`  | Consumer 名稱、auto-ack                                  |
| `RoutingKey`   | `string`          | 綁定用的 routing key                                     |
| `BatchSize`    | `int`             | 批次 flush 大小（僅 `BatchMessageQueue`，預設：1）        |
| `BatchTimeout` | `time.Duration`   | 批次 flush 間隔（預設：5s）                              |

### Exchange 類型

| 常數      | 值         |
|-----------|------------|
| `DIRECT`  | `"direct"` |
| `FANOUT`  | `"fanout"` |
| `TOPIC`   | `"topic"`  |
| `HEADERS` | `"headers"`|
