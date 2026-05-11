# 🌊 Event Streaming Lab
> "High-Availability Event-Driven Architecture with Apache Kafka."

## 📐 Message Flow
```mermaid
sequenceDiagram
    participant P as Producer
    participant K as Kafka
    participant C as Consumer
    P->>K: Publish Event
    K-->>C: Pull Message
    alt Success
        C->>K: Commit Offset
    else Failure
        C->>DLQ: Error Handling
    end
```

## 📂 Core Patterns
- [**kafka-standard-patterns/**](./kafka-standard-patterns): Idempotent Producer, DLQ Handling.
- [**distributed-transactions/**](./distributed-transactions): Saga Pattern 구현.