# 🌊 Event Streaming Lab
> "High-Availability Event-Driven Architecture with Apache Kafka."

이 리포지토리는 초당 수만 건의 이벤트를 무손실로 처리하고, 시스템 간 느슨한 결합(Loose Coupling)을 지향하는 **Event-Driven Architecture**의 중심지입니다.

## 📐 Message Flow
`mermaid
sequenceDiagram
    Producer->>Kafka: Publish Event (Acknowledge)
    Kafka-->>Consumer: Pull Message
    Consumer->>Business Logic: Process
    alt Success
        Consumer->>Kafka: Commit Offset
    else Failure
        Consumer->>DLQ: Send to Dead Letter Queue
    end
`

## 📂 Core Patterns
- [**kafka-standard-patterns/**](./kafka-standard-patterns): Idempotent Producer, Batch Size Tuning, DLQ Handling.
- [**distributed-transactions/**](./distributed-transactions): Saga Pattern (Orchestration/Choreography) 구현.

---
<div align="center">
  <img src="https://img.shields.io/badge/Kafka-Ecosystem-black?style=for-the-badge&logo=apache-kafka" />
  <img src="https://img.shields.io/badge/Architecture-Event_Driven-blue?style=for-the-badge" />
</div>