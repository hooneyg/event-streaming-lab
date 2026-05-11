# 🌊 Event Streaming Lab
> "Zero-Loss Data Pipeline & Resilient Distributed Systems."

이 리포지토리는 고가용성 메시징 시스템의 핵심인 **Apache Kafka**를 활용하여, 대규모 트래픽에서도 견고하게 동작하는 **Event-Driven Architecture**를 연구합니다.

## 🚀 Architectural Patterns
- **At-Least-Once & Exactly-Once**: 데이터 유실 방지 및 중복 처리 최적화.
- **Dead Letter Queue (DLQ)**: 실패한 이벤트의 격리 및 재처리 메커니즘.
- **Saga Pattern**: 분산 환경에서의 데이터 최종 정합성(Eventual Consistency) 보장.