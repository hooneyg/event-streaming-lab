# ADR 001: 분산 트랜잭션 처리를 위한 Transactional Outbox Pattern 채택

## 1. Status
**Accepted**

## 2. Context (배경)
MSA(Microservices Architecture) 환경에서 비즈니스 로직 처리(DB 데이터 변경)와 다른 마이크로서비스로의 상태 변경 전파(메시지 큐 발행)는 분산 시스템 트랜잭션의 핵심 난제입니다.
- 단순하게 `Order` 엔티티를 DB에 저장한 뒤(commit 완료), Kafka로 발행하는 방식은 "DB에는 저장되었으나 Kafka 브로커 장애로 전파 실패"라는 데이터 불일치(Inconsistency)를 야기합니다.
- 반대로 Kafka로 먼저 발행한 뒤 DB 트랜잭션을 commit하는 방식은 "Kafka에는 전송되었으나 DB 제약 조건 위배로 롤백"되는 경우 허위 이벤트를 타 시스템에 전파하게 됩니다.
- 2PC(Two-Phase Commit)는 성능 저하와 타 시스템과의 강력한 결합을 초래하여 현대 아키텍처에 부적합합니다.

## 3. Decision (결정)
메시지 발행의 신뢰성을 보장하기 위해 **Transactional Outbox Pattern**을 채택하고, At-least-once 전달을 보장합니다.

- **구현 방식:**
  1. `OrderService`에서 주문(`Order`)을 생성할 때, 동일한 로컬 트랜잭션 내에서 `Outbox` 테이블에 발행해야 할 이벤트를 `INIT` 상태로 저장합니다. (Atomicity 보장)
  2. 별도의 데몬 스레드(Spring `@Scheduled`가 적용된 `OutboxEventRelay`)가 주기적으로 `Outbox` 테이블을 폴링하여 `INIT` 상태의 이벤트를 읽어옵니다.
  3. `OutboxEventRelay`가 Kafka 브로커로 메시지를 발행(Publish)합니다.
  4. Kafka로부터 Ack를 수신하면 `Outbox` 엔티티의 상태를 `PUBLISHED`로 업데이트합니다.

## 4. Rationale (결정 이유)
- **강력한 일관성(Strong Consistency) 확보:** 로컬 RDBMS 트랜잭션을 활용하므로 비즈니스 로직 성공과 이벤트 발행 대기열 적재의 원자성이 100% 보장됩니다.
- **격리 및 내결함성:** Kafka 클러스터에 장애가 발생하더라도 주문 도메인은 정상적으로 동작합니다(가용성 확보). Kafka 장애가 복구되면 스케줄러가 미발행 이벤트들을 순차적으로 재전송합니다(At-least-once 보장).
- **Debezium/CDC 대비 유연성:** 별도의 CDC(Change Data Capture) 인프라 구축 없이 애플리케이션 레벨의 구현만으로 이벤트 구조화 및 전송을 컨트롤할 수 있어 초기 랩 구성에 적합합니다.

## 5. Consequences (결과 및 고려사항)
- **폴링 오버헤드:** 스케줄러가 DB를 주기적으로 폴링(Polling)하므로 트래픽이 방대해질 경우 DB에 부하를 줄 수 있습니다.
- **At-least-once 한계:** 스케줄러가 발행 성공 후 DB의 상태를 `PUBLISHED`로 변경하기 직전에 서버가 다운되면, 재기동 시 동일한 이벤트를 다시 발행하게 됩니다 (중복 발행 발생).
- **Mitigation (해결책):** 이 패턴은 필연적으로 "중복 발행" 가능성을 내포하므로, Consumer 측에서 반드시 식별자를 기반으로 한 **멱등성(Idempotent) 처리 로직**(`ProcessedEvent` 테이블 활용 등)을 함께 구현해야만 완전한 분산 시스템 안정성이 달성됩니다.
