# 🛠️ Event Streaming Lab Troubleshooting Guide

본 문서는 `event-streaming-lab`에서 Transactional Outbox 패턴 및 Kafka 컨슈머 구현 중 발생할 수 있는 데이터 정합성 이슈와 해결 과정을 기록합니다.

---

## 1. Outbox 폴링 스케줄러 동시성 문제 (Duplicate Publishing)

### 🚨 Problem (증상)
스케일 아웃(Scale-out) 환경에서 애플리케이션 인스턴스가 2대 이상 실행 중일 때, 
`OutboxEventRelay` 스케줄러가 동일한 이벤트를 동시에 폴링하여 Kafka로 **동일한 메시지를 2번 이상 발행하는 중복 이슈**가 빈번하게 발생함.

### 🔍 Cause Analysis (원인 분석)
- `SELECT * FROM outbox WHERE status = 'INIT'` 쿼리는 여러 노드에서 동시에 실행될 수 있음.
- 분산 락(Distributed Lock)이나 비관적 락(Pessimistic Lock)이 적용되지 않아 발생한 레이스 컨디션(Race Condition).

### ✅ Solution (해결 방안)
Spring Data JPA의 비관적 락(`@Lock(LockModeType.PESSIMISTIC_WRITE)`)을 `OutboxRepository`에 적용하여, 
한 노드가 특정 이벤트를 폴링하여 처리 중일 때 다른 노드가 접근하지 못하도록(대기 상태로) 강제함. (또는 DB 엔진의 `SKIP LOCKED` 구문 활용)

```java
// OutboxRepository.java (해결 스니펫)
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT o FROM Outbox o WHERE o.status = 'INIT'")
List<Outbox> findPendingEventsForUpdate();
```

*(Advanced: 폴링 부하가 극한으로 치달을 경우, 폴링 방식 대신 Debezium을 활용한 CDC(Change Data Capture) 아키텍처로의 전환을 권장합니다.)*

---

## 2. Consumer 멱등성 검증 시 성능 병목 (DB I/O 오버헤드)

### 🚨 Problem (증상)
`OrderConsumer`가 메시지를 받을 때마다 중복 수신 방지를 위해 `processed_events` 테이블을 조회하는데, 
초당 수천 건의 이벤트가 인입될 경우 **Consumer의 처리량(Throughput)이 극심하게 저하되고 DB CPU 사용률이 치솟는 현상** 발생.

### 🔍 Cause Analysis (원인 분석)
- At-least-once 환경에서 중복 방어(Idempotent)를 위해 DB `SELECT -> 로직 -> INSERT`의 과정이 이벤트마다 수행됨.
- RDBMS 특성상 디스크 I/O가 동반되어 대용량 스트림 처리 시 병목이 발생함.

### ✅ Solution (해결 방안)
1. **1차 방어 (In-Memory Cache):** Redis와 같은 인메모리 스토어에 `eventId`를 TTL(Time-To-Live)과 함께 캐싱하여, 중복 검증의 99%를 Redis 단에서 O(1)의 속도로 차단.
2. **2차 방어 (DB Unique Index):** `processed_events` 테이블의 `event_id` 컬럼에 `UNIQUE INDEX`를 설정. 만약 Redis 캐시 유실 등으로 1차 방어가 뚫리더라도, 중복 INSERT 시도 시 `DataIntegrityViolationException`이 발생하게 하여 최종 데이터 정합성을 보호.

---

## 3. Kafka Producer 데이터 유실 (Ack 타임아웃)

### 🚨 Problem (증상)
Kafka 클러스터 내의 특정 브로커 인스턴스 1대가 다운(Kill)되었을 때,
Outbox 스케줄러가 보낸 메시지가 성공적으로 처리되었다고 간주되었으나, 실제로 다른 Consumer가 해당 메시지를 읽지 못하는 **Silent Data Loss** 현상 발생.

### 🔍 Cause Analysis (원인 분석)
- Producer의 `acks` 설정이 `1`(리더만 응답)로 설정되어 있었음.
- 브로커 리더가 메시지를 기록하고 Producer에게 Ack를 보낸 직후, 다른 팔로워 브로커로 데이터가 복제되기 전에 리더 브로커가 비정상 종료됨.

### ✅ Solution (해결 방안)
- `application.yml`의 Kafka Producer 설정을 엔터프라이즈 환경에 맞게 상향 조정.
- `acks=all` (모든 복제본이 데이터 기록을 확인해야 Ack 응답) 및 `enable.idempotence=true` (프로듀서 레벨의 중복 재전송 방지) 속성을 활성화하여 신뢰성을 극대화함.

```yaml
spring:
  kafka:
    producer:
      acks: all
      properties:
        enable.idempotence: true
```
