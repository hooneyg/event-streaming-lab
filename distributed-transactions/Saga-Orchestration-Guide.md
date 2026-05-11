# Distributed Transactions: Saga Pattern (Orchestration)
분산 시스템에서 트랜잭션을 유지하기 위한 **Saga 패턴**의 흐름입니다.

1. **Order Service**: 주문 생성 및 `OrderCreated` 이벤트 발행.
2. **Kafka**: 이벤트를 `orders` 토픽으로 전송.
3. **Payment Service**: 결제 처리 후 성공 시 `PaymentSuccess` 이벤트 발행.
4. **Inventory Service**: 재고 차감.
* 만약 결제 실패 시, 보상 트랜잭션(Compensating Transaction)을 통해 주문 취소 상태로 변경.