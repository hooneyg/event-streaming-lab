package com.hooney.lab.eventstream.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hooney.lab.eventstream.domain.order.Order;
import com.hooney.lab.eventstream.domain.outbox.Outbox;
import com.hooney.lab.eventstream.infrastructure.persistence.OrderRepository;
import com.hooney.lab.eventstream.infrastructure.persistence.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 🛠️ OrderService (주문 서비스)
 * 
 * [연구 대상: Transactional Outbox Pattern]
 * 
 * [왜 이 패턴을 사용하는가?]
 * 분산 시스템에서 'DB 업데이트'와 '메시지 발행'은 하나의 트랜잭션으로 묶기 어렵습니다.
 * (DB는 성공했는데 Kafka 전송이 실패하거나, 반대의 경우가 발생할 수 있음)
 * 
 * [해결책]
 * 1. 주문 정보를 저장할 때, 발행할 이벤트(JSON)를 'Outbox' 테이블에 같은 DB 트랜잭션으로 저장합니다.
 * 2. DB 트랜잭션이 커밋되면 주문과 이벤트는 반드시 함께 저장됨이 보장됩니다.
 * 3. 별도의 프로세스(Relay)가 Outbox 테이블을 읽어 Kafka로 안전하게 전달합니다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    /**
     * 주문 생성 및 이벤트 적재
     * @Transactional 어노테이션을 통해 Order와 Outbox 저장이 원자적(Atomic)으로 처리됨을 보장합니다.
     */
    @Transactional
    public Long createOrder(String product, Long amount, String customerEmail) {
        log.info(">>>> [OrderService] 주문 생성 요청 수신: {}", product);

        // 1. 비즈니스 로직 실행: 주문 엔티티 생성 및 DB 저장
        Order order = Order.create(product, amount, customerEmail);
        orderRepository.save(order);

        // 2. 이벤트 데이터 구성: 주문 엔티티를 JSON 문자열로 변환 (Payload)
        String payload = "";
        try {
            payload = objectMapper.writeValueAsString(order);
        } catch (JsonProcessingException e) {
            log.error(">>>> 이벤트 데이터 JSON 변환 중 오류 발생", e);
            throw new RuntimeException("이벤트 직렬화 실패", e);
        }

        // 3. Outbox 적재: 나중에 Kafka로 보낼 이벤트를 DB에 미리 기록 (동일 트랜잭션)
        Outbox outbox = Outbox.create(
                "ORDER",           // 도메인 종류
                order.getId(),     // 식별값
                "ORDER_CREATED",   // 이벤트 타입
                payload            // 데이터 내용
        );
        outboxRepository.save(outbox);

        log.info(">>>> [OrderService] 트랜잭션 완료 준비 (OrderId: {}, OutboxId: {})", 
                order.getId(), outbox.getId());
        
        return order.getId();
    }
}
