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
 * 주문 저장(비즈니스)과 아웃박스 저장(이벤트)을 하나의 트랜잭션으로 묶어
 * DB 업데이트는 성공했는데 메시지 발행은 실패하는 불일치 상황을 원천 차단합니다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public Long createOrder(String product, Long amount, String customerEmail) {
        log.info(">>>> [OrderService] 주문 생성 시작: {}", product);

        // 1. 비즈니스 로직 실행 (Order 저장)
        Order order = Order.create(product, amount, customerEmail);
        orderRepository.save(order);

        // 2. 이벤트 페이로드 생성 (JSON)
        String payload = "";
        try {
            payload = objectMapper.writeValueAsString(order);
        } catch (JsonProcessingException e) {
            log.error("이벤트 페이로드 변환 실패", e);
            throw new RuntimeException("Serialization Error", e);
        }

        // 3. Outbox 테이블에 이벤트 적재 (동일한 트랜잭션!)
        Outbox outbox = Outbox.create(
                "ORDER",
                order.getId(),
                "ORDER_CREATED",
                payload
        );
        outboxRepository.save(outbox);

        log.info(">>>> [OrderService] 주문 및 아웃박스 저장 완료 (OrderId: {}, OutboxId: {})", 
                order.getId(), outbox.getId());
        
        return order.getId();
    }
}
