package com.hooney.lab.eventstream.infrastructure.messaging;

import com.hooney.lab.eventstream.domain.outbox.ProcessedEvent;
import com.hooney.lab.eventstream.infrastructure.persistence.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 🎧 OrderConsumer (주문 이벤트 소비자)
 * 
 * [연구 대상: Idempotent Consumer]
 * 중복 수신된 메시지를 ProcessedEvent 테이블 조회를 통해 걸러내어
 * 비즈니스 로직이 중복 실행되는 것을 방지합니다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderConsumer {

    private final ProcessedEventRepository processedEventRepository;

    @Transactional
    @KafkaListener(topics = "lab.order", groupId = "lab-group")
    public void consume(ConsumerRecord<String, String> record) {
        String eventId = record.key();
        log.info(">>>> [OrderConsumer] 메시지 수신: (Key: {}, Payload: {})", eventId, record.value());

        // 1. 멱등성 체크: 이미 처리된 이벤트인지 확인
        if (processedEventRepository.existsById(eventId)) {
            log.warn(">>>> [OrderConsumer] 중복된 이벤트 수신 차단 (EventId: {})", eventId);
            return;
        }

        // 2. 비즈니스 로직 실행 (예: 알림 발송, 재고 차감 등)
        log.info(">>>> [OrderConsumer] 비즈니스 로직 실행 중... (주문 처리 완료)");

        // 3. 처리 이력 기록 (동일 트랜잭션)
        processedEventRepository.save(ProcessedEvent.of(eventId));
        
        log.info(">>>> [OrderConsumer] 이벤트 처리 완료 및 멱등성 이력 저장");
    }
}
