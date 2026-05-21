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
 * [연구 대상: Idempotent Consumer (멱등성 소비자)]
 * 
 * [왜 멱등성이 필요한가?]
 * 네트워크 장애 등으로 인해 프로듀서(Relay)가 동일한 메시지를 두 번 이상 보낼 수 있습니다.
 * 소비자 측에서 이를 그대로 처리하면 결제 중복, 알림 중복 등의 심각한 문제가 발생합니다.
 * 
 * [해결책]
 * 1. 메시지 고유의 Key(이벤트 ID)를 'ProcessedEvent' 이력 테이블에서 조회합니다.
 * 2. 이미 존재한다면 이미 처리된 것이므로 무시(Discard)합니다.
 * 3. 존재하지 않는다면 비즈니스 로직을 수행하고, 이력 테이블에 저장합니다.
 * 4. 이 모든 과정은 하나의 트랜잭션으로 처리되어야 원자성이 보장됩니다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderConsumer {

    private final ProcessedEventRepository processedEventRepository;

    /**
     * Kafka 메시지 리스너
     * lab.order 토픽의 메시지를 수신하여 멱등적으로 처리합니다.
     */
    @Transactional
    @KafkaListener(topics = "lab.order", groupId = "lab-group")
    public void consume(ConsumerRecord<String, String> record) {
        String eventId = record.key(); // 이벤트 고유 식별자 (Outbox ID 또는 UUID)
        log.info(">>>> [OrderConsumer] 메시지 수신 성공: (Key: {}, Topic: {})", eventId, record.topic());
        System.out.println("🚨🚨🚨 [OrderConsumer] 메시지 수신 성공: " + eventId);

        // 1. 멱등성 체크: 이미 처리된 이벤트인지 DB 조회
        if (processedEventRepository.existsById(eventId)) {
            log.warn(">>>> [OrderConsumer] 중복 이벤트 감지 - 처리를 건너뜁니다. (EventId: {})", eventId);
            return;
        }

        // 2. 비즈니스 로직 실행 (실제 서비스에서는 타 도메인 상태 변경이나 외부 API 호출 등이 일어남)
        log.info(">>>> [OrderConsumer] 비즈니스 로직 수행 (Payload: {})", record.value());
        if ("INVALID_PAYLOAD".equals(record.value())) {
            log.error(">>>> [OrderConsumer] 비정상 페이로드 감지 - 예외를 던져 재시도를 유도합니다.");
            throw new IllegalArgumentException("Invalid payload error");
        }

        // 3. 처리 완료 이력 기록: 동일한 트랜잭션 내에서 처리 완료 사실을 영속화
        processedEventRepository.save(ProcessedEvent.of(eventId));
        
        log.info(">>>> [OrderConsumer] 이벤트 처리 및 멱등성 이력 저장 완료");
    }
}
