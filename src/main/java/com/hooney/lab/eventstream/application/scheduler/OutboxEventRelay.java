package com.hooney.lab.eventstream.application.scheduler;

import com.hooney.lab.eventstream.domain.outbox.Outbox;
import com.hooney.lab.eventstream.domain.outbox.OutboxStatus;
import com.hooney.lab.eventstream.infrastructure.persistence.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 📡 OutboxEventRelay (이벤트 중계기)
 * 
 * [연구 대상: Event Relay]
 * Outbox 테이블에 적재된 이벤트를 주기적으로 읽어 Kafka로 발행하는 역할을 합니다.
 * 
 * [최소 한 번 발행 (At-least-once delivery) 보장 전략]
 * 1. 아직 발행되지 않은(INIT) 상태의 이벤트를 DB에서 조회합니다.
 * 2. Kafka로 성공적으로 전송된 것이 확인되면 상태를 PUBLISHED로 변경합니다.
 * 3. 만약 전송 중에 장애가 발생하면 상태가 INIT으로 유지되어, 다음 스케줄링 때 재시도됩니다.
 * 4. 이 과정에서 중복 발행이 발생할 수 있으나, 이는 Consumer 측에서 멱등성 로직으로 해결합니다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxEventRelay {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 주기적으로 미발행 이벤트를 체크하여 전송
     * fixedDelay 설정을 통해 이전 작업 종료 후 5초 간격으로 실행됩니다.
     */
    @Scheduled(fixedDelay = 5000) 
    @Transactional
    public void relay() {
        // 1. 전송 대기 중인(INIT) 이벤트 리스트 조회
        List<Outbox> outboxes = outboxRepository.findAllByStatus(OutboxStatus.INIT);
        
        if (outboxes.isEmpty()) {
            return;
        }

        log.info(">>>> [OutboxRelay] 미발행 이벤트 {}건 발견. Kafka 전송 프로세스 가동...", outboxes.size());

        for (Outbox outbox : outboxes) {
            try {
                // 2. Kafka 전송: 토픽명은 'lab.도커명' 형식을 따름
                String topic = "lab." + outbox.getAggregateType().toLowerCase();
                
                // Key값으로 aggregateId를 주어 동일 주문에 대한 이벤트 순서 보장 유도
                kafkaTemplate.send(topic, String.valueOf(outbox.getAggregateId()), outbox.getPayload())
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                log.debug(">>>> [OutboxRelay] Kafka 전송 성공 (ID: {})", outbox.getId());
                            } else {
                                log.error(">>>> [OutboxRelay] Kafka 전송 실패 (ID: {})", outbox.getId(), ex);
                            }
                        });

                // 3. 전송 시도 후 상태 변경 (성공/실패 여부에 따라 상태 관리 고도화 가능)
                outbox.published();
                
            } catch (Exception e) {
                log.error(">>>> [OutboxRelay] 이벤트 처리 중 예외 발생 (ID: {})", outbox.getId(), e);
            }
        }
    }
}
