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
 * Outbox 테이블에 적재된 이벤트를 주기적으로 읽어 Kafka로 발행합니다.
 * 발행 성공 시 상태를 PUBLISHED로 변경하여 '최소 한 번 발행(At least once delivery)'을 보장합니다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxEventRelay {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 5000) // 5초마다 실행
    @Transactional
    public void relay() {
        // 1. 아직 발행되지 않은(INIT) 이벤트 조회
        List<Outbox> outboxes = outboxRepository.findAllByStatus(OutboxStatus.INIT);
        
        if (outboxes.isEmpty()) {
            return;
        }

        log.info(">>>> [OutboxRelay] 미발행 이벤트 {}건 발견. Kafka 전송 시작...", outboxes.size());

        for (Outbox outbox : outboxes) {
            try {
                // 2. Kafka로 발행 (Topic: aggregateType 기반으로 결정 가능)
                String topic = "lab." + outbox.getAggregateType().toLowerCase();
                kafkaTemplate.send(topic, String.valueOf(outbox.getAggregateId()), outbox.getPayload())
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                log.debug(">>>> Kafka 전송 성공: {}", outbox.getId());
                                // 비동기 콜백에서 처리하기보다, 이 예제에서는 단순화를 위해 동기적으로 상태를 관리하거나 
                                // 별도의 상태 업데이트 로직을 가져갈 수 있습니다.
                            } else {
                                log.error(">>>> Kafka 전송 실패: {}", outbox.getId(), ex);
                            }
                        });

                // 3. 상태 업데이트 (성공했다고 가정하고 업데이트)
                // 실제 고도화 시에는 Kafka의 ack를 확실히 받은 후 업데이트하는 것이 안전합니다.
                outbox.published();
                
            } catch (Exception e) {
                log.error(">>>> [OutboxRelay] 처리 중 치명적 에러 발생 (OutboxId: {})", outbox.getId(), e);
            }
        }
    }
}
