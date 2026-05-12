package com.hooney.lab.eventstream.domain.outbox;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

/**
 * ✅ ProcessedEvent (처리된 이벤트 기록)
 * 
 * [역할: Idempotent Consumer]
 * 중복 수신된 메시지를 걸러내기 위해 이미 처리 완료된 이벤트 ID를 저장합니다.
 */
@Entity
@Table(name = "processed_events")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class ProcessedEvent {

    @Id
    private String eventId; // Kafka 메시지 키 또는 고유 이벤트 ID

    private LocalDateTime processedAt;

    public static ProcessedEvent of(String eventId) {
        return ProcessedEvent.builder()
                .eventId(eventId)
                .processedAt(LocalDateTime.now())
                .build();
    }
}
