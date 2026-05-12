package com.hooney.lab.eventstream.domain.outbox;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

/**
 * 📮 Outbox (아웃박스 엔티티)
 * 
 * [역할]
 * 1. 비즈니스 로직과 동일한 트랜잭션에서 이벤트를 저장.
 * 2. 메시지 브로커(Kafka)로의 전송 보장(Guaranteed Delivery)을 위한 임시 저장소.
 */
@Entity
@Table(name = "outbox")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class Outbox {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String aggregateType; // 예: "ORDER"

    @Column(nullable = false)
    private Long aggregateId;    // 예: Order ID

    @Column(nullable = false)
    private String eventType;    // 예: "ORDER_CREATED"

    @Lob
    @Column(nullable = false, columnDefinition = "LONGTEXT")
    private String payload;      // 이벤트 데이터 (JSON)

    @Enumerated(EnumType.STRING)
    private OutboxStatus status; // INIT, PUBLISHED, FAILED

    private LocalDateTime createdAt;

    public static Outbox create(String aggregateType, Long aggregateId, String eventType, String payload) {
        return Outbox.builder()
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .eventType(eventType)
                .payload(payload)
                .status(OutboxStatus.INIT)
                .createdAt(LocalDateTime.now())
                .build();
    }

    public void published() {
        this.status = OutboxStatus.PUBLISHED;
    }
}
