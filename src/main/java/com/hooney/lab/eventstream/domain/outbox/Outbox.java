package com.hooney.lab.eventstream.domain.outbox;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

/**
 * 📮 Outbox (아웃박스 엔티티)
 * 
 * [역할]
 * 1. 비즈니스 로직과 동일한 트랜잭션에서 이벤트를 저장하여 발행을 보장합니다.
 * 2. 메시지 브로커(Kafka)가 잠시 불능 상태라도, 이벤트는 DB에 안전하게 보관됩니다.
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
    private String aggregateType; // 집계 대상 타입 (예: "ORDER")

    @Column(nullable = false)
    private Long aggregateId;    // 집계 대상 식별자 (예: Order ID)

    @Column(nullable = false)
    private String eventType;    // 발생한 이벤트의 종류 (예: "ORDER_CREATED")

    @Lob
    @Column(nullable = false, columnDefinition = "LONGTEXT")
    private String payload;      // 이벤트 상세 데이터 (JSON 형식)

    @Enumerated(EnumType.STRING)
    private OutboxStatus status; // 처리 상태 (INIT: 대기, PUBLISHED: 발행완료, FAILED: 실패)

    private LocalDateTime createdAt; // 생성 일시

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

    /**
     * 발행 완료 상태로 변경
     */
    public void published() {
        this.status = OutboxStatus.PUBLISHED;
    }
}
