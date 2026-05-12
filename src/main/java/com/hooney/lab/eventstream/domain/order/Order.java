package com.hooney.lab.eventstream.domain.order;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

/**
 * 📦 Order (주문 엔티티)
 * 
 * [연구 목적]
 * 비즈니스 상태 변화의 주체입니다. 
 * 주문이 생성되면 반드시 '주문 생성 이벤트'가 아웃박스에 함께 저장되어야 합니다.
 */
@Entity
@Table(name = "orders") // 예약어 충돌 방지를 위해 테이블명 명시
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String product; // 상품명

    @Column(nullable = false)
    private Long amount; // 주문 금액

    @Column(nullable = false)
    private String customerEmail; // 고객 이메일

    @Enumerated(EnumType.STRING)
    private OrderStatus status; // 주문 상태

    private LocalDateTime createdAt; // 생성 일시

    /**
     * 주문 생성 팩토리 메서드
     */
    public static Order create(String product, Long amount, String customerEmail) {
        return Order.builder()
                .product(product)
                .amount(amount)
                .customerEmail(customerEmail)
                .status(OrderStatus.CREATED)
                .createdAt(LocalDateTime.now())
                .build();
    }
}
