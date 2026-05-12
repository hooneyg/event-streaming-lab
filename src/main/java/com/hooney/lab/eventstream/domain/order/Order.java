package com.hooney.lab.eventstream.domain.order;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

/**
 * 📦 Order (주문 엔티티)
 * 비즈니스 도메인의 핵심 엔티티입니다.
 */
@Entity
@Table(name = "orders")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String product;

    @Column(nullable = false)
    private Long amount;

    @Column(nullable = false)
    private String customerEmail;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    private LocalDateTime createdAt;

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
