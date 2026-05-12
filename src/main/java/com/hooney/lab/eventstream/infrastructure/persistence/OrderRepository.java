package com.hooney.lab.eventstream.infrastructure.persistence;

import com.hooney.lab.eventstream.domain.order.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
}
