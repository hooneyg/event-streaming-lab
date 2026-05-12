package com.hooney.lab.eventstream.infrastructure.persistence;

import com.hooney.lab.eventstream.domain.outbox.Outbox;
import com.hooney.lab.eventstream.domain.outbox.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface OutboxRepository extends JpaRepository<Outbox, Long> {
    List<Outbox> findAllByStatus(OutboxStatus status);
}
