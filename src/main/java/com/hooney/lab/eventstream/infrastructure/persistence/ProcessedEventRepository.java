package com.hooney.lab.eventstream.infrastructure.persistence;

import com.hooney.lab.eventstream.domain.outbox.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
