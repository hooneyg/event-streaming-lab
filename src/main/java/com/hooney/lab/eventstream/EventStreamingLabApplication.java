package com.hooney.lab.eventstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 🌊 Event Streaming Lab (이벤트 스트림 분산 처리 연구소)
 * 
 * [연구 주제]
 * 1. Transactional Outbox Pattern을 활용한 메시지 발행 보장
 * 2. Kafka를 활용한 분산 이벤트 처리
 * 3. 멱등성 소비자(Idempotent Consumer) 구현
 */
@EnableScheduling
@SpringBootApplication
public class EventStreamingLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventStreamingLabApplication.class, args);
    }

}
