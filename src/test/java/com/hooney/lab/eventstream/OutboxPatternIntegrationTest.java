package com.hooney.lab.eventstream;

import com.hooney.lab.eventstream.application.OrderService;
import com.hooney.lab.eventstream.domain.outbox.OutboxStatus;
import com.hooney.lab.eventstream.domain.outbox.ProcessedEvent;
import com.hooney.lab.eventstream.infrastructure.persistence.OrderRepository;
import com.hooney.lab.eventstream.infrastructure.persistence.OutboxRepository;
import com.hooney.lab.eventstream.infrastructure.persistence.ProcessedEventRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 🧪 OutboxPatternIntegrationTest
 * 
 * [연구 및 검증 항목]
 * 1. Transactional Outbox: 주문 생성 시 DB에 주문과 이벤트가 동시에 저장되는가?
 * 2. Event Relay: 스케줄러가 이벤트를 Kafka로 정상 발행하고 상태를 변경하는가?
 * 3. Idempotent Consumer: 소비자가 메시지를 수신하여 비즈니스 로직을 수행하고 중복을 방지하는가?
 */
@SpringBootTest
@Testcontainers
class OutboxPatternIntegrationTest {

    // 🐳 MySQL 컨테이너 정의
    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
            .withDatabaseName("eventdb")
            .withUsername("root")
            .withPassword("root");

    // 🐳 Kafka 컨테이너 정의
    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    // ⚙️ 동적으로 생성된 컨테이너 정보를 스프링 설정에 주입
    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Test
    @DisplayName("Transactional Outbox 패턴 전체 프로세스 검증 (발행부터 소비까지)")
    void outbox_pattern_full_cycle_test() {
        // [Given] 새로운 주문 요청 데이터
        String product = "MacBook Pro M3";
        Long amount = 3500000L;
        String email = "hooney@example.com";

        // [When] 주문 생성 (비즈니스 로직 + Outbox 적재)
        Long orderId = orderService.createOrder(product, amount, email);

        // [Then] 1단계: DB에 주문과 아웃박스가 'INIT' 상태로 저장되었는지 확인
        assertThat(orderRepository.existsById(orderId)).isTrue();
        
        var outboxes = outboxRepository.findAllByStatus(OutboxStatus.INIT);
        assertThat(outboxes).hasSize(1);
        assertThat(outboxes.get(0).getAggregateId()).isEqualTo(orderId);
        System.out.println("✅ 1단계 성공: 주문 및 아웃박스 DB 저장 완료");

        // [Then] 2단계: 스케줄러(Relay)가 작동하여 Kafka로 전송하고 상태를 'PUBLISHED'로 바꾸는지 확인 (최대 10초 대기)
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
                    var publishedOutboxes = outboxRepository.findAllByStatus(OutboxStatus.PUBLISHED);
                    assertThat(publishedOutboxes).hasSize(1);
                });
        System.out.println("✅ 2단계 성공: Event Relay에 의한 Kafka 발행 및 상태 업데이트 완료");

        // [Then] 3단계: Consumer가 Kafka 메시지를 수신하여 멱등성 이력을 남겼는지 확인
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
                    List<ProcessedEvent> processedEvents = processedEventRepository.findAll();
                    assertThat(processedEvents).isNotEmpty();
                    // Outbox ID가 Kafka Key로 사용되었으므로 식별 가능
                    assertThat(processedEvents.get(0).getEventId()).isNotNull();
                });
        System.out.println("✅ 3단계 성공: Consumer 수신 및 멱등성 처리 완료");
    }
}
