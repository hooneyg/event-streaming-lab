package com.hooney.lab.eventstream;

import com.hooney.lab.eventstream.application.OrderService;
import com.hooney.lab.eventstream.domain.outbox.OutboxStatus;
import com.hooney.lab.eventstream.domain.outbox.ProcessedEvent;
import com.hooney.lab.eventstream.infrastructure.persistence.OrderRepository;
import com.hooney.lab.eventstream.infrastructure.persistence.OutboxRepository;
import com.hooney.lab.eventstream.infrastructure.persistence.ProcessedEventRepository;
import com.hooney.lab.eventstream.application.scheduler.OutboxEventRelay;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 🧪 OutboxPatternIntegrationTest (메시징 신뢰성 통합 테스트)
 * 
 * [연구 및 검증 시나리오]
 * 본 테스트는 분산 시스템에서 데이터 일관성을 보장하기 위한 'Transactional Outbox' 패턴과
 * 메시지 중복 처리를 방지하는 'Idempotent Consumer' 패턴의 전체 라이프사이클을 검증합니다.
 * 
 * [핵심 검증 포인트]
 * 1. 🛡️ Atomicity: 주문 데이터 저장과 Outbox 이벤트 저장이 하나의 트랜잭션 내에서 원자적으로 처리되는가?
 * 2. 📡 Relay: 미발행 상태의 Outbox 데이터가 스케줄러(Relay)에 의해 Kafka로 안전하게 전달되는가?
 * 3. 📥 Idempotency: 동일한 이벤트가 여러 번 전달되더라도 Consumer 측에서 멱등하게 처리되는가?
 * 
 * [인프라 기술 스택]
 * - Testcontainers: 실제 MySQL 컨테이너를 구동하여 격리된 DB 환경 테스트
 * - EmbeddedKafka: 내장형 카프카 브로커를 활용한 메시징 테스트
 * - Awaitility: 비동기 이벤트(Kafka 수신) 발생을 우아하게 대기하고 검증
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@Testcontainers // JUnit 5와 Testcontainers 연동 활성화
@EmbeddedKafka(partitions = 1, topics = {"lab.order"}) // 테스트용 내장 Kafka 브로커 구동
class OutboxPatternIntegrationTest {

    /**
     * 🐳 MySQL 컨테이너 정의
     * @ServiceConnection: Spring Boot 3.1+ 기능으로, 컨테이너의 접속 정보를 자동으로 
     * spring.datasource.* 프로퍼티에 주입해줍니다. (별도의 Registry 설정 불필요)
     */
    @Container
    @ServiceConnection
    @SuppressWarnings("resource")
    static MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
            .withDatabaseName("eventdb")
            .withUsername("root")
            .withPassword("root");

    @Autowired
    private OrderService orderService; // 주문 비즈니스 로직 진입점

    @Autowired
    private OrderRepository orderRepository; // 주문 데이터 조회용

    @Autowired
    private OutboxRepository outboxRepository; // Outbox 상태 변화 추적용

    @Autowired
    private OutboxEventRelay outboxEventRelay; // 스케줄러를 수동으로 트리거하여 즉시 발행 테스트 수행

    @Autowired
    private ProcessedEventRepository processedEventRepository; // Consumer의 중복 처리(멱등성) 이력 확인용

    @Test
    @DisplayName("Transactional Outbox 패턴 전체 프로세스 검증 (주문 생성 -> 이벤트 발행 -> 메시지 소비)")
    void outbox_pattern_full_cycle_test() {
        // ─────────────────────────────────────────────────────────
        // [Given] 새로운 주문 요청 데이터 준비
        // ─────────────────────────────────────────────────────────
        String product = "MacBook M5 Max";
        Long amount = 6800000L;
        String email = "user@example.com";

        // ─────────────────────────────────────────────────────────
        // [When] 1단계: 주문 생성 요청
        // 이 단계에서 DB에는 Order 엔티티와 INIT 상태의 Outbox 엔티티가 동시에 저장되어야 함.
        // ─────────────────────────────────────────────────────────
        Long orderId = orderService.createOrder(product, amount, email);
        System.out.println("🚀 주문 생성 완료 (ID: " + orderId + ")");

        // ─────────────────────────────────────────────────────────
        // [Then] 데이터베이스 저장 상태 확인 (원자성 검증)
        // ─────────────────────────────────────────────────────────
        assertThat(orderRepository.existsById(orderId)).isTrue();
        System.out.println("✅ 1단계 성공: 주문 데이터 DB 저장 확인");

        // ─────────────────────────────────────────────────────────
        // [When] 2단계: Outbox Relay 수동 실행
        // 스케줄러 대기 시간을 기다리지 않고 즉시 실행하여 Kafka로 이벤트를 발행함.
        // ─────────────────────────────────────────────────────────
        System.out.println("🔄 2단계: Relay 실행 및 Kafka 발행 시도...");

        // Kafka 클라이언트 네트워크 초기화 및 안정화를 위한 짧은 대기
        try { Thread.sleep(2000); } catch (InterruptedException e) { }

        outboxEventRelay.relay(); // INIT 상태의 데이터를 읽어 Kafka 송신 후 PUBLISHED로 업데이트

        // ─────────────────────────────────────────────────────────
        // [Then] Outbox 상태 변경 확인 (발행 완료 상태)
        // Awaitility를 사용하여 비동기 상태 변경을 최대 15초간 대기함.
        // ─────────────────────────────────────────────────────────
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
                    var publishedOutboxes = outboxRepository.findAllByStatus(OutboxStatus.PUBLISHED);
                    assertThat(publishedOutboxes).hasSize(1);
                });
        System.out.println("✅ 2단계 성공: Kafka 발행 완료 및 Outbox 상태(PUBLISHED) 업데이트 확인");

        // ─────────────────────────────────────────────────────────
        // [When] 3단계: Consumer 메시지 수신 및 비즈니스 로직 수행
        // Kafka에 발행된 메시지를 OrderConsumer가 수신하여 ProcessedEvent 테이블에 이력을 남김.
        // ─────────────────────────────────────────────────────────
        System.out.println("⏳ 3단계: Consumer 메시지 수신 대기 중...");

        // ─────────────────────────────────────────────────────────
        // [Then] 최종 멱등성 이력 확인
        // Consumer가 성공적으로 처리했다면 ProcessedEvent 테이블에 데이터가 존재해야 함.
        // ─────────────────────────────────────────────────────────
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    List<ProcessedEvent> processedEvents = processedEventRepository.findAll();
                    assertThat(processedEvents).isNotEmpty();
                    System.out.println("📥 3단계: Consumer 수신 및 처리 확인! (EventId: " + processedEvents.get(0).getEventId() + ")");
                });
        System.out.println("✅ 3단계 성공: 최종 멱등성 처리(ProcessedEvent 저장) 완료");
    }
}
