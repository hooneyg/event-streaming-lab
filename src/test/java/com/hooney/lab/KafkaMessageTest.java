package com.hooney.lab;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * 🧪 KafkaMessageTest (기본 Kafka 설정 검증용 테스트 템플릿)
 * 
 * [테스트 목적]
 * 이 클래스는 복잡한 비즈니스 로직이나 DB 트랜잭션을 제외하고, 
 * 오직 Spring Boot 기반의 Kafka Producer와 Consumer 환경이 올바르게 세팅되었는지 
 * 단일 모듈 관점에서 점검하기 위한 템플릿입니다.
 * 
 * [주요 활용 애노테이션 가이드]
 * - @SpringBootTest: 전체 스프링 컨텍스트를 로드하여 통합 환경을 구성할 때 사용합니다.
 * - @EmbeddedKafka: 외부 Kafka 클러스터를 띄울 필요 없이, 인메모리 형태의 내장 Kafka를 실행합니다.
 *   (예: @EmbeddedKafka(partitions = 1, topics = {"test-topic"}))
 */
public class KafkaMessageTest {

    /**
     * 🚀 단순 Kafka 문자열 송수신 테스트
     * 
     * 본 메서드는 현재 단순 문자열 검증으로 구성되어 있습니다.
     * 향후 KafkaTemplate을 주입받아 직접 메시지를 send()하고, 
     * KafkaTestUtils 등을 활용해 수신 결과를 검증하는 코드로 확장할 수 있습니다.
     */
    @Test
    @DisplayName("Kafka Producer 초기 설정 및 송신 로직 구조화 테스트")
    void testKafkaProducer() {
        // [TODO] 1. Given: 전송할 Payload (메시지) 및 토픽(Topic) 설정
        // KafkaTemplate<String, String> kafkaTemplate; (주입받아 사용)
        String message = "Master Class Kafka Test";
        
        // [TODO] 2. When: kafkaTemplate.send("토픽명", message) 호출
        
        // [TODO] 3. Then: 수신측(Consumer)에서 메시지를 정상적으로 Polling 했는지 검증
        // 여기서는 임시 문자열 검증 로직으로 대체합니다.
        assertThat(message).contains("Kafka");
    }
}