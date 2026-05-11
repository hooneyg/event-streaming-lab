package com.hooney.lab;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class KafkaMessageTest {
    @Test
    void testKafkaProducer() {
        // [YOUR_CONFIG] 여기에 카프카 설정을 넣고 테스트하세요.
        String message = "Master Class Kafka Test";
        assertThat(message).contains("Kafka");
    }
}