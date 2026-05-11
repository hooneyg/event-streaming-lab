package com.hooney.lab;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/**
 * 메시지 무손실 전송을 보장하는 최적화된 카프카 프로듀서 샘플입니다.
 * 실무에서 이벤트 정합성이 중요할 때 필수적으로 적용해야 하는 설정들을 포함합니다.
 */
public class ReliableProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        
        // 1. 모든 복제본(ISR)에 메시지가 저장될 때까지 기다림 (무손실 보장)
        props.put(ProducerConfig.ACKS_CONFIG, "all"); 
        
        // 2. 멱등성 보장 (네트워크 재시도로 인한 메시지 중복 전송 방지)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); 
        
        // 3. 재시도 횟수를 최대로 설정하여 일시적인 장애 시 자동 복구
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // [YOUR_TOPIC] 자리에 실제 발행할 토픽명을 입력하세요.
        producer.send(new ProducerRecord<>("[YOUR_TOPIC]", "key", "Reliable Message"));
        
        producer.close();
    }
}