package com.hooney.lab;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class ReliableProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 무손실 보장
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // 중복 전송 방지
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>("[YOUR_TOPIC]", "key", "Reliable Message"));
        producer.close();
    }
}