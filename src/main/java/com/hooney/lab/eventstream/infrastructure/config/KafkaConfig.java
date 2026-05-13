package com.hooney.lab.eventstream.infrastructure.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * ⚙️ KafkaConfig (Kafka 인프라 핵심 설정)
 * 
 * [설정 목적]
 * Event Streaming Lab 환경에서 Transactional Outbox 및 Idempotent Consumer 패턴을
 * 안정적으로 구현하기 위해 프로듀서(Producer)와 컨슈머(Consumer)의 상세 속성을 제어합니다.
 * 
 * [주요 특징]
 * 1. acks = all 및 enable.idempotence = true 설정으로 데이터 유실 없는 정확히 한 번(Exactly-once) 전송의 기초를 다집니다.
 * 2. @EnableKafka를 통해 애플리케이션 내의 @KafkaListener 애노테이션을 활성화합니다.
 */
@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:127.0.0.1:9094}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:lab-group}")
    private String groupId;

    // ═══════════════════════════════════════════════════════
    // 📨 Producer Configuration (메시지 발행자 설정)
    // ═══════════════════════════════════════════════════════
    
    /**
     * Kafka Producer 생성을 위한 팩토리 빈(Bean)입니다.
     * Transactional Outbox Pattern에서 DB 트랜잭션 성공 후 이벤트를 발송할 때 사용됩니다.
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        // 데이터 안정성 최상위 설정 (리더와 모든 팔로워가 메시지를 기록해야 성공으로 인정)
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        
        // 멱등성 프로듀서 활성화 (네트워크 재시도 시 중복 발행 방지)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Spring 프레임워크에서 제공하는 Kafka 메시지 전송용 고수준 템플릿입니다.
     * 이 템플릿을 통해 쉽게 메시지를 발행(send)할 수 있습니다.
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ═══════════════════════════════════════════════════════
    // 📥 Consumer Configuration (메시지 수신자 설정)
    // ═══════════════════════════════════════════════════════
    
    /**
     * Kafka Consumer 생성을 위한 팩토리 빈(Bean)입니다.
     * 이벤트를 수신하여 비즈니스 로직(예: Idempotent Consumer 패턴)을 수행할 때 사용됩니다.
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        // 새 컨슈머 그룹이 생성되거나 오프셋 정보가 없을 때 가장 처음부터 메시지를 읽도록 설정
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * @KafkaListener 애노테이션이 부착된 메서드를 감지하여 비동기 메시지 수신 컨테이너를 생성하는 팩토리입니다.
     * 이 빈이 등록되어 있어야 통합 테스트 및 실제 런타임에서 리스너가 정상적으로 초기화됩니다.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
