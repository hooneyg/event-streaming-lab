package com.hooney.lab.eventstream.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * 🛠️ JacksonConfig (JSON 직렬화 설정)
 * 
 * [연구 대상: ObjectMapper 설정]
 * 
 * 스프링 부트에서 JSON 데이터를 처리할 때 사용하는 ObjectMapper를 커스텀 설정합니다.
 * 특정 환경에서 자동 설정(Auto-configuration)이 누락되는 경우를 대비하여 명시적으로 빈(Bean)을 등록합니다.
 */
@Configuration
public class JacksonConfig {

    /**
     * ObjectMapper 빈(Bean)을 생성하여 컨텍스트에 등록합니다.
     * @Primary 어노테이션을 통해 기본 주입 대상으로 설정합니다.
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        
        // 1. Java 8 날짜/시간 모듈 등록 (LocalDateTime 등을 위해 필수)
        mapper.registerModule(new JavaTimeModule());
        
        // 2. 날짜를 타임스탬프 숫자 형태가 아닌 ISO-8601 문자열로 출력하도록 설정
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        
        return mapper;
    }
}
