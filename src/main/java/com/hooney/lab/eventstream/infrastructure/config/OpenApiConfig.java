package com.hooney.lab.eventstream.infrastructure.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 📖 OpenApiConfig (API 문서화 설정)
 * 
 * [연구 목적]
 * 1. Swagger UI를 통해 Transactional Outbox 패턴을 시각적으로 테스트할 수 있는 환경을 제공합니다.
 * 2. API의 명세와 비즈니스 의미를 명확히 기록하여 협업 효율성을 높입니다.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI eventStreamingLabOpenAPI() {
        return new OpenAPI()
                .components(new Components())
                .info(new Info()
                        .title("🌊 Event Streaming Lab API")
                        .description("### Transactional Outbox & Idempotent Consumer Pattern 연구\n\n" +
                                "이 API는 마이크로서비스 아키텍처(MSA)에서 분산 트랜잭션 문제를 해결하기 위한 " +
                                "**Transactional Outbox** 패턴의 실습용 인터페이스입니다.\n\n" +
                                "#### 주요 실습 시나리오:\n" +
                                "1. **주문 생성**: 주문과 동시에 아웃박스(Outbox) 테이블에 이벤트를 원자적으로 저장합니다.\n" +
                                "2. **이벤트 발행**: 별도의 릴레이 프로세스가 아웃박스를 읽어 Kafka로 메시지를 전송합니다.\n" +
                                "3. **메시지 신뢰성**: Kafka 브로커 장애 시에도 메시지 유실 없이 전송되는지 확인합니다.")
                        .version("v1.0.0")
                        .contact(new Contact()
                                .name("Hooney")
                                .url("https://github.com/hooneyg"))
                        .license(new License()
                                .name("Apache 2.0")
                                .url("http://springdoc.org")));
    }
}
