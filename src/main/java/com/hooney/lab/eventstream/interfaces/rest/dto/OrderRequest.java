package com.hooney.lab.eventstream.interfaces.rest.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 📦 OrderRequest (주문 생성 요청 DTO)
 * 
 * [연구 목적]
 * 사용자가 입력한 주문 정보를 캡슐화하여 API 계층에서 서비스 계층으로 전달합니다.
 * Swagger(OpenAPI) 어노테이션을 사용하여 문서상에 의미를 명확히 기록합니다.
 */
@Getter
@Setter
@NoArgsConstructor
@Schema(description = "주문 생성 요청 정보")
public class OrderRequest {

    @NotBlank(message = "제품명은 필수입니다.")
    @Schema(description = "주문할 제품의 이름", example = "MacBook M5 Max")
    private String product;

    @NotNull(message = "금액은 필수입니다.")
    @Min(value = 1, message = "금액은 1원 이상이어야 합니다.")
    @Schema(description = "제품 가격 (단위: 원)", example = "6800000")
    private Long amount;

    @NotBlank(message = "고객 이메일은 필수입니다.")
    @Schema(description = "주문 고객의 이메일 주소", example = "user@example.com")
    private String customerEmail;
}
