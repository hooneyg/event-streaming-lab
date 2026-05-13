package com.hooney.lab.eventstream.interfaces.rest;

import com.hooney.lab.eventstream.application.OrderService;
import com.hooney.lab.eventstream.interfaces.rest.dto.OrderRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 🎮 OrderController (주문 인터페이스 계층)
 * 
 * [연구 대상: Transactional Outbox Pattern 실행]
 * 
 * [역할]
 * 1. 외부(웹 브라우저, Swagger)로부터의 주문 요청을 수신합니다.
 * 2. 입력값에 대한 검증을 수행합니다.
 * 3. 서비스 계층(OrderService)을 호출하여 주문 저장 및 Outbox 적재 트랜잭션을 시작합니다.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
@Tag(name = "Orders", description = "주문 관리 API (Transactional Outbox 패턴 테스트용)")
public class OrderController {

    private final OrderService orderService;

    /**
     * 주문 생성 API
     * 
     * [흐름]
     * POST /api/v1/orders 호출 -> 컨트롤러 수신 -> 서비스 계층 호출
     * -> DB 트랜잭션 시작 -> Order 저장 & Outbox 저장 -> 트랜잭션 커밋
     * -> (이후 스케줄러가 Outbox를 읽어 Kafka로 전송)
     */
    @PostMapping
    @Operation(
        summary = "새 주문 생성", 
        description = "주문을 생성하고 **Transactional Outbox** 패턴을 사용하여 이벤트를 DB에 원자적으로 적재합니다. " +
                      "이후 스케줄러가 이 데이터를 읽어 Kafka로 전송합니다."
    )
    @io.swagger.v3.oas.annotations.responses.ApiResponses(value = {
        @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "주문 생성 성공 및 Outbox 적재 완료"),
        @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "잘못된 요청 파라미터"),
        @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "500", description = "서버 내부 오류")
    })
    public ResponseEntity<Long> createOrder(@Valid @RequestBody OrderRequest request) {
        log.info(">>>> [OrderController] API 호출 수신: {}", request.getProduct());

        // 서비스 계층 호출을 통해 비즈니스 로직 및 아웃박스 패턴 실행
        Long orderId = orderService.createOrder(
                request.getProduct(),
                request.getAmount(),
                request.getCustomerEmail()
        );

        log.info(">>>> [OrderController] 주문 생성 완료 (ID: {})", orderId);
        
        return ResponseEntity.ok(orderId);
    }
}
