package ru.example.apigateway;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.cloud.config.enabled=false",
                "eureka.client.enabled=false",
                // Настраиваем тестовый маршрут в никуда, чтобы гарантированно вызвать срабатывание Circuit Breaker
                "spring.cloud.gateway.routes[0].id=test-user-route",
                "spring.cloud.gateway.routes[0].uri=http://localhost:9999",
                "spring.cloud.gateway.routes[0].predicates[0]=Path=/api/users/**",
                "spring.cloud.gateway.routes[0].filters[0].name=CircuitBreaker",
                "spring.cloud.gateway.routes[0].filters[0].args.name=testBreaker",
                "spring.cloud.gateway.routes[0].filters[0].args.fallbackUri=forward:/fallback/users"
        }
)
@AutoConfigureWebTestClient
class ApiGatewayFallbackTest {

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void whenUserServiceIsDown_CircuitBreakerReturnsFallback() {
        webTestClient.get()
                .uri("/api/users")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
                .expectBody()
                .jsonPath("$.error")
                .isEqualTo("Сервис пользователей (user-service) временно недоступен. Попробуйте позже.");
    }
}
