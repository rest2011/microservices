package ru.example.apigateway.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping("/fallback")
public class FallbackController {

    @RequestMapping("/users")
    public Mono<ResponseEntity<Map<String, String>>> userServiceFallback() {
        return Mono.just(ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "Сервис пользователей (user-service) временно недоступен. Попробуйте позже.")));
    }

    @RequestMapping("/notifications")
    public Mono<ResponseEntity<Map<String, String>>> notificationServiceFallback() {
        return Mono.just(ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "Сервис уведомлений (notification-service) временно недоступен. Попробуйте позже.")));
    }
}
