package ru.example.notificationservice.controller;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.example.notificationservice.dto.EmailRequestDto;
import ru.example.notificationservice.service.EmailService;

@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private final EmailService emailService;

    public NotificationController(EmailService emailService) {
        this.emailService = emailService;
    }

    @PostMapping("/send")
    public ResponseEntity<String> sendNotification(@Valid @RequestBody EmailRequestDto request) {
        emailService.sendEmail(request.getEmail(), request.getSubject(), request.getText());
        return ResponseEntity.ok("Сообщение успешно отправлено");
    }
}
