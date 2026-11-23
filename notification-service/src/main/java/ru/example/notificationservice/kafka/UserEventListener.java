package ru.example.notificationservice.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.example.notificationservice.dto.UserEventDto;
import ru.example.notificationservice.service.EmailService;

@Component
public class UserEventListener {

    private static final Logger log = LoggerFactory.getLogger(UserEventListener.class);
    private final EmailService emailService;

    public UserEventListener(EmailService emailService) {
        this.emailService = emailService;
    }

    @KafkaListener(topics = "user-events", groupId = "notification-group")
    public void handleUserEvent(UserEventDto event) {
        if (event == null || event.getOperation() == null) {
            log.warn("Получено пустое событие или событие без операции: {}", event);
            return;
        }

        String operation = event.getOperation().toUpperCase();

        switch (operation) {
            case "CREATE" -> emailService.sendEmail(
                    event.getEmail(),
                    "Успешная регистрация",
                    "Здравствуйте! Ваш аккаунт на сайте ваш сайт был успешно создан."
            );
            case "DELETE" -> emailService.sendEmail(
                    event.getEmail(),
                    "Удаление аккаунта",
                    "Здравствуйте! Ваш аккаунт был удалён."
            );
            default -> log.warn("Получена неизвестная операция '{}'. Письмо не отправлено.", event.getOperation());
        }
    }
}

