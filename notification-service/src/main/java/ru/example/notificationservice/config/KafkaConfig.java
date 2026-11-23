package ru.example.notificationservice.config;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    public CommonErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        // Отправляем сбойные сообщения в топик <имя_топика>-dlt (т.е. user-events-dlt)
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> {
                    log.error("Сообщение отправлено в DLT из-за ошибки: {}", ex.getMessage());
                    return new TopicPartition(record.topic() + "-dlt", record.partition());
                });

        // Делаем 2 повторные попытки с интервалом 1000 мс (1 сек), затем отправляем в DLT
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));

        return errorHandler;
    }
}
