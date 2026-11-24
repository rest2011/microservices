package ru.example.notificationservice;

import com.icegreen.greenmail.configuration.GreenMailConfiguration;
import com.icegreen.greenmail.junit5.GreenMailExtension;
import com.icegreen.greenmail.util.ServerSetup;
import jakarta.mail.internet.MimeMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import ru.example.notificationservice.dto.EmailRequestDto;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.mail.host=localhost",
                "spring.mail.port=3025",
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.cloud.config.enabled=false",
                "eureka.client.enabled=false"
        }
)
@EmbeddedKafka(partitions = 1, topics = {"user-events", "user-events-dlt"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class NotificationIntegrationTest {

    @RegisterExtension
    static GreenMailExtension greenMail = new GreenMailExtension(new ServerSetup(3025, null, ServerSetup.PROTOCOL_SMTP))
            .withConfiguration(GreenMailConfiguration.aConfig().withDisabledAuthentication());

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private TestRestTemplate restTemplate;

    private KafkaTemplate<String, String> stringKafkaTemplate;

    @BeforeEach
    void setUp() throws Exception {
        greenMail.purgeEmailFromAllMailboxes();

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put("key.serializer", StringSerializer.class);
        producerProps.put("value.serializer", StringSerializer.class);
        stringKafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    @Test
    void kafkaCreateEvent_FullChain_SendsWelcomeEmail() throws Exception {
        String jsonEvent = "{\"email\":\"newuser@test.com\",\"operation\":\"CREATE\"}";

        stringKafkaTemplate.send("user-events", jsonEvent);

        assertTrue(greenMail.waitForIncomingEmail(5000, 1));

        MimeMessage message = Arrays.stream(greenMail.getReceivedMessages())
                .findFirst()
                .orElseThrow();
        String recipient = Arrays.stream(message.getAllRecipients())
                .findFirst()
                .orElseThrow()
                .toString();

        assertEquals("newuser@test.com", recipient);
        assertTrue(message.getContent().toString()
                .contains("Здравствуйте! Ваш аккаунт на сайте ваш сайт был успешно создан."));
    }

    @Test
    void kafkaDeleteEvent_FullChain_SendsGoodbyeEmail() throws Exception {
        String jsonEvent = "{\"email\":\"deleteduser@test.com\",\"operation\":\"DELETE\"}";

        stringKafkaTemplate.send("user-events", jsonEvent);

        assertTrue(greenMail.waitForIncomingEmail(5000, 1));

        MimeMessage message = Arrays.stream(greenMail.getReceivedMessages())
                .findFirst()
                .orElseThrow();
        String recipient = Arrays.stream(message.getAllRecipients())
                .findFirst()
                .orElseThrow()
                .toString();

        assertEquals("deleteduser@test.com", recipient);
        assertTrue(message.getContent().toString()
                .contains("Здравствуйте! Ваш аккаунт был удалён."));
    }

    @Test
    void kafkaUnknownOperation_DoesNotSendEmail() {
        String jsonEvent = "{\"email\":\"unknown@test.com\",\"operation\":\"UPDATE\"}";

        stringKafkaTemplate.send("user-events", jsonEvent);

        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> assertEquals(0, greenMail.getReceivedMessages().length));
    }

    @Test
    void kafkaBrokenJson_SentToDeadLetterTopic() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dlt-test-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<String, String> dltConsumer = new DefaultKafkaConsumerFactory<>(
                consumerProps, new StringDeserializer(), new StringDeserializer()
        ).createConsumer();
        dltConsumer.subscribe(Collections.singletonList("user-events-dlt"));

        String brokenJson = "{broken-json-data: 123";
        stringKafkaTemplate.send("user-events", brokenJson);

        ConsumerRecord<String, String> dltRecord = KafkaTestUtils.getSingleRecord(dltConsumer, "user-events-dlt", Duration.ofSeconds(10));
        assertNotNull(dltRecord);

        // Очищаем кавычки JSON и декодируем Base64 обратно в исходную строку
        String base64Payload = dltRecord.value().replace("\"", "");
        String decodedPayload = new String(Base64.getDecoder().decode(base64Payload), StandardCharsets.UTF_8);

        assertEquals(brokenJson, decodedPayload);
        assertEquals(0, greenMail.getReceivedMessages().length);

        dltConsumer.close();
    }


    @Test
    void sendNotificationViaApi_Success_WhenValidEmail() throws Exception {
        EmailRequestDto request = new EmailRequestDto(
                "apiuser@test.com",
                "Тестовая тема",
                "Текст из REST API"
        );

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/notifications/send",
                request,
                String.class
        );

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(greenMail.waitForIncomingEmail(5000, 1));

        MimeMessage message = Arrays.stream(greenMail.getReceivedMessages())
                .findFirst()
                .orElseThrow();
        String recipient = Arrays.stream(message.getAllRecipients())
                .findFirst()
                .orElseThrow()
                .toString();

        assertEquals("apiuser@test.com", recipient);
        assertTrue(message.getContent().toString().contains("Текст из REST API"));
    }

    @Test
    void sendNotificationViaApi_Returns400_WhenInvalidEmail() {
        EmailRequestDto invalidRequest = new EmailRequestDto(
                "not-a-valid-email",
                "Тема",
                "Текст"
        );

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/notifications/send",
                invalidRequest,
                String.class
        );

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals(0, greenMail.getReceivedMessages().length);
    }
}
