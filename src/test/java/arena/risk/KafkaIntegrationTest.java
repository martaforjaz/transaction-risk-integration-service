package arena.risk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "risk.retry-delay-ms=4000")
class KafkaIntegrationTest {
    private static final PostgreSQLContainer<?> DB = new PostgreSQLContainer<>("postgres:17-alpine");
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<UUID> OFFLINE = ConcurrentHashMap.newKeySet();
    private static final Map<UUID, AtomicInteger> CALLS = new ConcurrentHashMap<>();
    private static final HttpServer RISK = startRisk();

    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate db;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaListenerEndpointRegistry listeners;

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry properties) {
        DB.start();
        KAFKA.start();
        properties.add("spring.datasource.url", DB::getJdbcUrl);
        properties.add("spring.datasource.username", DB::getUsername);
        properties.add("spring.datasource.password", DB::getPassword);
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        properties.add("risk.url", () -> "http://127.0.0.1:" + RISK.getAddress().getPort() + "/evaluate");
    }

    @AfterAll
    static void stopRisk() {
        RISK.stop(0);
    }

    @Test
    void duplicateKafkaDeliveryDoesNotEvaluateAgain() throws Exception {
        var id = submitOnline();
        var event = JSON.writeValueAsString(new Messaging.Event(id, 1, 3));
        kafka.send(Messaging.WORK, id.toString(), event).get(5, TimeUnit.SECONDS);
        kafka.send(Messaging.WORK, id.toString(), event).get(5, TimeUnit.SECONDS);
        // A new record on the same single partition cannot complete before both duplicates were read.
        submitOnline();
        assertThat(read(id).attempts()).isEqualTo(1);
        assertThat(CALLS.get(id).get()).isEqualTo(1);
    }

    @Test
    void retryPublishedWhileConsumerIsStoppedIsRecoveredOnRestart() {
        var id = UUID.randomUUID();
        var worker = listeners.getListenerContainer("risk-worker");
        OFFLINE.add(id);
        try {
            submit(id);
            await().atMost(Duration.ofSeconds(15)).until(() -> "FAILED".equals(read(id).status()));
            worker.stop();
            OFFLINE.remove(id);
            await().atMost(Duration.ofSeconds(15)).until(() -> db.queryForObject(
                    "SELECT COUNT(*) FROM risk_outbox WHERE transaction_id=?", Integer.class, id) == 0);
            assertThat(read(id).attempts()).isEqualTo(1);
            assertThat(CALLS.get(id).get()).isEqualTo(1);
        } finally {
            OFFLINE.remove(id);
            worker.start();
        }
        awaitDecision(id);
        assertThat(read(id).attempts()).isEqualTo(2);
        assertThat(CALLS.get(id).get()).isEqualTo(2);
    }

    @Test
    void exhaustedRetriesReachDeadLetterTopicAndCanBeReplayed() {
        var id = UUID.randomUUID();
        OFFLINE.add(id);
        try (var dead = new KafkaConsumer<String, String>(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "group.id", UUID.randomUUID().toString(),
                "auto.offset.reset", "earliest",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"))) {
            dead.subscribe(List.of(Messaging.DEAD));
            submit(id);
            await().pollInSameThread().atMost(Duration.ofSeconds(35)).until(() -> {
                for (var message : dead.poll(Duration.ofMillis(300))) {
                    var event = JSON.readValue(message.value(), Messaging.Event.class);
                    if (id.equals(event.id())) return event.attempt() == 3;
                }
                return false;
            });
            assertThat(read(id).status()).isEqualTo("FAILED");
            assertThat(CALLS.get(id).get()).isEqualTo(3);
        } finally {
            OFFLINE.remove(id);
        }
        assertThat(http.postForEntity("/transactions/{id}/retry", null, Transactions.State.class, id)
                .getStatusCode().is2xxSuccessful()).isTrue();
        awaitDecision(id);
        assertThat(read(id).attempts()).isEqualTo(4);
    }

    private UUID submitOnline() {
        var id = UUID.randomUUID();
        submit(id);
        awaitDecision(id);
        return id;
    }

    private void submit(UUID id) {
        assertThat(http.postForEntity("/transactions/gateway",
                Map.of("id", id, "amountMinor", 1250, "currency", "EUR"), Transactions.State.class)
                .getStatusCode().is2xxSuccessful()).isTrue();
    }

    private void awaitDecision(UUID id) {
        await().atMost(Duration.ofSeconds(20)).until(() -> "DECIDED".equals(read(id).status()));
    }

    private Transactions.State read(UUID id) {
        return http.getForObject("/transactions/{id}", Transactions.State.class, id);
    }

    private static HttpServer startRisk() {
        try {
            var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/evaluate", exchange -> {
                try (exchange) {
                    var id = UUID.fromString(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
                    CALLS.computeIfAbsent(id, key -> new AtomicInteger()).incrementAndGet();
                    exchange.getRequestBody().readAllBytes();
                    if (OFFLINE.contains(id)) {
                        exchange.sendResponseHeaders(503, -1);
                    } else {
                        var bytes = "{\"decision\":\"ALLOW\"}".getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        exchange.sendResponseHeaders(200, bytes.length);
                        exchange.getResponseBody().write(bytes);
                    }
                }
            });
            server.start();
            return server;
        } catch (IOException error) {
            throw new IllegalStateException(error);
        }
    }
}

