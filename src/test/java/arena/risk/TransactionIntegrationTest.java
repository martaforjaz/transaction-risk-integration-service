package arena.risk;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "risk.async=false")
class TransactionIntegrationTest {
    private static final PostgreSQLContainer<?> DB = new PostgreSQLContainer<>("postgres:17-alpine");
    private static final AtomicBoolean OFFLINE = new AtomicBoolean();
    private static final AtomicInteger CALLS = new AtomicInteger();
    private static final HttpServer RISK = riskServer();

    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate db;

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry properties) {
        DB.start();
        properties.add("spring.datasource.url", DB::getJdbcUrl);
        properties.add("spring.datasource.username", DB::getUsername);
        properties.add("spring.datasource.password", DB::getPassword);
        properties.add("risk.url", () -> "http://127.0.0.1:" + RISK.getAddress().getPort() + "/evaluate");
    }

    @AfterAll
    static void stop() {
        RISK.stop(0);
    }

    @Test
    void concurrentDuplicateAcrossAdaptersProducesOneDecision() {
        var id = UUID.randomUUID();
        int before = CALLS.get();
        var gateway = CompletableFuture.runAsync(() -> assertThat(gateway(id, 1250).getStatusCode().is2xxSuccessful()).isTrue());
        var ledger = CompletableFuture.runAsync(() -> {
            var result = http.postForEntity("/transactions/ledger",
                    Map.of("reference", id, "total", "12.50", "ccy", "EUR"), Transactions.State.class);
            assertThat(result.getStatusCode().is2xxSuccessful()).isTrue();
        });
        CompletableFuture.allOf(gateway, ledger).join();

        assertThat(read(id).decision()).isEqualTo("ALLOW");
        assertThat(read(id).attempts()).isEqualTo(1);
        assertThat(CALLS.get() - before).isEqualTo(1);
        assertThat(db.queryForObject("SELECT COUNT(*) FROM transactions WHERE id=?", Integer.class, id)).isEqualTo(1);
        assertThat(gateway(id, 1400).getStatusCode().value()).isEqualTo(409);
    }

    @Test
    void failedEvaluationIsPersistedAndExplicitRetryResumesIt() {
        var id = UUID.randomUUID();
        int before = CALLS.get();
        OFFLINE.set(true);
        try {
            var initial = gateway(id, 50000);
            assertThat(initial.getStatusCode().value()).isEqualTo(202);
            assertThat(read(id).status()).isEqualTo("FAILED");
            assertThat(read(id).attempts()).isEqualTo(1);
            assertThat(read(id).decision()).isNull();
            assertThat(gateway(id, 50000).getStatusCode().value()).isEqualTo(202);
        } finally {
            OFFLINE.set(false);
        }

        var result = http.postForEntity("/transactions/{id}/retry", null, Transactions.State.class, id);
        assertThat(result.getStatusCode().value()).isEqualTo(200);
        assertThat(result.getBody().attempts()).isEqualTo(2);
        assertThat(read(id).status()).isEqualTo("DECIDED");
        http.postForEntity("/transactions/{id}/retry", null, Transactions.State.class, id);
        assertThat(CALLS.get() - before).isEqualTo(2);
    }

    private org.springframework.http.ResponseEntity<Transactions.State> gateway(UUID id, int amount) {
        return http.postForEntity("/transactions/gateway",
                Map.of("id", id, "amountMinor", amount, "currency", "EUR"), Transactions.State.class);
    }

    private Transactions.State read(UUID id) {
        return http.getForObject("/transactions/{id}", Transactions.State.class, id);
    }

    private static HttpServer riskServer() {
        try {
            var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/evaluate", exchange -> {
                try (exchange) {
                    CALLS.incrementAndGet();
                    exchange.getRequestBody().readAllBytes();
                    if (OFFLINE.get()) exchange.sendResponseHeaders(503, -1);
                    else {
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

