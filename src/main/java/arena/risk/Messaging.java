package arena.risk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "risk.async", havingValue = "true", matchIfMissing = true)
class Messaging {
    static final String WORK = "risk.evaluate";
    static final String DEAD = "risk.failed";

    record Event(UUID id, int attempt, int maxAttempt) {}

    @Bean
    NewTopic evaluationTopic() {
        return TopicBuilder.name(WORK).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic failedTopic() {
        return TopicBuilder.name(DEAD).partitions(1).replicas(1).build();
    }

    @Component
    @ConditionalOnProperty(name = "risk.async", havingValue = "true", matchIfMissing = true)
    static class Relay {
        private static final Logger LOG = LoggerFactory.getLogger(Relay.class);
        private final JdbcTemplate db;
        private final TransactionTemplate tx;
        private final KafkaTemplate<String, String> kafka;
        private final ObjectMapper json;

        Relay(JdbcTemplate db, TransactionTemplate tx, KafkaTemplate<String, String> kafka, ObjectMapper json) {
            this.db = db;
            this.tx = tx;
            this.kafka = kafka;
            this.json = json;
        }

        @Scheduled(fixedDelayString = "${risk.outbox-interval-ms:300}")
        void publishReady() {
            try {
                for (int i = 0; i < 20 && Boolean.TRUE.equals(tx.execute(status -> publishOne())); i++) {
                    // Bound each polling batch.
                }
            } catch (IllegalStateException error) {
                LOG.warn("Outbox publication deferred: {}", error.getMessage());
            }
        }

        private boolean publishOne() {
            var rows = db.query("""
                    SELECT seq, topic, transaction_id, attempt, max_attempt FROM risk_outbox
                    WHERE available_at <= now()
                    ORDER BY available_at, seq LIMIT 1 FOR UPDATE SKIP LOCKED
                    """, (rs, row) -> new Message(rs.getLong(1), rs.getString(2),
                    new Event(rs.getObject(3, UUID.class), rs.getInt(4), rs.getInt(5))));
            if (rows.isEmpty()) return false;

            var message = rows.getFirst();
            try {
                kafka.send(message.topic(), message.event().id().toString(),
                        json.writeValueAsString(message.event())).get(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Publication interrupted", error);
            } catch (Exception error) {
                throw new IllegalStateException("Broker did not acknowledge publication", error);
            }
            db.update("DELETE FROM risk_outbox WHERE seq=?", message.seq());
            return true;
        }

        private record Message(long seq, String topic, Event event) {}
    }

    @Component
    @ConditionalOnProperty(name = "risk.async", havingValue = "true", matchIfMissing = true)
    static class Consumer {
        private final ObjectMapper json;
        private final Transactions transactions;

        Consumer(ObjectMapper json, Transactions transactions) {
            this.json = json;
            this.transactions = transactions;
        }

        @KafkaListener(id = "risk-worker", topics = WORK)
        void receive(String payload) throws JsonProcessingException {
            transactions.consume(json.readValue(payload, Event.class));
        }
    }
}

