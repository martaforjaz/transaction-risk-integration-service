package arena.risk;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClientException;
import org.springframework.web.server.ResponseStatusException;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
class Transactions {
    private static final Logger LOG = LoggerFactory.getLogger(Transactions.class);
    private final JdbcTemplate db;
    private final RiskClient risk;
    private final boolean asynchronous;
    private final long retryDelayMs;

    Transactions(JdbcTemplate db, RiskClient risk,
                 @Value("${risk.async:true}") boolean asynchronous,
                 @Value("${risk.retry-delay-ms:1000}") long retryDelayMs) {
        this.db = db;
        this.risk = risk;
        this.asynchronous = asynchronous;
        this.retryDelayMs = retryDelayMs;
    }

    @Transactional
    public boolean insert(UUID id, long cents, String currency) {
        boolean created = db.update("INSERT INTO transactions (id,cents,currency) VALUES (?,?,?) ON CONFLICT DO NOTHING",
                id, cents, currency) == 1;
        if (created && asynchronous) enqueue(id, Messaging.WORK, 1, 3, 0);
        return created;
    }

    boolean asynchronous() {
        return asynchronous;
    }

    @Transactional
    public State scheduleRetry(UUID id) {
        var state = read(id, true);
        if (!"DECIDED".equals(state.status())) {
            enqueue(id, Messaging.WORK, state.attempts() + 1, state.attempts() + 3, 0);
        }
        return state;
    }

    @Transactional
    public void consume(Messaging.Event event) {
        var state = read(event.id(), true);
        if ("DECIDED".equals(state.status()) || event.attempt() != state.attempts() + 1) return;
        var result = evaluate(state);
        if ("FAILED".equals(result.status())) {
            if (event.attempt() < event.maxAttempt()) {
                long delay = retryDelayMs * (event.attempt() == event.maxAttempt() - 1 ? 2 : 1);
                enqueue(state.id(), Messaging.WORK, event.attempt() + 1, event.maxAttempt(), delay);
            } else {
                enqueue(state.id(), Messaging.DEAD, event.attempt(), event.maxAttempt(), 0);
            }
        }
    }

    private void enqueue(UUID id, String topic, int attempt, int maxAttempt, long delayMs) {
        db.update("""
                INSERT INTO risk_outbox (transaction_id, topic, attempt, max_attempt, available_at)
                VALUES (?,?,?,?, now() + ? * interval '1 millisecond') ON CONFLICT DO NOTHING
                """, id, topic, attempt, maxAttempt, delayMs);
    }

    State get(UUID id) {
        return read(id, false);
    }

    private State read(UUID id, boolean lock) {
        return db.query("SELECT * FROM transactions WHERE id = ?" + (lock ? " FOR UPDATE" : ""),
                (rs, n) -> new State(id, rs.getLong("cents"), rs.getString("currency"),
                        rs.getString("status"), rs.getInt("attempts"), rs.getString("decision")), id)
                .stream().findFirst().orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Unknown reference"));
    }

    @Transactional
    public State process(UUID id) {
        var state = read(id, true);
        if (state.status().equals("DECIDED")) return state;
        return evaluate(state);
    }

    private State evaluate(State state) {
        var id = state.id();
        String decision;
        try {
            decision = risk.evaluate(id, state.amountMinor(), state.currency());
        } catch (RestClientException e) {
            LOG.warn("Risk endpoint failed for {} on attempt {}", id, state.attempts() + 1);
            decision = null;
        }
        db.update("UPDATE transactions SET status=?, attempts=attempts+1, decision=? WHERE id=?",
                decision == null ? "FAILED" : "DECIDED", decision, id);
        return get(id);
    }

    record State(UUID id, long amountMinor, String currency, String status, int attempts, String decision) {}
}

