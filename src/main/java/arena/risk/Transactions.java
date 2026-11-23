package arena.risk;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    Transactions(JdbcTemplate db, RiskClient risk) {
        this.db = db;
        this.risk = risk;
    }

    boolean insert(UUID id, long cents, String currency) {
        return db.update("INSERT INTO transactions (id,cents,currency) VALUES (?,?,?) ON CONFLICT DO NOTHING",
                id, cents, currency) == 1;
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

