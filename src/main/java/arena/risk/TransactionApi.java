package arena.risk;

import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CONFLICT;

@RestController
@RequestMapping("/transactions")
class TransactionApi {
    private final Transactions transactions;

    TransactionApi(Transactions transactions) {
        this.transactions = transactions;
    }

    @PostMapping("/gateway")
    ResponseEntity<Transactions.State> gateway(@RequestBody Gateway input) {
        return submit(input.id(), input.amountMinor(), input.currency());
    }

    @PostMapping("/ledger")
    ResponseEntity<Transactions.State> ledger(@RequestBody Ledger input) {
        try {
            return submit(input.reference(), input.total().movePointRight(2).longValueExact(), input.ccy());
        } catch (ArithmeticException | NullPointerException e) {
            throw new ResponseStatusException(BAD_REQUEST, "Expected an exact two-decimal amount");
        }
    }

    @GetMapping("/{id}")
    Transactions.State read(@PathVariable UUID id) {
        return transactions.get(id);
    }

    @PostMapping("/{id}/retry")
    ResponseEntity<Transactions.State> retry(@PathVariable UUID id) {
        return response(transactions.asynchronous() ? transactions.scheduleRetry(id) : transactions.process(id));
    }

    private ResponseEntity<Transactions.State> submit(UUID id, long amount, String currency) {
        if (id == null || amount <= 0 || !("EUR".equals(currency) || "USD".equals(currency))) {
            throw new ResponseStatusException(BAD_REQUEST, "Expected a UUID, a positive amount and EUR or USD");
        }
        if (transactions.insert(id, amount, currency)) {
            return response(transactions.asynchronous() ? transactions.get(id) : transactions.process(id));
        }
        var existing = transactions.get(id);
        if (existing.amountMinor() != amount || !existing.currency().equals(currency)) {
            throw new ResponseStatusException(CONFLICT, "Reference already used for a different transaction");
        }
        return response(existing);
    }

    private ResponseEntity<Transactions.State> response(Transactions.State state) {
        return state.status().equals("DECIDED") ? ResponseEntity.ok(state) : ResponseEntity.accepted().body(state);
    }

    record Gateway(UUID id, long amountMinor, String currency) {}
    record Ledger(UUID reference, BigDecimal total, String ccy) {}
}

