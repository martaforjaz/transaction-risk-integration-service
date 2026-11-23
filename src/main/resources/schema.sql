CREATE TABLE IF NOT EXISTS transactions (
    id uuid PRIMARY KEY,
    cents bigint NOT NULL CHECK (cents > 0),
    currency varchar(3) NOT NULL,
    status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','FAILED','DECIDED')),
    attempts integer NOT NULL DEFAULT 0,
    decision text CHECK (decision IN ('ALLOW','REVIEW')),
    CHECK ((status = 'DECIDED') = (decision IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS risk_outbox (
    seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id uuid NOT NULL REFERENCES transactions(id),
    topic text NOT NULL,
    attempt integer NOT NULL,
    max_attempt integer NOT NULL,
    available_at timestamptz NOT NULL,
    UNIQUE (transaction_id, topic, attempt)
);
CREATE INDEX IF NOT EXISTS risk_outbox_ready ON risk_outbox (available_at, seq);

