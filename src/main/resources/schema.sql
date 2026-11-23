CREATE TABLE IF NOT EXISTS transactions (
    id uuid PRIMARY KEY,
    cents bigint NOT NULL CHECK (cents > 0),
    currency varchar(3) NOT NULL,
    status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','FAILED','DECIDED')),
    attempts integer NOT NULL DEFAULT 0,
    decision text CHECK (decision IN ('ALLOW','REVIEW')),
    CHECK ((status = 'DECIDED') = (decision IS NOT NULL))
);

