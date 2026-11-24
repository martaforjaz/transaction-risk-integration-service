# Transaction Risk Integration Service

![Two transaction formats converge into one PostgreSQL record and a risk evaluation with a retry path.](figures/workflow.svg)

A Java 21 and Spring Boot integration service for two synthetic transaction feeds. PostgreSQL stores normalized transactions and a transactional outbox. Kafka delivers evaluation requests to a worker that calls a separate HTTP risk endpoint. Docker Compose runs all four services.

## Delivery and recovery

```mermaid
flowchart LR
  A[Gateway and ledger] --> B[PostgreSQL transaction and outbox]
  B --> C[Kafka risk.evaluate]
  C --> D[Risk evaluation worker]
  D --> E[HTTP risk endpoint]
  D -->|One decision| B
  D -->|Delayed retry via outbox| B
  B -->|Attempts exhausted| F[Kafka risk.failed]
```

The API commits the intake row and its first outbox event together. The relay claims due rows with `FOR UPDATE SKIP LOCKED` and deletes a row only after Kafka acknowledges it. Kafka can deliver a message more than once. The consumer locks the transaction row and checks the expected attempt so repeated or stale events cannot create a second decision. A failed call schedules retries after one and two seconds by default. After three failed attempts, the worker writes an outbox event for `risk.failed`. The retry API can schedule a new cycle using the original transaction ID.

| Scenario | Behavior |
| --- | --- |
| Gateway sends integer minor units and ledger sends a decimal amount | Both adapters normalize to the same UUID, currency and minor-unit amount. |
| The same transaction arrives concurrently | The PostgreSQL primary key admits one record. Processing uses a row lock. |
| A UUID is reused with a different amount or currency | The API returns HTTP 409. |
| Risk service is unavailable | The worker persists the failed attempt and schedules a retry or emits a failure event. |
| Risk service becomes available | A scheduled retry can resume processing. The retry endpoint can replay an exhausted transaction. |

## Run locally

Requires Docker with Compose.

```sh
docker compose up --build -d
curl -s -X POST http://localhost:8080/transactions/gateway \
  -H 'Content-Type: application/json' \
  -d '{"id":"01aa8308-0480-4633-a99d-ea00f2c337bc","amountMinor":1250,"currency":"EUR"}'
curl -s -X POST http://localhost:8080/transactions/ledger \
  -H 'Content-Type: application/json' \
  -d '{"reference":"01aa8308-0480-4633-a99d-ea00f2c337bc","total":"12.50","ccy":"EUR"}'
```

Both responses refer to the same record. Intake may return HTTP 202 before the worker commits a decision. Use `GET /transactions/{id}` to inspect its status and confirm that its final attempt count is one.

## Diagnose and replay a failed transaction

Use a fresh UUID if the demo has already been run on the same database volume.

```sh
docker compose stop risk
curl -s -X POST http://localhost:8080/transactions/gateway \
  -H 'Content-Type: application/json' \
  -d '{"id":"6bcffb62-6758-4bdf-8f8a-4cf55f443a18","amountMinor":70000,"currency":"EUR"}'
curl -s http://localhost:8080/transactions/6bcffb62-6758-4bdf-8f8a-4cf55f443a18
docker compose logs app
```

Wait until the status endpoint reports `FAILED` with three attempts, then inspect the failure topic and replay:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic risk.failed --from-beginning --timeout-ms 5000
docker compose start risk
curl -s -X POST http://localhost:8080/transactions/6bcffb62-6758-4bdf-8f8a-4cf55f443a18/retry
curl -s http://localhost:8080/transactions/6bcffb62-6758-4bdf-8f8a-4cf55f443a18
```

The transaction eventually reaches `DECIDED` with `REVIEW` on attempt four. The original synchronous mode is available with `RISK_ASYNC=false`.

## Test

```sh
mvn verify
```

The integration suite starts PostgreSQL and Kafka with Testcontainers and exercises the HTTP API against a controlled risk endpoint. It checks concurrent intake, duplicate Kafka delivery, automatic recovery after stopping and restarting the Kafka listener, and manual replay after a failure event. GitHub Actions runs the suite and validates the Compose file.

The service guarantees one committed decision per transaction UUID. An interruption after the remote request but before the database commit can cause a subsequent request to contact the risk service again. The API forwards the transaction UUID as an idempotency key. The synthetic risk service returns a deterministic decision and has no side effects.


## Load and consumer recovery

![Backlog growth and drain during a consumer restart](benchmark/worker-recovery.png)

A local Docker Compose benchmark runs ingress and the Kafka consumer in separate containers, with a synthetic risk endpoint. The API stays up while the consumer is stopped. Each run uses fresh transaction IDs. After decisions are observed, the driver resubmits 20% of the IDs and checks in PostgreSQL that each unique transaction is decided with exactly one evaluation attempt.

| Scenario | Target submissions/s | Unique decisions | Observed p95 (s) | End-to-end decisions/s |
| --- | ---: | ---: | ---: | ---: |
| Normal | 10 | 60 | 0.982 | 9.77 |
| Normal | 30 | 120 | 0.999 | 26.40 |
| Worker stopped, then restarted | 25 | 125 | 12.158 | 10.09 |

All **305 unique transactions** completed across the three measured scenarios. **61 repeated submissions** preserved the existing decisions. In the outage scenario, all **125 requests** were accepted while the worker was stopped. The last decision was observed **6.407 seconds from the restart request**, including container startup, consumer rejoin and the delay until the next database sample.

The script polls PostgreSQL. Latency runs from the client receiving HTTP acceptance to the first sample that observes a decision. Throughput divides completed transactions by elapsed time from the first send to the final observed decision. The recovery scenario includes the worker downtime in throughput and latency. These are measurements from one local run with a mock risk endpoint. [Raw measurements](benchmark/results.json).

If unfinished transactions rise, compare Kafka consumer lag with outbox depth. Rising consumer lag with a draining outbox points to work waiting at the consumer. Check consumer availability and rebalance logs, then check the upstream risk endpoint's response latency and errors if the consumer is active.

### Reproduce

Requires Docker Compose with support for the `!override` tag and Python 3.11+. The stack uses port 18080 and a separate Compose project.

```sh
docker compose -p risk-benchmark -f compose.yaml -f benchmark/compose.yaml up -d --build
# Wait for the application and worker to start.
python benchmark/load.py
python -m pip install matplotlib
python benchmark/plot.py
docker compose -p risk-benchmark -f compose.yaml -f benchmark/compose.yaml stop
```
