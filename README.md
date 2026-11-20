# Replicated Log (Iteration 3)

Java 21 / Javalin in-memory replicated log: one Master, N Secondaries.

A client `POST` includes write concern `w` (counting the master):

- `w = 1` — return after the master has stored the message
- `w = 2` — return after the master and one secondary ACK
- `w = 3` — return after the master and two secondaries ACK

Replication continues in the background for the remaining secondaries. One blocked append does not block another client.

If a secondary is down, slow, or the request fails, the master retries until that secondary ACKs. Retries are unlimited. A slow ACK can time out and be sent again; the secondary stores each sequence once and does not show a later message while an earlier one is missing. After the secondary comes back, it catches up in order.

The master probes `GET /health` on each secondary. Status moves `HEALTHY → SUSPECTED → UNHEALTHY`. `GET /health` on the master reports those statuses. If the alive set loses majority, the master becomes read-only and `POST /messages` returns 503.

If `w` is omitted, the master waits for every secondary.

## Run with Docker Compose

```bash
docker compose up --build
```

- Master: http://localhost:8080
- Secondary 1: http://localhost:8081 (no delay)
- Secondary 2: http://localhost:8082 (5s artificial delay on replication)

## Curl

```bash
curl -s -X POST http://localhost:8080/messages \
  -H 'Content-Type: application/json' \
  -d '{"text":"m1","w":1}'

curl -s -X POST http://localhost:8080/messages \
  -H 'Content-Type: application/json' \
  -d '{"text":"m2","w":2}'

curl -s http://localhost:8080/health
curl -s http://localhost:8080/messages
curl -s http://localhost:8081/messages
curl -s http://localhost:8082/messages
```

`w=3` waits until both secondaries ACK, so it takes about 5 seconds while secondary 2 is delayed. Stop `secondary-2` and send `w=1`: the master returns immediately and keeps retrying. Start `secondary-2` again and its log fills in order. With both secondaries stopped, `GET /health` becomes `READ_ONLY` and append returns 503.

Tunables (milliseconds): `REPLICATION_TIMEOUT_MS` (default 60000), `RETRY_INTERVAL_MS` (default 1000), `HEARTBEAT_INTERVAL_MS` (default 1000). An unhealthy secondary is retried at twice the interval.

## Tests

```bash
./gradlew test
```

- `BlockingReplicationTest` — iteration 1, wait for every ACK, parallel fan-out
- `WriteConcernTest` — iteration 2, partial `w` and reordered delivery
- `RetryAndQuorumTest` — retry, dedup, total order across a gap, catch-up when a secondary joins, heartbeat, quorum

## Local run without Docker

```bash
NODE_ROLE=SECONDARY NODE_ID=secondary-1 PORT=8081 ./gradlew run
NODE_ROLE=SECONDARY NODE_ID=secondary-2 PORT=8082 REPLICATION_DELAY_MS=5000 ./gradlew run
NODE_ROLE=MASTER NODE_ID=master PORT=8080 \
  SECONDARIES=http://localhost:8081,http://localhost:8082 ./gradlew run
```
