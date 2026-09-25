# Replicated Log (Iteration 1)

Java 21 / Javalin in-memory replicated log: one Master, N Secondaries, blocking replication (wait for every ACK). Parallel fan-out, perfect-link assumptions only.

## Run with Docker Compose

```bash
docker compose up --build
```

- Master: http://localhost:8080
- Secondary 1: http://localhost:8081
- Secondary 2: http://localhost:8082

## Curl

Echo (Iteration 0):

```bash
curl -s -X POST http://localhost:8080/echo \
  -H 'Content-Type: application/json' \
  -d '{"text":"hello"}'
```

Append on master (blocks until all secondaries ACK):

```bash
curl -s -X POST http://localhost:8080/messages \
  -H 'Content-Type: application/json' \
  -d '{"text":"m1"}'
```

Read logs:

```bash
curl -s http://localhost:8080/messages
curl -s http://localhost:8081/messages
curl -s http://localhost:8082/messages
```

To see blocking replication, set `REPLICATION_DELAY_MS: "5000"` on `secondary-1` in `docker-compose.yml` and POST again: the client waits until the delayed ACK arrives.

## Tests (harness)

```bash
./gradlew test
```

`BlockingReplicationTest` follows the assignment harness: 5s delay on S1, 6s delay on S2, then no delay. Append of `m2` waits ~6s (`max`), not 11s (`sum`).

## Local run without Docker

```bash
NODE_ROLE=SECONDARY NODE_ID=secondary-1 PORT=8081 ./gradlew run
NODE_ROLE=SECONDARY NODE_ID=secondary-2 PORT=8082 ./gradlew run
NODE_ROLE=MASTER NODE_ID=master PORT=8080 \
  SECONDARIES=http://localhost:8081,http://localhost:8082 ./gradlew run
```
