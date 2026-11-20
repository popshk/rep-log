# Replicated Log (Iteration 2)

Java 21 / Javalin in-memory replicated log: one Master, N Secondaries.

Iteration 2 adds tunable semi-synchronous replication. A client `POST` includes write concern `w`:

- `w = 1` — return after the master has stored the message
- `w = 2` — return after the master and one secondary ACK
- `w = 3` — return after the master and two secondaries ACK

Replication to every secondary still runs. A smaller `w` does not cancel it, and it does not block another client's append. Secondaries apply messages by master sequence, so a reordered delivery stays hidden until the gap is filled.

If `w` is omitted, the master waits for every secondary (iteration 1 behaviour).

## Run with Docker Compose

```bash
docker compose up --build
```

- Master: http://localhost:8080
- Secondary 1: http://localhost:8081 (no delay)
- Secondary 2: http://localhost:8082 (5s artificial delay)

## Curl

Echo (Iteration 0):

```bash
curl -s -X POST http://localhost:8080/echo \
  -H 'Content-Type: application/json' \
  -d '{"text":"hello"}'
```

Append with write concern. `w=1` and `w=2` return immediately, because secondary 1 is fast. `w=3` waits about 5 seconds for secondary 2.

```bash
curl -s -X POST http://localhost:8080/messages \
  -H 'Content-Type: application/json' \
  -d '{"text":"m1","w":1}'

curl -s -X POST http://localhost:8080/messages \
  -H 'Content-Type: application/json' \
  -d '{"text":"m2","w":2}'

curl -s -X POST http://localhost:8080/messages \
  -H 'Content-Type: application/json' \
  -d '{"text":"m3","w":3}'
```

Read logs. Right after a small `w`, secondary 2 can be behind. It catches up in order.

```bash
curl -s http://localhost:8080/messages
curl -s http://localhost:8081/messages
curl -s http://localhost:8082/messages
```

## Tests (harness)

```bash
./gradlew test
```

`BlockingReplicationTest` is the iteration 1 case: append without `w` waits for every ACK, and the wait is the max delay, not the sum.

`WriteConcernTest` covers iteration 2:

- `w=1` / `w=2` return while secondary 2 is still delayed, and a parallel `w=1` is not blocked by an in-flight `w=3`
- the delayed secondary's visible log is a prefix of the master's log, then matches it
- `reorderMessages` delivers two messages backwards; the secondary still lists them in commit order

## Local run without Docker

```bash
NODE_ROLE=SECONDARY NODE_ID=secondary-1 PORT=8081 ./gradlew run
NODE_ROLE=SECONDARY NODE_ID=secondary-2 PORT=8082 REPLICATION_DELAY_MS=5000 ./gradlew run
NODE_ROLE=MASTER NODE_ID=master PORT=8080 \
  SECONDARIES=http://localhost:8081,http://localhost:8082 ./gradlew run
```
