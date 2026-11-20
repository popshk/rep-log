package replicatedlog.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import replicatedlog.log.InMemoryLog;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplicationService {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationService.class);

    private final NodeId nodeId;
    private final InMemoryLog log;
    private final Transport transport;
    private final List<NodeId> secondaries;
    private final Duration attemptTimeout;
    private final Duration retryInterval;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeats;
    private final ConcurrentHashMap<NodeId, NodeHealth> health = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<NodeId, Integer> consecutiveFailures = new ConcurrentHashMap<>();

    public ReplicationService(NodeId nodeId, InMemoryLog log, Transport transport, List<NodeId> secondaries) {
        this(nodeId, log, transport, secondaries, Duration.ofSeconds(60), Duration.ofSeconds(1), Duration.ofSeconds(1));
    }

    public ReplicationService(
            NodeId nodeId,
            InMemoryLog log,
            Transport transport,
            List<NodeId> secondaries,
            Duration attemptTimeout,
            Duration retryInterval,
            Duration heartbeatInterval
    ) {
        this.nodeId = nodeId;
        this.log = log;
        this.transport = transport;
        this.secondaries = List.copyOf(secondaries);
        this.attemptTimeout = attemptTimeout;
        this.retryInterval = retryInterval;
        this.secondaries.forEach(secondary -> health.put(secondary, NodeHealth.HEALTHY));
        this.heartbeats = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "heartbeat-" + nodeId.id());
            thread.setDaemon(true);
            return thread;
        });
        if (!this.secondaries.isEmpty() && heartbeatInterval != null && !heartbeatInterval.isZero() && !heartbeatInterval.isNegative()) {
            long millis = Math.max(1, heartbeatInterval.toMillis());
            heartbeats.scheduleWithFixedDelay(this::probeSecondaries, millis, millis, TimeUnit.MILLISECONDS);
        }
    }

    public int fullWriteConcern() {
        return secondaries.size() + 1;
    }

    public boolean hasQuorum() {
        int required = (secondaries.size() + 1) / 2 + 1;
        int alive = 1;
        for (NodeId secondary : secondaries) {
            if (health.getOrDefault(secondary, NodeHealth.HEALTHY) != NodeHealth.UNHEALTHY) {
                alive++;
            }
        }
        return alive >= required;
    }

    public Map<String, String> secondaryHealth() {
        Map<String, String> statuses = new LinkedHashMap<>();
        for (NodeId secondary : secondaries) {
            statuses.put(secondary.id(), health.getOrDefault(secondary, NodeHealth.HEALTHY).name());
        }
        return Map.copyOf(statuses);
    }

    public void append(String text) {
        append(text, fullWriteConcern());
    }

    public void append(String text, int writeConcern) {
        int maxConcern = fullWriteConcern();
        if (writeConcern < 1 || writeConcern > maxConcern) {
            throw new IllegalArgumentException(
                    "write concern w must be between 1 and " + maxConcern
            );
        }
        if (!hasQuorum()) {
            throw new NoQuorumException("master is read-only: no quorum");
        }

        long sequence = log.append(text);
        logger.info(
                "Master {} appended '{}' as seq {} with w={}. Replicating to {} secondaries",
                nodeId.id(),
                text,
                sequence,
                writeConcern,
                secondaries.size()
        );

        long started = System.nanoTime();
        List<CompletableFuture<Void>> acks = secondaries.stream()
                .map(secondary -> CompletableFuture.runAsync(
                        () -> replicateUntilAck(secondary, sequence, text),
                        executor
                ))
                .toList();
        awaitAcks(acks, writeConcern - 1);
        logger.info(
                "Write concern w={} satisfied for '{}' (seq {}) in {}",
                writeConcern,
                text,
                sequence,
                Duration.ofNanos(System.nanoTime() - started)
        );
    }

    public List<String> list() {
        return log.list();
    }

    private void replicateUntilAck(NodeId secondary, long sequence, String text) {
        CompletableFuture<Void> acked = new CompletableFuture<>();
        while (!acked.isDone()) {
            CompletableFuture<Boolean> outcome = new CompletableFuture<>();
            CompletableFuture.runAsync(() -> transport.replicate(nodeId, secondary, sequence, text), executor)
                    .whenComplete((ok, error) -> {
                        if (error == null) {
                            acked.complete(null);
                            outcome.complete(true);
                        } else {
                            logger.warn(
                                    "Replication of '{}' (seq {}) to {} failed: {}",
                                    text,
                                    sequence,
                                    secondary.id(),
                                    unwrap(error).toString()
                            );
                            outcome.complete(false);
                        }
                    });
            try {
                if (outcome.get(Math.max(1, attemptTimeout.toMillis()), TimeUnit.MILLISECONDS) || acked.isDone()) {
                    return;
                }
            } catch (TimeoutException e) {
                if (acked.isDone()) {
                    return;
                }
                logger.info(
                        "No ACK for '{}' (seq {}) from {} within {}, retrying",
                        text,
                        sequence,
                        secondary.id(),
                        attemptTimeout
                );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Replication interrupted", e);
            } catch (ExecutionException e) {
                throw new RuntimeException(unwrap(e));
            }
            if (acked.isDone()) {
                return;
            }
            sleep(retryDelay(secondary));
        }
    }

    private Duration retryDelay(NodeId secondary) {
        if (health.getOrDefault(secondary, NodeHealth.HEALTHY) == NodeHealth.UNHEALTHY) {
            return retryInterval.multipliedBy(2);
        }
        return retryInterval;
    }

    private static void sleep(Duration delay) {
        if (delay == null || delay.isZero() || delay.isNegative()) {
            return;
        }
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Replication interrupted", e);
        }
    }

    private void probeSecondaries() {
        for (NodeId secondary : secondaries) {
            boolean alive;
            try {
                alive = transport.probe(nodeId, secondary);
            } catch (RuntimeException e) {
                logger.warn("Heartbeat to {} failed: {}", secondary.id(), e.toString());
                alive = false;
            }
            NodeHealth previous = health.getOrDefault(secondary, NodeHealth.HEALTHY);
            NodeHealth next;
            if (alive) {
                consecutiveFailures.put(secondary, 0);
                next = NodeHealth.HEALTHY;
            } else {
                int failures = consecutiveFailures.merge(secondary, 1, Integer::sum);
                next = failures == 1 ? NodeHealth.SUSPECTED : NodeHealth.UNHEALTHY;
            }
            health.put(secondary, next);
            if (previous != next) {
                logger.info("Secondary {} health {} -> {}", secondary.id(), previous, next);
            }
        }
    }

    private void awaitAcks(List<CompletableFuture<Void>> futures, int needed) {
        if (needed <= 0) {
            return;
        }
        CompletableFuture<Void> gate = new CompletableFuture<>();
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger finished = new AtomicInteger();
        int total = futures.size();
        for (CompletableFuture<Void> future : futures) {
            future.whenComplete((ignored, error) -> {
                if (error == null && successes.incrementAndGet() >= needed) {
                    gate.complete(null);
                }
                if (finished.incrementAndGet() == total && successes.get() < needed) {
                    Throwable failure = error == null
                            ? new IllegalStateException("Received " + successes.get() + " ACKs, need " + needed)
                            : unwrap(error);
                    gate.completeExceptionally(failure);
                }
            });
        }
        try {
            gate.join();
        } catch (CompletionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw new RuntimeException(cause);
        }
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
