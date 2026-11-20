package replicatedlog.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import replicatedlog.log.InMemoryLog;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplicationService {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationService.class);

    private final NodeId nodeId;
    private final InMemoryLog log;
    private final Transport transport;
    private final List<NodeId> secondaries;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public ReplicationService(NodeId nodeId, InMemoryLog log, Transport transport, List<NodeId> secondaries) {
        this.nodeId = nodeId;
        this.log = log;
        this.transport = transport;
        this.secondaries = List.copyOf(secondaries);
    }

    public int fullWriteConcern() {
        return secondaries.size() + 1;
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
                        () -> transport.replicate(nodeId, secondary, sequence, text),
                        executor
                ))
                .toList();
        acks.forEach(future -> future.whenComplete((ok, error) -> {
            if (error != null) {
                logger.warn(
                        "Replication of '{}' (seq {}) failed: {}",
                        text,
                        sequence,
                        unwrap(error).toString()
                );
            }
        }));

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
