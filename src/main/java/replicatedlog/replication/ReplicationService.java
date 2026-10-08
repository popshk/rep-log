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

public class ReplicationService {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationService.class);

    private final NodeId nodeId;
    private final InMemoryLog log;
    private final Transport transport;
    private final List<NodeId> secondaries;

    public ReplicationService(NodeId nodeId, InMemoryLog log, Transport transport, List<NodeId> secondaries) {
        this.nodeId = nodeId;
        this.log = log;
        this.transport = transport;
        this.secondaries = List.copyOf(secondaries);
    }

    public void append(String text) {
        log.append(text);
        logger.info(
                "Master {} appended '{}'. Replicating to {} secondaries in parallel",
                nodeId.id(),
                text,
                secondaries.size()
        );

        long started = System.nanoTime();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            CompletableFuture<?>[] replicas = secondaries.stream()
                    .map(secondary -> CompletableFuture.runAsync(
                            () -> transport.replicate(nodeId, secondary, text),
                            executor
                    ))
                    .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(replicas).join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new RuntimeException(cause);
        }
        logger.info(
                "Received ACKs from all secondaries for '{}' in {}",
                text,
                Duration.ofNanos(System.nanoTime() - started)
        );
    }

    public List<String> list() {
        return log.list();
    }
}
