package replicatedlog.replication;

import replicatedlog.node.SecondaryNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class MockedTransport implements Transport {
    private final ConcurrentHashMap<Route, Duration> delays = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SecondaryNode> secondaries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Route, Reorder> reorders = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<Long>> deliveredSequences = new ConcurrentHashMap<>();
    private final Set<Route> dropped = ConcurrentHashMap.newKeySet();
    private final Set<String> unreachable = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<Route, AtomicInteger> attempts = new ConcurrentHashMap<>();

    public void register(SecondaryNode node) {
        secondaries.put(node.id().id(), node);
    }

    public void setDelay(NodeId from, NodeId to, Duration delay) {
        delays.put(new Route(from.id(), to.id()), delay);
    }

    public void removeDelay(NodeId from, NodeId to) {
        delays.remove(new Route(from.id(), to.id()));
    }

    public void dropMessage(NodeId from, NodeId to) {
        dropped.add(new Route(from.id(), to.id()));
    }

    public void removeDrop(NodeId from, NodeId to) {
        dropped.remove(new Route(from.id(), to.id()));
    }

    public void setUnreachable(NodeId to) {
        unreachable.add(to.id());
    }

    public void setReachable(NodeId to) {
        unreachable.remove(to.id());
    }

    public int attemptCount(NodeId from, NodeId to) {
        AtomicInteger count = attempts.get(new Route(from.id(), to.id()));
        return count == null ? 0 : count.get();
    }

    /**
     * Holds the next {@code messageCount} deliveries on this route, then delivers them
     * from the highest sequence to the lowest. Each {@code replicate} call blocks until
     * its own message has been delivered, so the caller must have all {@code messageCount}
     * sends in flight together.
     */
    public void reorderMessages(NodeId from, NodeId to, int messageCount) {
        if (messageCount < 2) {
            throw new IllegalArgumentException("reorder count must be at least 2");
        }
        reorders.put(new Route(from.id(), to.id()), new Reorder(messageCount));
    }

    public int bufferedCount(NodeId from, NodeId to) {
        Reorder reorder = reorders.get(new Route(from.id(), to.id()));
        if (reorder == null) {
            return 0;
        }
        synchronized (reorder) {
            return reorder.buffered.size();
        }
    }

    public List<Long> deliveredSequences(NodeId to) {
        return List.copyOf(deliveredSequences.getOrDefault(to.id(), List.of()));
    }

    @Override
    public boolean probe(NodeId from, NodeId to) {
        return !unreachable.contains(to.id());
    }

    @Override
    public void replicate(NodeId from, NodeId to, long sequence, String message) {
        Route route = new Route(from.id(), to.id());
        attempts.computeIfAbsent(route, ignored -> new AtomicInteger()).incrementAndGet();
        if (dropped.contains(route) || unreachable.contains(to.id())) {
            throw new IllegalStateException("message to " + to.id() + " was dropped");
        }
        Pending held = enqueueIfReordering(from, to, sequence, message);
        if (held != null) {
            await(held);
            return;
        }
        awaitDelay(from, to);
        deliver(to, sequence, message);
    }

    private Pending enqueueIfReordering(NodeId from, NodeId to, long sequence, String message) {
        Route route = new Route(from.id(), to.id());
        Reorder reorder = reorders.get(route);
        if (reorder == null) {
            return null;
        }
        synchronized (reorder) {
            if (reorders.get(route) != reorder) {
                return null;
            }
            Pending pending = new Pending(sequence, message);
            reorder.buffered.add(pending);
            if (reorder.buffered.size() >= reorder.count) {
                List<Pending> batch = new ArrayList<>(reorder.buffered);
                reorder.buffered.clear();
                reorders.remove(route, reorder);
                batch.sort(Comparator.comparingLong((Pending item) -> item.sequence).reversed());
                try {
                    for (Pending item : batch) {
                        deliver(to, item.sequence, item.message);
                        item.done.complete(null);
                    }
                } catch (RuntimeException e) {
                    for (Pending item : batch) {
                        item.done.completeExceptionally(e);
                    }
                    throw e;
                }
            }
            return pending;
        }
    }

    private static void await(Pending pending) {
        try {
            pending.done.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new RuntimeException(cause);
        }
    }

    private void awaitDelay(NodeId from, NodeId to) {
        Duration delay = delays.get(new Route(from.id(), to.id()));
        if (delay == null || delay.isZero() || delay.isNegative()) {
            return;
        }
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void deliver(NodeId to, long sequence, String message) {
        SecondaryNode secondary = secondaries.get(to.id());
        if (secondary == null) {
            throw new IllegalStateException("Secondary " + to.id() + " is not registered in MockedTransport");
        }
        deliveredSequences.computeIfAbsent(to.id(), ignored -> new CopyOnWriteArrayList<>()).add(sequence);
        secondary.append(sequence, message);
    }

    private record Route(String from, String to) {
    }

    private static final class Reorder {
        private final int count;
        private final List<Pending> buffered = new ArrayList<>();

        private Reorder(int count) {
            this.count = count;
        }
    }

    private static final class Pending {
        private final long sequence;
        private final String message;
        private final CompletableFuture<Void> done = new CompletableFuture<>();

        private Pending(long sequence, String message) {
            this.sequence = sequence;
            this.message = message;
        }
    }
}
