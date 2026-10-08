package replicatedlog.replication;

import replicatedlog.node.SecondaryNode;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

public class MockedTransport implements Transport {
    private final ConcurrentHashMap<Route, Duration> delays = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SecondaryNode> secondaries = new ConcurrentHashMap<>();

    public void register(SecondaryNode node) {
        secondaries.put(node.id().id(), node);
    }

    public void setDelay(NodeId from, NodeId to, Duration delay) {
        delays.put(new Route(from.id(), to.id()), delay);
    }

    public void removeDelay(NodeId from, NodeId to) {
        delays.remove(new Route(from.id(), to.id()));
    }

    @Override
    public void replicate(NodeId from, NodeId to, String message) {
        Duration delay = delays.get(new Route(from.id(), to.id()));
        if (delay != null) {
            try {
                Thread.sleep(delay.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        SecondaryNode secondary = secondaries.get(to.id());
        if (secondary == null) {
            throw new IllegalStateException("Secondary " + to.id() + " is not registered in MockedTransport");
        }
        secondary.append(message);
    }

    private record Route(String from, String to) {
    }
}
