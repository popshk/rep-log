package replicatedlog.node;

import replicatedlog.log.InMemoryLog;
import replicatedlog.replication.NodeId;
import replicatedlog.replication.ReplicationService;
import replicatedlog.replication.Transport;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class MasterNode {
    private final NodeId id;
    private final InMemoryLog log;
    private final ReplicationService replication;

    public MasterNode(NodeId id, Transport transport, List<NodeId> secondaries) {
        this(id, transport, secondaries, new InMemoryLog());
    }

    public MasterNode(NodeId id, Transport transport, List<NodeId> secondaries, InMemoryLog log) {
        this(id, transport, secondaries, log, Duration.ofSeconds(60), Duration.ofSeconds(1), Duration.ofSeconds(1));
    }

    public MasterNode(
            NodeId id,
            Transport transport,
            List<NodeId> secondaries,
            Duration attemptTimeout,
            Duration retryInterval,
            Duration heartbeatInterval
    ) {
        this(id, transport, secondaries, new InMemoryLog(), attemptTimeout, retryInterval, heartbeatInterval);
    }

    private MasterNode(
            NodeId id,
            Transport transport,
            List<NodeId> secondaries,
            InMemoryLog log,
            Duration attemptTimeout,
            Duration retryInterval,
            Duration heartbeatInterval
    ) {
        this.id = id;
        this.log = log;
        this.replication = new ReplicationService(
                id,
                log,
                transport,
                secondaries,
                attemptTimeout,
                retryInterval,
                heartbeatInterval
        );
    }

    public NodeId id() {
        return id;
    }

    public void appendMsg(String text) {
        replication.append(text);
    }

    public void appendMsg(String text, int writeConcern) {
        replication.append(text, writeConcern);
    }

    public List<String> listMsgs() {
        return log.list();
    }

    public Map<String, String> secondaryHealth() {
        return replication.secondaryHealth();
    }
}
