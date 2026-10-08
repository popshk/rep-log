package replicatedlog.node;

import replicatedlog.log.InMemoryLog;
import replicatedlog.replication.NodeId;
import replicatedlog.replication.ReplicationService;
import replicatedlog.replication.Transport;

import java.util.List;

public class MasterNode {
    private final NodeId id;
    private final InMemoryLog log;
    private final ReplicationService replication;

    public MasterNode(NodeId id, Transport transport, List<NodeId> secondaries) {
        this(id, transport, secondaries, new InMemoryLog());
    }

    public MasterNode(NodeId id, Transport transport, List<NodeId> secondaries, InMemoryLog log) {
        this.id = id;
        this.log = log;
        this.replication = new ReplicationService(id, log, transport, secondaries);
    }

    public NodeId id() {
        return id;
    }

    public void appendMsg(String text) {
        replication.append(text);
    }

    public List<String> listMsgs() {
        return log.list();
    }
}
