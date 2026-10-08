package replicatedlog.node;

import replicatedlog.log.InMemoryLog;
import replicatedlog.replication.NodeId;

import java.util.List;

public class SecondaryNode {
    private final NodeId id;
    private final InMemoryLog log;

    public SecondaryNode(NodeId id) {
        this(id, new InMemoryLog());
    }

    public SecondaryNode(NodeId id, InMemoryLog log) {
        this.id = id;
        this.log = log;
    }

    public NodeId id() {
        return id;
    }

    public void append(String text) {
        log.append(text);
    }

    public List<String> listMsgs() {
        return log.list();
    }
}
