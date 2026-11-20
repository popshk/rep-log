package replicatedlog.replication;

public interface Transport {
    void replicate(NodeId from, NodeId to, long sequence, String message);
}
