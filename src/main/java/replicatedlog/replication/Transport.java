package replicatedlog.replication;

public interface Transport {
    void replicate(NodeId from, NodeId to, long sequence, String message);

    default boolean probe(NodeId from, NodeId to) {
        return true;
    }
}
