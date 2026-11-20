package replicatedlog.replication;

public class NoQuorumException extends RuntimeException {
    public NoQuorumException(String message) {
        super(message);
    }
}
