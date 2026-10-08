package replicatedlog.api;

public record AckResponse(boolean ack) {
    public AckResponse() {
        this(true);
    }
}
