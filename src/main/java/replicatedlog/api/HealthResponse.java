package replicatedlog.api;

public record HealthResponse(String status, String role, String nodeId) {
    public HealthResponse(String role, String nodeId) {
        this("UP", role, nodeId);
    }
}
