package replicatedlog.api;

import java.util.Map;

public record HealthResponse(String status, String role, String nodeId, Map<String, String> secondaries) {
    public HealthResponse(String role, String nodeId) {
        this("UP", role, nodeId, Map.of());
    }
}
