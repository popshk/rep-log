package replicatedlog.config;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public record AppConfig(
        NodeRole role,
        String nodeId,
        int port,
        List<String> secondaryUrls,
        long replicationDelayMs
) {
    public static AppConfig fromEnv() {
        NodeRole role = NodeRole.valueOf(
                Optional.ofNullable(System.getenv("NODE_ROLE")).orElse("MASTER").toUpperCase(Locale.ROOT)
        );
        String nodeId = Optional.ofNullable(System.getenv("NODE_ID")).orElse(role.name().toLowerCase(Locale.ROOT));
        int port = Optional.ofNullable(System.getenv("PORT")).map(Integer::parseInt).orElse(8080);
        List<String> secondaryUrls = Arrays.stream(
                        Optional.ofNullable(System.getenv("SECONDARIES")).orElse("").split(","))
                .map(String::trim)
                .filter(url -> !url.isEmpty())
                .toList();
        long replicationDelayMs = Optional.ofNullable(System.getenv("REPLICATION_DELAY_MS"))
                .map(Long::parseLong)
                .orElse(0L);

        return new AppConfig(role, nodeId, port, secondaryUrls, replicationDelayMs);
    }
}
