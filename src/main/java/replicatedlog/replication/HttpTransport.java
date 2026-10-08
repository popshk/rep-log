package replicatedlog.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import replicatedlog.api.JsonSupport;
import replicatedlog.api.MessageRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HttpTransport implements Transport {
    private static final Logger logger = LoggerFactory.getLogger(HttpTransport.class);

    private final Map<NodeId, String> secondaryUrls;
    private final HttpClient client;

    public HttpTransport(Map<NodeId, String> secondaryUrls, HttpClient client) {
        this.secondaryUrls = Map.copyOf(secondaryUrls);
        this.client = client;
    }

    @Override
    public void replicate(NodeId from, NodeId to, String message) {
        String url = secondaryUrls.get(to);
        if (url == null) {
            throw new IllegalStateException("Unknown secondary " + to.id());
        }
        logger.info("Replicating '{}' from {} to {} ({})", message, from.id(), to.id(), url);
        try {
            String body = JsonSupport.MAPPER.writeValueAsString(new MessageRequest(message));
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url + "/replicate"))
                    .timeout(Duration.ofSeconds(60))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "Replication to " + to.id() + " failed with HTTP " + response.statusCode()
                );
            }
            logger.info("Received ACK from {}", to.id());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Replication to " + to.id() + " interrupted", e);
        } catch (IOException e) {
            throw new UncheckedIOException("Replication to " + to.id() + " failed", e);
        }
    }

    public static HttpTransport fromUrls(List<String> urls, HttpClient client) {
        Map<NodeId, String> mapping = new LinkedHashMap<>();
        for (String url : urls) {
            mapping.put(nodeIdFromUrl(url), url.replaceAll("/+$", ""));
        }
        return new HttpTransport(mapping, client);
    }

    public static NodeId nodeIdFromUrl(String url) {
        String host = URI.create(url).getHost();
        return new NodeId(host != null ? host : url);
    }
}
