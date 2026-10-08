package replicatedlog.api;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class EchoClient implements AutoCloseable {
    private final String baseUrl;
    private final HttpClient client;
    private final boolean ownsClient;

    public EchoClient(String baseUrl) {
        this(baseUrl, HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(), true);
    }

    public EchoClient(String baseUrl, HttpClient client) {
        this(baseUrl, client, false);
    }

    private EchoClient(String baseUrl, HttpClient client, boolean ownsClient) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.client = client;
        this.ownsClient = ownsClient;
    }

    public String echo(String text) {
        try {
            String body = JsonSupport.MAPPER.writeValueAsString(new EchoRequest(text));
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/echo"))
                    .timeout(Duration.ofSeconds(60))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("Echo failed with HTTP " + response.statusCode());
            }
            return JsonSupport.MAPPER.readValue(response.body(), EchoResponse.class).text();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Echo request interrupted", e);
        } catch (IOException e) {
            throw new UncheckedIOException("Echo request failed", e);
        }
    }

    @Override
    public void close() {
        if (ownsClient) {
            client.close();
        }
    }
}
