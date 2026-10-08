package replicatedlog;

import io.javalin.Javalin;
import org.junit.jupiter.api.Test;
import replicatedlog.api.EchoResponse;
import replicatedlog.api.HealthResponse;
import replicatedlog.api.JsonSupport;
import replicatedlog.api.MessagesResponse;
import replicatedlog.config.AppConfig;
import replicatedlog.config.NodeRole;
import replicatedlog.log.InMemoryLog;
import replicatedlog.replication.NodeId;
import replicatedlog.replication.ReplicationService;
import replicatedlog.replication.Transport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EchoAndApiTest {
    @Test
    void echoReturnsTheSamePayload() throws Exception {
        AppConfig config = new AppConfig(NodeRole.SECONDARY, "echo-node", 8080, List.of(), 0);
        Javalin app = App.createApp(config);
        app.start(0);
        try {
            HttpResponse<String> response = postJson(app.port(), "/echo", "{\"text\":\"hello\"}");
            assertEquals(200, response.statusCode());
            assertEquals(
                    new EchoResponse("hello"),
                    JsonSupport.MAPPER.readValue(response.body(), EchoResponse.class)
            );
        } finally {
            app.stop();
        }
    }

    @Test
    void healthReportsRoleAndNodeId() throws Exception {
        AppConfig config = new AppConfig(NodeRole.MASTER, "master", 8080, List.of(), 0);
        Javalin app = App.createApp(config);
        app.start(0);
        try {
            HttpResponse<String> response = get(app.port(), "/health");
            assertEquals(200, response.statusCode());
            assertEquals(
                    new HealthResponse("MASTER", "master"),
                    JsonSupport.MAPPER.readValue(response.body(), HealthResponse.class)
            );
        } finally {
            app.stop();
        }
    }

    @Test
    void masterAppendWaitsForTransportAcksThenGetReturnsTheLog() throws Exception {
        InMemoryLog log = new InMemoryLog();
        List<NodeId> secondaries = List.of(new NodeId("s1"), new NodeId("s2"));
        ConcurrentLinkedQueue<String> acked = new ConcurrentLinkedQueue<>();
        Transport transport = (from, to, message) -> acked.add(to.id());
        ReplicationService replication = new ReplicationService(new NodeId("master"), log, transport, secondaries);

        AppConfig config = new AppConfig(NodeRole.MASTER, "master", 8080, List.of(), 0);
        Javalin app = App.createApp(config, log, replication);
        app.start(0);
        try {
            HttpResponse<String> post = postJson(app.port(), "/messages", "{\"text\":\"m1\"}");
            assertEquals(200, post.statusCode());
            assertEquals(Set.of("s1", "s2"), Set.copyOf(acked));

            HttpResponse<String> get = get(app.port(), "/messages");
            assertEquals(
                    new MessagesResponse(List.of("m1")),
                    JsonSupport.MAPPER.readValue(get.body(), MessagesResponse.class)
            );
        } finally {
            app.stop();
        }
    }

    private static HttpResponse<String> get(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .GET()
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> postJson(int port, String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }
}
