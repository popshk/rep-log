package replicatedlog;

import io.javalin.Javalin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import replicatedlog.api.HealthResponse;
import replicatedlog.api.JsonSupport;
import replicatedlog.api.MessagesResponse;
import replicatedlog.config.AppConfig;
import replicatedlog.config.NodeRole;
import replicatedlog.log.InMemoryLog;
import replicatedlog.node.MasterNode;
import replicatedlog.node.SecondaryNode;
import replicatedlog.replication.MockedTransport;
import replicatedlog.replication.NoQuorumException;
import replicatedlog.replication.NodeId;
import replicatedlog.replication.ReplicationService;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RetryAndQuorumTest {
    private final NodeId masterId = new NodeId("master");
    private final NodeId secondary1Id = new NodeId("secondary-1");
    private final NodeId secondary2Id = new NodeId("secondary-2");

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void droppedMessageIsRetriedUntilTheLinkRecovers() throws Exception {
        MockedTransport transport = new MockedTransport();
        SecondaryNode secondary = new SecondaryNode(secondary1Id);
        transport.register(secondary);
        MasterNode master = master(transport, List.of(secondary1Id), Duration.ofMillis(200), Duration.ofMillis(80));

        transport.dropMessage(masterId, secondary1Id);
        try {
            master.appendMsg("m1", 1);
            awaitTrue(() -> transport.attemptCount(masterId, secondary1Id) >= 3, Duration.ofSeconds(2));
            assertEquals(List.of(), secondary.listMsgs());
            transport.removeDrop(masterId, secondary1Id);
            awaitEquals(List.of("m1"), secondary::listMsgs, Duration.ofSeconds(2));
        } finally {
            transport.removeDrop(masterId, secondary1Id);
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void delayedAckIsRetriedAndDeduplicated() throws Exception {
        MockedTransport transport = new MockedTransport();
        SecondaryNode secondary = new SecondaryNode(secondary2Id);
        transport.register(new SecondaryNode(secondary1Id));
        transport.register(secondary);
        MasterNode master = master(transport, List.of(secondary1Id, secondary2Id), Duration.ofMillis(150), Duration.ofMillis(80));

        transport.setDelay(masterId, secondary2Id, Duration.ofMillis(700));
        master.appendMsg("m1", 1);
        master.appendMsg("m2", 1);
        awaitTrue(() -> transport.attemptCount(masterId, secondary2Id) >= 4, Duration.ofSeconds(2));
        transport.removeDelay(masterId, secondary2Id);
        awaitEquals(List.of("m1", "m2"), secondary::listMsgs, Duration.ofSeconds(3));
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void gapStaysHiddenUntilTheRetriedMessageArrives() throws Exception {
        MockedTransport transport = new MockedTransport();
        SecondaryNode secondary = new SecondaryNode(secondary1Id);
        transport.register(secondary);
        transport.register(new SecondaryNode(secondary2Id));
        MasterNode master = master(
                transport,
                List.of(secondary1Id, secondary2Id),
                Duration.ofMillis(200),
                Duration.ofMillis(1_200)
        );

        master.appendMsg("m1", 1);
        master.appendMsg("m2", 1);
        awaitEquals(List.of("m1", "m2"), secondary::listMsgs, Duration.ofSeconds(2));

        transport.dropMessage(masterId, secondary1Id);
        try {
            int attemptsBefore = transport.attemptCount(masterId, secondary1Id);
            master.appendMsg("m3", 1);
            awaitTrue(() -> transport.attemptCount(masterId, secondary1Id) > attemptsBefore, Duration.ofSeconds(2));
            transport.removeDrop(masterId, secondary1Id);
            master.appendMsg("m4", 1);
            awaitTrue(() -> transport.deliveredSequences(secondary1Id).contains(4L), Duration.ofSeconds(2));
            assertEquals(List.of("m1", "m2"), secondary.listMsgs());
            awaitEquals(List.of("m1", "m2", "m3", "m4"), secondary::listMsgs, Duration.ofSeconds(4));
        } finally {
            transport.removeDrop(masterId, secondary1Id);
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void writeConcernThreeWaitsUntilTheMissingSecondaryJoins() throws Exception {
        MockedTransport transport = new MockedTransport();
        SecondaryNode secondary1 = new SecondaryNode(secondary1Id);
        SecondaryNode secondary2 = new SecondaryNode(secondary2Id);
        transport.register(secondary1);
        MasterNode master = master(transport, List.of(secondary1Id, secondary2Id), Duration.ofMillis(200), Duration.ofMillis(80));

        master.appendMsg("m1", 1);
        long m2Ms = measureMillis(() -> master.appendMsg("m2", 2));
        assertTrue(m2Ms < 1_000, "w=2 should return after secondary-1, was " + m2Ms + "ms");

        CompletableFuture<Void> m3 = CompletableFuture.runAsync(() -> master.appendMsg("m3", 3));
        awaitTrue(() -> master.listMsgs().contains("m3"), Duration.ofSeconds(2));
        assertFalse(m3.isDone());

        long m4Ms = measureMillis(() -> master.appendMsg("m4", 1));
        assertTrue(m4Ms < 1_000, "w=1 must not wait for the blocked w=3 append, was " + m4Ms + "ms");
        assertFalse(m3.isDone());
        assertEquals(List.of("m1", "m2", "m3", "m4"), master.listMsgs());

        transport.register(secondary2);
        m3.get(5, TimeUnit.SECONDS);
        List<String> expected = List.of("m1", "m2", "m3", "m4");
        awaitEquals(expected, secondary1::listMsgs, Duration.ofSeconds(2));
        awaitEquals(expected, secondary2::listMsgs, Duration.ofSeconds(3));
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void heartbeatWalksHealthySuspectedUnhealthy() throws Exception {
        MockedTransport transport = new MockedTransport();
        transport.register(new SecondaryNode(secondary1Id));
        MasterNode master = master(transport, List.of(secondary1Id), Duration.ofSeconds(2), Duration.ofMillis(50), Duration.ofMillis(200));

        assertEquals("HEALTHY", master.secondaryHealth().get(secondary1Id.id()));
        transport.setUnreachable(secondary1Id);
        try {
            Set<String> seen = new HashSet<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            while (System.nanoTime() < deadline && !seen.contains("UNHEALTHY")) {
                seen.add(master.secondaryHealth().get(secondary1Id.id()));
                Thread.sleep(10);
            }
            assertTrue(seen.contains("SUSPECTED"), "expected SUSPECTED among " + seen);
            assertTrue(seen.contains("UNHEALTHY"), "expected UNHEALTHY among " + seen);

            transport.setReachable(secondary1Id);
            awaitTrue(() -> "HEALTHY".equals(master.secondaryHealth().get(secondary1Id.id())), Duration.ofSeconds(2));
        } finally {
            transport.setReachable(secondary1Id);
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void masterRejectsAppendsWithoutQuorum() throws Exception {
        MockedTransport transport = new MockedTransport();
        transport.register(new SecondaryNode(secondary1Id));
        transport.register(new SecondaryNode(secondary2Id));
        InMemoryLog log = new InMemoryLog();
        ReplicationService replication = new ReplicationService(
                masterId,
                log,
                transport,
                List.of(secondary1Id, secondary2Id),
                Duration.ofSeconds(2),
                Duration.ofMillis(50),
                Duration.ofMillis(40)
        );
        AppConfig config = new AppConfig(NodeRole.MASTER, "master", 0, List.of(), 0);
        Javalin app = App.createApp(config, log, replication);
        app.start(0);
        try {
            transport.setUnreachable(secondary1Id);
            transport.setUnreachable(secondary2Id);
            awaitTrue(() -> {
                HealthResponse health = health(app.port());
                return "READ_ONLY".equals(health.status())
                        && "UNHEALTHY".equals(health.secondaries().get(secondary1Id.id()))
                        && "UNHEALTHY".equals(health.secondaries().get(secondary2Id.id()));
            }, Duration.ofSeconds(2));

            HttpResponse<String> rejected = postJson(app.port(), "{\"text\":\"blocked\",\"w\":1}");
            assertEquals(503, rejected.statusCode());
            assertTrue(rejected.body().contains("no quorum"));
            assertEquals(List.of(), messages(app.port()));
            assertThrows(NoQuorumException.class, () -> replication.append("blocked", 1));

            transport.setReachable(secondary1Id);
            awaitTrue(() -> "UP".equals(health(app.port()).status()), Duration.ofSeconds(2));
            assertEquals(200, postJson(app.port(), "{\"text\":\"m1\",\"w\":1}").statusCode());
            awaitEquals(List.of("m1"), () -> messagesUnchecked(app.port()), Duration.ofSeconds(2));
        } finally {
            transport.setReachable(secondary1Id);
            transport.setReachable(secondary2Id);
            app.stop();
        }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void httpReplicaCatchesUpAfterItJoins() throws Exception {
        Javalin secondary1 = startSecondary("secondary-1");
        int secondary2Port;
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            secondary2Port = socket.getLocalPort();
        }
        AppConfig secondary2Config = new AppConfig(NodeRole.SECONDARY, "secondary-2", 0, List.of(), 0);
        Javalin secondary2 = App.createApp(secondary2Config, new InMemoryLog(), (HttpClient) null);
        AppConfig masterConfig = new AppConfig(
                NodeRole.MASTER,
                "master",
                0,
                List.of(
                        "http://127.0.0.1:" + secondary1.port(),
                        "http://127.0.0.1:" + secondary2Port
                ),
                0,
                Duration.ofMillis(400),
                Duration.ofMillis(80),
                Duration.ofMillis(200)
        );
        Javalin master = App.createApp(masterConfig, new InMemoryLog(), App.createHttpClient());
        master.start(0);
        try {
            long m1Ms = measureMillis(() -> assertEquals(200, postJson(master.port(), "{\"text\":\"m1\",\"w\":1}").statusCode()));
            long m2Ms = measureMillis(() -> assertEquals(200, postJson(master.port(), "{\"text\":\"m2\",\"w\":2}").statusCode()));
            assertTrue(m1Ms < 1_000, "w=1 was " + m1Ms + "ms");
            assertTrue(m2Ms < 1_000, "w=2 was " + m2Ms + "ms");

            CompletableFuture<HttpResponse<String>> m3 = CompletableFuture.supplyAsync(
                    () -> postJson(master.port(), "{\"text\":\"m3\",\"w\":3}")
            );
            awaitTrue(() -> messagesUnchecked(master.port()).contains("m3") && !m3.isDone(), Duration.ofSeconds(2));

            long m4Ms = measureMillis(() -> assertEquals(
                    200,
                    postJson(master.port(), "{\"text\":\"m4\",\"w\":1}").statusCode()
            ));
            assertTrue(m4Ms < 1_000, "w=1 must not wait for the blocked w=3 append, was " + m4Ms + "ms");
            assertFalse(m3.isDone());

            List<String> expected = List.of("m1", "m2", "m3", "m4");
            awaitEquals(expected, () -> messagesUnchecked(secondary1.port()), Duration.ofSeconds(2));

            secondary2.start(secondary2Port);
            HttpResponse<String> m3Response = m3.get(8, TimeUnit.SECONDS);
            assertEquals(200, m3Response.statusCode());
            awaitEquals(expected, () -> messagesUnchecked(secondary2.port()), Duration.ofSeconds(4));
            assertEquals(expected, messages(master.port()));
        } finally {
            stopQuietly(master);
            stopQuietly(secondary1);
            stopQuietly(secondary2);
        }
    }

    private static void stopQuietly(Javalin app) {
        if (app == null) {
            return;
        }
        try {
            app.stop();
        } catch (RuntimeException ignored) {
        }
    }

    private MasterNode master(MockedTransport transport, List<NodeId> secondaries, Duration timeout, Duration retry) {
        return master(transport, secondaries, timeout, retry, Duration.ofHours(1));
    }

    private MasterNode master(
            MockedTransport transport,
            List<NodeId> secondaries,
            Duration timeout,
            Duration retry,
            Duration heartbeat
    ) {
        return new MasterNode(masterId, transport, secondaries, timeout, retry, heartbeat);
    }

    private static Javalin startSecondary(String nodeId) {
        AppConfig config = new AppConfig(NodeRole.SECONDARY, nodeId, 0, List.of(), 0);
        Javalin app = App.createApp(config, new InMemoryLog(), (HttpClient) null);
        app.start(0);
        return app;
    }

    private static long measureMillis(Runnable action) {
        long started = System.nanoTime();
        action.run();
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    }

    private static void awaitTrue(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(15);
        }
        fail("condition not met within " + timeout);
    }

    private static void awaitEquals(List<String> expected, Supplier<List<String>> actual, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<String> last = List.of();
        while (System.nanoTime() < deadline) {
            try {
                last = actual.get();
            } catch (RuntimeException e) {
                last = List.of();
            }
            if (expected.equals(last)) {
                return;
            }
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        assertEquals(expected, last);
    }

    private static HealthResponse health(int port) {
        try {
            HttpResponse<String> response = get(port, "/health");
            assertEquals(200, response.statusCode());
            return JsonSupport.MAPPER.readValue(response.body(), HealthResponse.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> messages(int port) throws Exception {
        HttpResponse<String> response = get(port, "/messages");
        assertEquals(200, response.statusCode());
        return JsonSupport.MAPPER.readValue(response.body(), MessagesResponse.class).messages();
    }

    private static List<String> messagesUnchecked(int port) {
        try {
            return messages(port);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static HttpResponse<String> get(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .GET()
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> postJson(int port, String body) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + port + "/messages"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
