package replicatedlog;

import io.javalin.Javalin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import replicatedlog.api.JsonSupport;
import replicatedlog.api.MessagesResponse;
import replicatedlog.config.AppConfig;
import replicatedlog.config.NodeRole;
import replicatedlog.log.InMemoryLog;
import replicatedlog.node.MasterNode;
import replicatedlog.node.SecondaryNode;
import replicatedlog.replication.MockedTransport;
import replicatedlog.replication.NodeId;
import replicatedlog.replication.ReplicationService;
import replicatedlog.replication.Transport;

import java.net.URI;
import java.util.ArrayList;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class WriteConcernTest {
    private final NodeId masterId = new NodeId("master");
    private final NodeId secondary1Id = new NodeId("secondary-1");
    private final NodeId secondary2Id = new NodeId("secondary-2");

    @Test
    void secondaryHidesAGapUntilTheMissingSequenceArrives() {
        SecondaryNode secondary = new SecondaryNode(secondary1Id);

        secondary.append(2, "m2");
        secondary.append(4, "m4");
        assertEquals(List.of(), secondary.listMsgs());

        secondary.append(1, "m1");
        assertEquals(List.of("m1", "m2"), secondary.listMsgs());

        secondary.append(2, "duplicate");
        assertEquals(List.of("m1", "m2"), secondary.listMsgs());

        secondary.append(3, "m3");
        assertEquals(List.of("m1", "m2", "m3", "m4"), secondary.listMsgs());
    }

    @Test
    void writeConcernMustCountTheMasterAndTheSecondaries() {
        MockedTransport transport = new MockedTransport();
        transport.register(new SecondaryNode(secondary1Id));
        transport.register(new SecondaryNode(secondary2Id));
        MasterNode master = new MasterNode(masterId, transport, List.of(secondary1Id, secondary2Id));

        assertThrows(IllegalArgumentException.class, () -> master.appendMsg("too-small", 0));
        assertThrows(IllegalArgumentException.class, () -> master.appendMsg("too-big", 4));
        assertEquals(List.of(), master.listMsgs());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void writeConcernReturnsEarlyWhileASlowerSecondaryIsStillCatchingUp() throws Exception {
        MockedTransport transport = new MockedTransport();
        SecondaryNode secondary1 = new SecondaryNode(secondary1Id);
        SecondaryNode secondary2 = new SecondaryNode(secondary2Id);
        transport.register(secondary1);
        transport.register(secondary2);
        MasterNode master = new MasterNode(masterId, transport, List.of(secondary1Id, secondary2Id));

        transport.setDelay(masterId, secondary2Id, Duration.ofSeconds(5));

        long m1Ms = measureMillis(() -> master.appendMsg("m1", 1));
        assertTrue(m1Ms < 1_000, "w=1 should return after the master alone, was " + m1Ms + "ms");
        assertEquals(List.of("m1"), master.listMsgs());
        assertEquals(List.of(), secondary2.listMsgs());

        long m2Ms = measureMillis(() -> master.appendMsg("m2", 2));
        assertTrue(m2Ms < 1_500, "w=2 should return after the fast secondary, was " + m2Ms + "ms");

        CompletableFuture<Long> m3 = CompletableFuture.supplyAsync(
                () -> measureMillis(() -> master.appendMsg("m3", 3))
        );
        awaitTrue(() -> master.listMsgs().contains("m3"), Duration.ofSeconds(2));
        assertFalse(m3.isDone(), "w=3 should still be waiting for secondary-2");

        long m4Ms = measureMillis(() -> master.appendMsg("m4", 1));
        assertTrue(m4Ms < 1_000, "w=1 must not wait for the blocked w=3 append, was " + m4Ms + "ms");
        assertFalse(m3.isDone(), "w=3 must still be blocked after the parallel w=1 append");
        assertEquals(List.of("m1", "m2", "m3", "m4"), master.listMsgs());

        awaitEquals(List.of("m1", "m2", "m3", "m4"), secondary1::listMsgs, Duration.ofSeconds(2));
        assertPrefixOf(List.of("m1", "m2", "m3"), secondary2.listMsgs());
        assertFalse(secondary2.listMsgs().contains("m3"), "secondary-2 has not ACKed m3 yet");

        long m3Ms = m3.get(12, TimeUnit.SECONDS);
        assertTrue(m3Ms >= 4_500 && m3Ms <= 10_000, "w=3 should block on the 5s delay, was " + m3Ms + "ms");

        List<String> expected = List.of("m1", "m2", "m3", "m4");
        awaitEquals(expected, secondary2::listMsgs, Duration.ofSeconds(8));
        assertEquals(expected, master.listMsgs());
        assertEquals(expected, secondary1.listMsgs());
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void reorderedDeliveryIsListedInTotalOrder() throws Exception {
        MockedTransport transport = new MockedTransport();
        SecondaryNode secondary1 = new SecondaryNode(secondary1Id);
        SecondaryNode secondary2 = new SecondaryNode(secondary2Id);
        transport.register(secondary1);
        transport.register(secondary2);
        MasterNode master = new MasterNode(masterId, transport, List.of(secondary1Id, secondary2Id));

        transport.reorderMessages(masterId, secondary1Id, 2);

        CompletableFuture<Void> first = CompletableFuture.runAsync(() -> master.appendMsg("m1"));
        awaitTrue(() -> transport.bufferedCount(masterId, secondary1Id) == 1, Duration.ofSeconds(2));
        master.appendMsg("m2");
        first.get(5, TimeUnit.SECONDS);

        assertEquals(List.of(2L, 1L), transport.deliveredSequences(secondary1Id));
        assertEquals(List.of("m1", "m2"), secondary1.listMsgs());
        assertEquals(List.of("m1", "m2"), secondary2.listMsgs());
        assertEquals(List.of("m1", "m2"), master.listMsgs());
    }

    @Test
    void omittedWriteConcernStillWaitsForEverySecondary() throws Exception {
        InMemoryLog log = new InMemoryLog();
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        Transport transport = (from, to, sequence, message) -> {
            calls.incrementAndGet();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        };
        ReplicationService replication = new ReplicationService(
                masterId,
                log,
                transport,
                List.of(secondary1Id, secondary2Id)
        );
        AppConfig config = new AppConfig(NodeRole.MASTER, "master", 0, List.of(), 0);
        Javalin app = App.createApp(config, log, replication);
        app.start(0);
        try {
            CompletableFuture<HttpResponse<String>> post = CompletableFuture.supplyAsync(
                    () -> postJson(app.port(), "{\"text\":\"m1\"}")
            );
            awaitTrue(() -> calls.get() == 2, Duration.ofSeconds(2));
            assertFalse(post.isDone(), "omitted w must wait for every secondary");
            release.countDown();
            HttpResponse<String> response = post.get(5, TimeUnit.SECONDS);
            assertEquals(200, response.statusCode());
            assertEquals(2, calls.get());
            assertEquals(List.of("m1"), messages(app.port()));
        } finally {
            app.stop();
        }
    }

    @Test
    void httpWriteConcernOneReturnsWhileReplicationIsStillRunning() throws Exception {
        AppConfig secondaryConfig = new AppConfig(NodeRole.SECONDARY, "secondary-1", 0, List.of(), 1_500);
        Javalin secondary = App.createApp(secondaryConfig, new InMemoryLog(), (HttpClient) null);
        secondary.start(0);
        AppConfig masterConfig = new AppConfig(
                NodeRole.MASTER,
                "master",
                0,
                List.of("http://127.0.0.1:" + secondary.port()),
                0
        );
        Javalin master = App.createApp(masterConfig, new InMemoryLog(), App.createHttpClient());
        master.start(0);
        try {
            long elapsed = measureMillis(() -> {
                HttpResponse<String> response = postJson(master.port(), "{\"text\":\"m1\",\"w\":1}");
                assertEquals(200, response.statusCode());
            });
            assertTrue(elapsed < 700, "w=1 should not wait for the delayed secondary, was " + elapsed + "ms");
            assertEquals(List.of("m1"), messages(master.port()));
            assertEquals(List.of(), messages(secondary.port()));

            awaitEquals(List.of("m1"), () -> messagesUnchecked(secondary.port()), Duration.ofSeconds(4));
        } finally {
            master.stop();
            secondary.stop();
        }
    }

    @Test
    void httpReplicationKeepsOrderOnEverySecondary() throws Exception {
        Javalin secondary1 = startSecondary("secondary-1", 0);
        Javalin secondary2 = startSecondary("secondary-2", 0);
        Javalin master = startMaster(secondary1.port(), secondary2.port());
        try {
            assertEquals(200, postJson(master.port(), "{\"text\":\"m1\",\"w\":3}").statusCode());
            assertEquals(200, postJson(master.port(), "{\"text\":\"m2\",\"w\":2}").statusCode());
            assertEquals(200, postJson(master.port(), "{\"text\":\"m3\",\"w\":1}").statusCode());

            List<String> expected = List.of("m1", "m2", "m3");
            awaitEquals(expected, () -> messagesUnchecked(secondary1.port()), Duration.ofSeconds(3));
            awaitEquals(expected, () -> messagesUnchecked(secondary2.port()), Duration.ofSeconds(3));
            assertEquals(expected, messages(master.port()));
        } finally {
            master.stop();
            secondary1.stop();
            secondary2.stop();
        }
    }

    @Test
    void invalidWriteConcernIsRejected() throws Exception {
        Javalin secondary = startSecondary("secondary-1", 0);
        Javalin master = startMaster(secondary.port());
        try {
            HttpResponse<String> response = postJson(master.port(), "{\"text\":\"m1\",\"w\":3}");
            assertEquals(400, response.statusCode());
            assertTrue(response.body().contains("write concern"));
            assertEquals(List.of(), messages(master.port()));
            assertEquals(List.of(), messages(secondary.port()));
        } finally {
            master.stop();
            secondary.stop();
        }
    }

    private static Javalin startSecondary(String nodeId, long delayMs) {
        AppConfig config = new AppConfig(NodeRole.SECONDARY, nodeId, 0, List.of(), delayMs);
        Javalin app = App.createApp(config, new InMemoryLog(), (HttpClient) null);
        app.start(0);
        return app;
    }

    private static Javalin startMaster(int... secondaryPorts) {
        List<String> urls = new ArrayList<>();
        for (int port : secondaryPorts) {
            urls.add("http://127.0.0.1:" + port);
        }
        AppConfig config = new AppConfig(NodeRole.MASTER, "master", 0, urls, 0);
        Javalin app = App.createApp(config, new InMemoryLog(), App.createHttpClient());
        app.start(0);
        return app;
    }

    private static void assertPrefixOf(List<String> expectedPrefixSpace, List<String> actual) {
        assertTrue(
                actual.size() <= expectedPrefixSpace.size()
                        && expectedPrefixSpace.subList(0, actual.size()).equals(actual),
                "expected a prefix of " + expectedPrefixSpace + " but was " + actual
        );
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
            Thread.sleep(20);
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
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        assertEquals(expected, last);
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
