package replicatedlog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import replicatedlog.node.MasterNode;
import replicatedlog.node.SecondaryNode;
import replicatedlog.replication.MockedTransport;
import replicatedlog.replication.NodeId;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockingReplicationTest {
    private final NodeId masterId = new NodeId("master");
    private final NodeId secondary1Id = new NodeId("secondary-1");
    private final NodeId secondary2Id = new NodeId("secondary-2");

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void replicationIsBlockingAndParallelAsSpecifiedInIteration1() {
        MockedTransport transport = new MockedTransport();
        SecondaryNode secondary1 = new SecondaryNode(secondary1Id);
        SecondaryNode secondary2 = new SecondaryNode(secondary2Id);
        transport.register(secondary1);
        transport.register(secondary2);
        MasterNode master = new MasterNode(masterId, transport, List.of(secondary1Id, secondary2Id));

        transport.setDelay(masterId, secondary1Id, Duration.ofSeconds(5));
        long m1Ms = measureMillis(() -> master.appendMsg("m1"));
        assertEquals(List.of("m1"), master.listMsgs());
        assertTrue(m1Ms >= 4_000 && m1Ms <= 9_000, "append m1 should block ~5s, was " + m1Ms + "ms");

        transport.setDelay(masterId, secondary2Id, Duration.ofSeconds(6));
        long m2Ms = measureMillis(() -> master.appendMsg("m2"));
        assertEquals(List.of("m1", "m2"), master.listMsgs());
        assertTrue(m2Ms >= 5_500 && m2Ms <= 10_000, "append m2 should block ~6s (max of 5s and 6s), was " + m2Ms + "ms");
        assertTrue(m2Ms < 11_000, "append m2 must run in parallel, not 5s+6s sequential, was " + m2Ms + "ms");

        transport.removeDelay(masterId, secondary1Id);
        transport.removeDelay(masterId, secondary2Id);

        long m3Ms = measureMillis(() -> master.appendMsg("m3"));
        assertTrue(m3Ms < 500, "append m3 should not block after delays are removed, was " + m3Ms + "ms");

        List<String> expected = List.of("m1", "m2", "m3");
        assertEquals(expected, master.listMsgs());
        assertEquals(expected, secondary1.listMsgs());
        assertEquals(expected, secondary2.listMsgs());
    }

    private static long measureMillis(Runnable action) {
        long started = System.nanoTime();
        action.run();
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    }
}
