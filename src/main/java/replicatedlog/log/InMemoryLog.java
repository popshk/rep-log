package replicatedlog.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory log with master-assigned sequence numbers.
 * A message becomes visible only when every lower sequence is already present,
 * so secondaries keep a total order even if deliveries arrive reordered.
 */
public class InMemoryLog {
    private final List<String> committed = new ArrayList<>();
    private final Map<Long, String> pending = new HashMap<>();
    private long nextSequence = 1;
    private long nextExpected = 1;

    public synchronized long append(String text) {
        long sequence = nextSequence++;
        pending.put(sequence, text);
        drain();
        return sequence;
    }

    public synchronized void append(long sequence, String text) {
        if (sequence < nextExpected || pending.containsKey(sequence)) {
            return;
        }
        pending.put(sequence, text);
        drain();
    }

    public synchronized List<String> list() {
        return List.copyOf(committed);
    }

    private void drain() {
        String text;
        while ((text = pending.remove(nextExpected)) != null) {
            committed.add(text);
            nextExpected++;
        }
    }
}
