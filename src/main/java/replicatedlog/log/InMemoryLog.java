package replicatedlog.log;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryLog {
    private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

    public void append(String text) {
        messages.add(text);
    }

    public List<String> list() {
        return List.copyOf(messages);
    }
}
