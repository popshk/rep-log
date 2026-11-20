package replicatedlog.api;

public record MessageRequest(String text, Integer w, Long seq) {
}
