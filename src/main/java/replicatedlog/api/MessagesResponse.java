package replicatedlog.api;

import java.util.List;

public record MessagesResponse(List<String> messages) {
}
