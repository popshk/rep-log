package replicatedlog.api;

import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import replicatedlog.config.AppConfig;
import replicatedlog.config.NodeRole;
import replicatedlog.log.InMemoryLog;
import replicatedlog.replication.NoQuorumException;
import replicatedlog.replication.ReplicationService;

import java.util.Map;

public final class HttpApi {
    private static final Logger logger = LoggerFactory.getLogger("replicatedlog.api");

    private HttpApi() {
    }

    public static void configure(Javalin app, AppConfig config, InMemoryLog log, ReplicationService replication) {
        app.exception(IllegalArgumentException.class, (exception, ctx) ->
                ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", exception.getMessage()))
        );
        app.exception(NoQuorumException.class, (exception, ctx) ->
                ctx.status(HttpStatus.SERVICE_UNAVAILABLE).json(Map.of("error", exception.getMessage()))
        );

        app.get("/health", ctx -> {
            Map<String, String> secondaries = replication == null ? Map.of() : replication.secondaryHealth();
            String status = replication != null && !replication.hasQuorum() ? "READ_ONLY" : "UP";
            ctx.json(new HealthResponse(status, config.role().name(), config.nodeId(), secondaries));
        });

        app.post("/echo", ctx -> {
            EchoRequest request = ctx.bodyAsClass(EchoRequest.class);
            logger.info("Echo on {} ({}): {}", config.nodeId(), config.role(), request.text());
            ctx.json(new EchoResponse(request.text()));
        });

        app.get("/messages", ctx -> ctx.json(new MessagesResponse(log.list())));

        if (config.role() == NodeRole.MASTER) {
            if (replication == null) {
                throw new IllegalStateException("Master node requires a ReplicationService");
            }
            app.post("/messages", ctx -> {
                MessageRequest request = ctx.bodyAsClass(MessageRequest.class);
                if (request.text() == null || request.text().isBlank()) {
                    throw new IllegalArgumentException("Message text must not be blank");
                }
                int writeConcern = request.w() == null ? replication.fullWriteConcern() : request.w();
                logger.info(
                        "Client POST /messages '{}' w={} on master {}",
                        request.text(),
                        writeConcern,
                        config.nodeId()
                );
                replication.append(request.text(), writeConcern);
                ctx.status(HttpStatus.OK).json(new MessagesResponse(replication.list()));
            });
        } else {
            app.post("/replicate", ctx -> {
                MessageRequest request = ctx.bodyAsClass(MessageRequest.class);
                if (request.text() == null || request.text().isBlank()) {
                    throw new IllegalArgumentException("Message text must not be blank");
                }
                if (request.seq() == null) {
                    throw new IllegalArgumentException("Replicated message requires a sequence number");
                }
                if (config.replicationDelayMs() > 0) {
                    logger.info(
                            "Secondary {} delaying replication of '{}' (seq {}) for {} ms",
                            config.nodeId(),
                            request.text(),
                            request.seq(),
                            config.replicationDelayMs()
                    );
                    try {
                        Thread.sleep(config.replicationDelayMs());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
                log.append(request.seq(), request.text());
                logger.info(
                        "Secondary {} stored '{}' (seq {}) and sending ACK",
                        config.nodeId(),
                        request.text(),
                        request.seq()
                );
                ctx.json(new AckResponse());
            });
        }
    }
}
