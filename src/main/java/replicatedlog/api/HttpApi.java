package replicatedlog.api;

import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import replicatedlog.config.AppConfig;
import replicatedlog.config.NodeRole;
import replicatedlog.log.InMemoryLog;
import replicatedlog.replication.ReplicationService;

public final class HttpApi {
    private static final Logger logger = LoggerFactory.getLogger("replicatedlog.api");

    private HttpApi() {
    }

    public static void configure(Javalin app, AppConfig config, InMemoryLog log, ReplicationService replication) {
        app.get("/health", ctx -> ctx.json(new HealthResponse(config.role().name(), config.nodeId())));

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
                logger.info("Client POST /messages '{}' on master {}", request.text(), config.nodeId());
                replication.append(request.text());
                ctx.status(HttpStatus.OK).json(new MessagesResponse(replication.list()));
            });
        } else {
            app.post("/replicate", ctx -> {
                MessageRequest request = ctx.bodyAsClass(MessageRequest.class);
                if (config.replicationDelayMs() > 0) {
                    logger.info(
                            "Secondary {} delaying replication of '{}' for {} ms",
                            config.nodeId(),
                            request.text(),
                            config.replicationDelayMs()
                    );
                    try {
                        Thread.sleep(config.replicationDelayMs());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
                log.append(request.text());
                logger.info("Secondary {} appended '{}' and sending ACK", config.nodeId(), request.text());
                ctx.json(new AckResponse());
            });
        }
    }
}
