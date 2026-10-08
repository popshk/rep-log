package replicatedlog;

import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import replicatedlog.api.EchoClient;
import replicatedlog.api.HttpApi;
import replicatedlog.api.JsonSupport;
import replicatedlog.config.AppConfig;
import replicatedlog.config.NodeRole;
import replicatedlog.log.InMemoryLog;
import replicatedlog.replication.HttpTransport;
import replicatedlog.replication.NodeId;
import replicatedlog.replication.ReplicationService;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public final class App {
    private static final Logger logger = LoggerFactory.getLogger("replicatedlog.App");

    private App() {
    }

    public static void main(String[] args) {
        if (args.length > 0 && "echo".equals(args[0])) {
            if (args.length < 2) {
                throw new IllegalArgumentException("Usage: echo <baseUrl> <text>");
            }
            String url = args[1];
            String text = args.length > 2
                    ? String.join(" ", Arrays.copyOfRange(args, 2, args.length))
                    : "hello";
            if (text.isBlank()) {
                text = "hello";
            }
            try (EchoClient client = new EchoClient(url)) {
                System.out.println(client.echo(text));
            }
            return;
        }

        AppConfig config = AppConfig.fromEnv();
        logger.info(
                "Starting {} node '{}' on port {} (secondaries={}, delayMs={})",
                config.role(),
                config.nodeId(),
                config.port(),
                config.secondaryUrls(),
                config.replicationDelayMs()
        );

        createApp(config).start("0.0.0.0", config.port());
    }

    public static Javalin createApp(AppConfig config) {
        return createApp(config, new InMemoryLog(), createHttpClient());
    }

    public static Javalin createApp(AppConfig config, InMemoryLog log, HttpClient httpClient) {
        ReplicationService replication = null;
        if (config.role() == NodeRole.MASTER) {
            HttpTransport transport = HttpTransport.fromUrls(config.secondaryUrls(), httpClient);
            List<NodeId> secondaries = config.secondaryUrls().stream()
                    .map(HttpTransport::nodeIdFromUrl)
                    .toList();
            replication = new ReplicationService(new NodeId(config.nodeId()), log, transport, secondaries);
        }
        return createApp(config, log, replication, httpClient);
    }

    public static Javalin createApp(AppConfig config, InMemoryLog log, ReplicationService replication) {
        return createApp(config, log, replication, null);
    }

    private static Javalin createApp(
            AppConfig config,
            InMemoryLog log,
            ReplicationService replication,
            HttpClient httpClient
    ) {
        Javalin app = Javalin.create(cfg -> {
            cfg.jsonMapper(new JavalinJackson(JsonSupport.MAPPER, false));
            cfg.showJavalinBanner = false;
            cfg.requestLogger.http((ctx, ms) ->
                    logger.info("{} {} -> {} ({} ms)", ctx.method(), ctx.path(), ctx.status(), ms)
            );
        });
        if (httpClient != null) {
            app.events(event -> event.serverStopping(httpClient::close));
        }
        HttpApi.configure(app, config, log, replication);
        return app;
    }

    public static HttpClient createHttpClient() {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }
}
