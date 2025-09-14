package agg;

import common.HttpMessage.HttpRequest;
import common.HttpMessage.HttpResponse;
import common.LamportClock;
import common.JsonUtil;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Router {
    private final StateStore store;        // in-memory store of weather data
    private final LamportClock clock;      // shared Lamport clock
    private final PutWorker putWorker;     // worker to handle PUT requests

    private static final Pattern ID_FIELD = Pattern.compile("\"id\"\\s*:\\s*\"([^\"]+)\"");

    public Router(StateStore store, LamportClock clock, PutWorker putWorker) {
        this.store = store;
        this.clock = clock;
        this.putWorker = putWorker;
    }

    // Main request handler: routes GET and PUT
    public HttpResponse handle(HttpRequest req) {
        String method = req.method.toUpperCase();
        OptionalLong lamportHdr = req.lamportHeader();
        lamportHdr.ifPresent(clock::onReceive);

        switch (method) {
            case "GET":
                if ("/weather.json".equals(req.path)) return handleGet();
                break;
            case "PUT":
                if ("/weather.json".equals(req.path)) return handlePut(req);
                break;
            default:
                break;
        }
        // Fallback for unsupported methods/paths
        HttpResponse bad = HttpResponse.of(400, "Bad Request", null, null);
        bad.headers.put("X-Lamport", Long.toString(clock.peek()));
        return bad;
    }

    // Handle GET /weather.json
    private HttpResponse handleGet() {
        clock.tick();
        if (store.isEmpty()) {
            // No data → return 204
            HttpResponse r = HttpResponse.of(204, "No Content", null, null);
            r.headers.put("X-Lamport", Long.toString(clock.peek()));
            return r;
        }
        // Serialize all station JSON into an array
        List<String> raw = store.snapshotRawJson();
        String array = JsonUtil.joinObjectsToArray(raw);
        String body = "{\"stations\": " + array + "}";
        HttpResponse r = HttpResponse.of(200, "OK", body, "application/json");
        r.headers.put("X-Lamport", Long.toString(clock.peek()));
        return r;
    }

    // Handle PUT /weather.json
    private HttpResponse handlePut(HttpRequest req) {
        // Empty body → return 204
        if (req.body == null || req.body.length == 0) {
            clock.tick();
            HttpResponse r = HttpResponse.of(204, "No Content", null, null);
            r.headers.put("X-Lamport", Long.toString(clock.peek()));
            return r;
        }

        String body = new String(req.body, StandardCharsets.UTF_8);

        // Basic JSON format check
        if (!body.trim().startsWith("{") || !body.trim().endsWith("}")) {
            clock.tick();
            HttpResponse r = HttpResponse.of(500, "Internal Server Error",
                    "{\"error\":\"invalid JSON format\"}", "application/json");
            r.headers.put("X-Lamport", Long.toString(clock.peek()));
            return r;
        }

        // Extract station id
        Matcher m = ID_FIELD.matcher(body);
        if (!m.find()) {
            clock.tick();
            HttpResponse r = HttpResponse.of(500, "Internal Server Error",
                    "{\"error\":\"missing id field\"}", "application/json");
            r.headers.put("X-Lamport", Long.toString(clock.peek()));
            return r;
        }
        String stationId = m.group(1);

        // Identify content server (header or fallback id)
        String contentServerId = req.headers.getOrDefault("X-Content-Server", "cs-" + UUID.randomUUID());

        // Submit to PutWorker
        long Lreq = req.lamportHeader().orElse(clock.peek());
        CompletableFuture<PutWorker.Result> fut = putWorker.submit(Lreq, contentServerId, stationId, body);

        try {
            // Wait for result and set proper status code
            PutWorker.Result res = fut.get();
            HttpResponse r = HttpResponse.of(res.created ? 201 : 200,
                    res.created ? "Created" : "OK",
                    null, null);
            r.headers.put("X-Lamport", Long.toString(res.appliedLamport));
            return r;
        } catch (Exception e) {
            // On failure return 500
            clock.tick();
            HttpResponse r = HttpResponse.of(500, "Internal Server Error",
                    "{\"error\":\"failed to apply PUT\"}", "application/json");
            r.headers.put("X-Lamport", Long.toString(clock.peek()));
            return r;
        }
    }
}