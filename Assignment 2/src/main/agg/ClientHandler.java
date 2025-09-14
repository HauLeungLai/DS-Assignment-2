package agg;

import common.HttpMessage.HttpRequest;
import common.HttpMessage.HttpResponse;
import common.HttpMessage;
import common.LamportClock;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ClientHandler implements Runnable {
    private final Socket socket;       // client connection socket
    private final Router router;       // routes requests to GET/PUT handlers
    private final LamportClock clock;  // shared Lamport clock

    public ClientHandler(Socket socket, Router router, LamportClock clock) {
        this.socket = socket;
        this.router = router;
        this.clock = clock;
    }

    @Override
    public void run() {
        try (Socket s = socket;
             InputStream in = s.getInputStream();
             OutputStream out = s.getOutputStream()) {

            // Parse incoming HTTP request
            HttpRequest req = HttpMessage.HttpRequest.parse(in);

            // Update Lamport clock on receive (if header exists)
            req.lamportHeader().ifPresent(clock::onReceive);

            // Route request to appropriate handler (GET/PUT/error)
            HttpResponse resp = router.handle(req);

            // Add Lamport header before sending response
            resp.headers.put("X-Lamport", Long.toString(clock.onSend()));

            // Send response to client
            resp.write(out);

        } catch (IOException e) {
            // Ignore I/O errors from broken connections
        }
    }
}