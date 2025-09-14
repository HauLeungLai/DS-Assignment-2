package test;

import org.junit.jupiter.api.*;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AggregationServerTest {
    private static Process serverProcess; // reference to the running server

    @BeforeAll
    static void startServer() throws Exception {
        // Start AggregationServer on port 4567 before all tests
        serverProcess = new ProcessBuilder(
                "java", "-cp", "out", "agg.AggregationServer", "4567"
        ).inheritIO().start();
        Thread.sleep(1000); // wait for server to fully start
    }

    @AfterAll
    static void stopServer() {
        // Stop the server after all tests
        serverProcess.destroy();
    }

    // Utility to send a raw HTTP request string and return the full response
    private String sendRawHttp(String request) throws Exception {
        try (Socket socket = new Socket("localhost", 4567)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            out.write(request.getBytes(StandardCharsets.UTF_8));
            out.flush();

            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            StringBuilder sb = new StringBuilder();
            String line;

            // Read headers
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
                if (line.isEmpty()) break; // end of headers
            }

            // Parse content length if present
            String headers = sb.toString();
            int len = 0;
            for (String h : headers.split("\n")) {
                if (h.toLowerCase().startsWith("content-length:")) {
                    len = Integer.parseInt(h.split(":")[1].trim());
                }
            }

            // Read body if any
            if (len > 0) {
                char[] buf = new char[len];
                br.read(buf);
                sb.append(new String(buf));
            }
            return sb.toString();
        }
    }

    @Test @Order(1)
    void testPutFirstTimeCreated() throws Exception {
        // First PUT should return 201 Created (or 200 OK if already exists)
        String body = "{\"id\":\"IDS60901\",\"name\":\"Adelaide\",\"air_temp\":13.3}";
        String req = "PUT /weather.json HTTP/1.1\r\n" +
                "Host: localhost:4567\r\n" +
                "Content-Type: application/json\r\n" +
                "Content-Length: " + body.length() + "\r\n" +
                "X-Lamport: 1\r\n" +
                "X-Content-Server: cs1\r\n\r\n" +
                body;
        String resp = sendRawHttp(req);
        assertTrue(resp.contains("201 Created") || resp.contains("200 OK"));
    }

    @Test @Order(2)
    void testPutSecondTimeOk() throws Exception {
        // Second PUT from same content server should return 200 OK
        String body = "{\"id\":\"IDS60901\",\"name\":\"Adelaide\",\"air_temp\":15.0}";
        String req = "PUT /weather.json HTTP/1.1\r\n" +
                "Host: localhost:4567\r\n" +
                "Content-Type: application/json\r\n" +
                "Content-Length: " + body.length() + "\r\n" +
                "X-Lamport: 2\r\n" +
                "X-Content-Server: cs1\r\n\r\n" +
                body;
        String resp = sendRawHttp(req);
        assertTrue(resp.contains("200 OK"));
    }

    @Test @Order(3)
    void testGetReturnsData() throws Exception {
        // GET should return JSON with the previously stored station
        String req = "GET /weather.json HTTP/1.1\r\n" +
                "Host: localhost:4567\r\n" +
                "X-Lamport: 3\r\n\r\n";
        String resp = sendRawHttp(req);
        assertTrue(resp.contains("200 OK"));
        assertTrue(resp.contains("\"id\":\"IDS60901\""));
    }

    @Test @Order(4)
    void testEmptyPutReturns204() throws Exception {
        // PUT with no body should return 204 No Content
        String req = "PUT /weather.json HTTP/1.1\r\n" +
                "Host: localhost:4567\r\n" +
                "Content-Type: application/json\r\n" +
                "Content-Length: 0\r\n" +
                "X-Lamport: 4\r\n" +
                "X-Content-Server: cs2\r\n\r\n";
        String resp = sendRawHttp(req);
        assertTrue(resp.contains("204 No Content"));
    }

    @Test @Order(5)
    void testInvalidJsonReturns500() throws Exception {
        // PUT with invalid JSON should return 500 Internal Server Error
        String badBody = "{not valid json}";
        String req = "PUT /weather.json HTTP/1.1\r\n" +
                "Host: localhost:4567\r\n" +
                "Content-Type: application/json\r\n" +
                "Content-Length: " + badBody.length() + "\r\n" +
                "X-Lamport: 5\r\n" +
                "X-Content-Server: cs3\r\n\r\n" +
                badBody;
        String resp = sendRawHttp(req);
        assertTrue(resp.contains("500"));
    }

    @Test @Order(6)
    void testBadMethodReturns400() throws Exception {
        // Any unsupported method (POST) should return 400 Bad Request
        String req = "POST /weather.json HTTP/1.1\r\n" +
                "Host: localhost:4567\r\n" +
                "X-Lamport: 6\r\n\r\n";
        String resp = sendRawHttp(req);
        assertTrue(resp.contains("400"));
    }

    @Test @Order(7)
    void testExpiryAfter30Seconds() throws Exception {
        // Data from a content server should expire after 30 seconds of inactivity
        String body = "{\"id\":\"IDS99999\",\"name\":\"TempStation\",\"air_temp\":20.0}";
        String req = "PUT /weather.json HTTP/1.1\r\n" +
                "Host: localhost:4567\r\n" +
                "Content-Type: application/json\r\n" +
                "Content-Length: " + body.length() + "\r\n" +
                "X-Lamport: 7\r\n" +
                "X-Content-Server: tempCS\r\n\r\n" +
                body;
        String resp = sendRawHttp(req);
        assertTrue(resp.contains("200 OK") || resp.contains("201 Created"));

        Thread.sleep(31000); // wait for expiry

        String getReq = "GET /weather.json HTTP/1.1\r\nHost: localhost:4567\r\nX-Lamport: 8\r\n\r\n";
        String getResp = sendRawHttp(getReq);
        assertTrue(getResp.contains("204 No Content"));
    }

    @Test @Order(8)
    void testWalRecoveryAfterCrash() throws Exception {
        // Server should recover data from WAL after crash and restart
        String body = "{\"id\":\"IDS77777\",\"name\":\"CrashTest\",\"air_temp\":12.3}";
        String req = "PUT /weather.json HTTP/1.1\r\n" +
                "Host: localhost:4567\r\n" +
                "Content-Type: application/json\r\n" +
                "Content-Length: " + body.length() + "\r\n" +
                "X-Lamport: 9\r\n" +
                "X-Content-Server: crashCS\r\n\r\n" +
                body;
        String resp = sendRawHttp(req);
        assertTrue(resp.contains("200 OK") || resp.contains("201 Created"));

        // Simulate crash
        serverProcess.destroy();
        Thread.sleep(1000);

        // Restart server
        serverProcess = new ProcessBuilder(
                "java", "-cp", "out", "agg.AggregationServer", "4567"
        ).inheritIO().start();
        Thread.sleep(1000);

        // Data should still exist thanks to WAL replay
        String getReq = "GET /weather.json HTTP/1.1\r\nHost: localhost:4567\r\nX-Lamport: 10\r\n\r\n";
        String getResp = sendRawHttp(getReq);
        assertTrue(getResp.contains("CrashTest"));
    }

    @Test @Order(9)
    void testLamportClockMonotonic() throws Exception {
        // Ensure Lamport clock values never go backwards
        String getReq1 = "GET /weather.json HTTP/1.1\r\nHost: localhost:4567\r\nX-Lamport: 11\r\n\r\n";
        String resp1 = sendRawHttp(getReq1);

        String getReq2 = "GET /weather.json HTTP/1.1\r\nHost: localhost:4567\r\nX-Lamport: 12\r\n\r\n";
        String resp2 = sendRawHttp(getReq2);

        int lamport1 = extractLamport(resp1);
        int lamport2 = extractLamport(resp2);

        assertTrue(lamport2 >= lamport1, "Lamport clock must be monotonic");
    }

    // Helper to extract Lamport header value from HTTP response
    private int extractLamport(String resp) {
        for (String line : resp.split("\n")) {
            if (line.toLowerCase().startsWith("x-lamport:")) {
                return Integer.parseInt(line.split(":")[1].trim());
            }
        }
        return -1;
    }
}