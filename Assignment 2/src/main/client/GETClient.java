package client;

import common.LamportClock;
import java.io.*;
import java.net.Socket;
import java.net.URI;


public class GETClient {
    private static final LamportClock clock = new LamportClock(); // local Lamport clock
    private static final int MAX_RETRIES = 3;     // number of retry attempts
    private static final int RETRY_DELAY_MS = 2000; // retry delay (2 seconds)

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("java client.GETClient <server:port> [stationId]");
            System.exit(1);
        }
        // Parse server URI
        URI uri = parseServerUri(args[0]);
        String host = uri.getHost() == null ? "localhost" : uri.getHost();
        int port = (uri.getPort() == -1 ? 4567 : uri.getPort());

        // Retry loop for robustness
        boolean success = false;
        for (int attempt = 1; attempt <= MAX_RETRIES && !success; attempt++) {
            try {
                runGetRequest(host, port);
                success = true;
            } catch (IOException e) {
                System.err.println("Attempt " + attempt + " failed: " + e.getMessage());
                if (attempt < MAX_RETRIES) {
                    System.err.println("Retrying in " + (RETRY_DELAY_MS / 1000) + " seconds");
                    Thread.sleep(RETRY_DELAY_MS);
                } else {
                    System.err.println("ALL RETRIES FAILED. EXITING.");
                }
            }
        }
    }

    // Perform GET /weather.json request to the server
    private static void runGetRequest(String host, int port) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            long L = clock.onSend(); // Lamport tick for sending

            // Build and send GET request
            String request =
                    "GET /weather.json HTTP/1.1\r\n" +
                            "Host: " + host + ":" + port + "\r\n" +
                            "X-Lamport: " + L + "\r\n" +
                            "\r\n";
            out.write(request.getBytes());
            out.flush();

            // Read status line
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String statusLine = br.readLine();
            if (statusLine == null) {
                System.err.println("No response from server");
                return;
            }
            System.out.println(statusLine);

            // Read headers
            int contentLength = 0;
            String line;
            while (!(line = br.readLine()).isEmpty()) {
                System.out.println(line);
                if (line.toLowerCase().startsWith("content-length:")) {
                    contentLength = Integer.parseInt(line.split(":")[1].trim());
                }
                if (line.toLowerCase().startsWith("x-lamport:")) {
                    long Lresp = Long.parseLong(line.split(":")[1].trim());
                    clock.onReceive(Lresp); // update Lamport clock
                }
            }

            // Read and pretty-print body if present
            if (contentLength > 0) {
                char[] buf = new char[contentLength];
                int read = br.read(buf);
                String body = new String(buf, 0, read);
                System.out.println("\n ---Weather Data---");
                prettyPrintJson(body);
            } else {
                System.out.println("No Content");
            }
        }
    }

    // Normalize server URI string
    private static URI parseServerUri(String arg) throws Exception {
        if (!arg.startsWith("http")) {
            arg = "http://" + arg;
        }
        return new URI(arg);
    }

    // Simple pretty-print of JSON into key = value pairs
    private static void prettyPrintJson(String json) {
        json = json.replace("{", "")
                .replace("}", "")
                .replace("[", "")
                .replace("]", "");
        String[] pairs = json.split(",");
        for (String p : pairs) {
            String[] kv = p.split(":", 2);
            if (kv.length == 2) {
                String key = kv[0].trim().replaceAll("\"", "");
                String val = kv[1].trim().replaceAll("\"", "");
                System.out.println(key + " = " + val);
            }
        }
    }
}