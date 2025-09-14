package client;

import common.LamportClock;

import java.io.*;
import java.net.Socket;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

public class ContentServer {
    private static final LamportClock clock = new LamportClock(); // local Lamport clock

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java client.ContentServer <server:port> <datafile.txt>");
            System.exit(1);
        }

        // Parse server URI and extract host/port
        URI uri = parseServerUri(args[0]);
        String host = uri.getHost() == null ? "localhost" : uri.getHost();
        int port = (uri.getPort() == -1 ? 4567 : uri.getPort());
        String filePath = args[1];

        // Read key-value pairs from file and build JSON
        Map<String, String> fields = readKeyValueFile(filePath);
        if (!fields.containsKey("id")) {
            System.err.println("Error: data file must contain an 'id' field");
            System.exit(1);
        }
        String json = buildJson(fields);

        // Connect to aggregation server
        try (Socket socket = new Socket(host, port)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            long L = clock.onSend(); // increment Lamport clock for send

            // Build HTTP PUT request
            byte[] body = json.getBytes();
            String request =
                    "PUT /weather.json HTTP/1.1\r\n" +
                            "Host: " + host + ":" + port + "\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length: " + body.length + "\r\n" +
                            "X-Lamport: " + L + "\r\n" +
                            "X-Content-Server: cs1\r\n" +
                            "\r\n";

            // Send request + body
            out.write(request.getBytes());
            out.write(body);
            out.flush();

            // Read server response
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String statusLine = br.readLine();
            if (statusLine == null) {
                System.err.println("No response from server");
                return;
            }
            System.out.println(statusLine);

            // Process headers
            String line;
            while ((line = br.readLine()) != null && !line.isEmpty()) {
                System.out.println(line);
                if (line.toLowerCase().startsWith("x-lamport:")) {
                    long Lresp = Long.parseLong(line.split(":")[1].trim());
                    clock.onReceive(Lresp); // update Lamport clock
                }
            }
        }
    }

    // Ensure "localhost:4567" etc. can be parsed into a URI
    private static URI parseServerUri(String arg) throws Exception {
        if (!arg.startsWith("http")) {
            arg = "http://" + arg;
        }
        return new URI(arg);
    }

    // Read text file of key:value pairs into a map
    private static Map<String, String> readKeyValueFile(String path) throws IOException {
        Map<String, String> map = new LinkedHashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || !line.contains(":")) continue;
                String[] kv = line.split(":", 2);
                map.put(kv[0].trim(), kv[1].trim());
            }
        }
        return map;
    }

    // Build JSON string from the map
    private static String buildJson(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        boolean first = true;
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (!first) sb.append(",\n");
            String key = e.getKey();
            String val = e.getValue();
            // numeric values stay numeric, others quoted
            if (val.matches("-?\\d+(\\.\\d+)?")) {
                sb.append("  \"").append(key).append("\": ").append(val);
            } else {
                sb.append("  \"").append(key).append("\": \"").append(val).append("\"");
            }
            first = false;
        }
        sb.append("\n}");
        return sb.toString();
    }
}