package common;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class HttpMessage {

    public static final String CRLF = "\r\n"; // HTTP line ending

    //  HTTP Request
    public static class HttpRequest {
        public final String method;
        public final String path;
        public final String version;
        public final Map<String, String> headers;
        public final byte[] body;

        private HttpRequest(String method, String path, String version,
                            Map<String, String> headers, byte[] body) {
            this.method = method;
            this.path = path;
            this.version = version;
            this.headers = headers;
            this.body = body;
        }

        // Parse an HTTP request from InputStream
        public static HttpRequest parse(InputStream in) throws IOException {
            BufferedInputStream bin = new BufferedInputStream(in);
            bin.mark(8192);
            ByteArrayOutputStream headerBuf = new ByteArrayOutputStream();

            // Read until CRLFCRLF (end of headers)
            int prev = -1, prev2 = -1, prev3 = -1;
            while (true) {
                int b = bin.read();
                if (b == -1) throw new EOFException("unexpected EOF while reading headers");
                headerBuf.write(b);
                if (prev3 == '\r' && prev2 == '\n' && prev == '\r' && b == '\n') {
                    break; // found header terminator
                }
                prev3 = prev2; prev2 = prev; prev = b;
            }

            // Split headers
            String headerText = headerBuf.toString(StandardCharsets.US_ASCII);
            String[] lines = headerText.split("\\r?\\n");
            if (lines.length == 0) throw new IOException("empty request");

            // Parse request line: METHOD PATH VERSION
            String[] reqLine = lines[0].split(" ", 3);
            if (reqLine.length < 3) throw new IOException("bad request line");
            String method = reqLine[0];
            String path = reqLine[1];
            String version = reqLine[2];

            // Parse headers
            Map<String, String> headers = new LinkedHashMap<>();
            for (int i = 1; i < lines.length; i++) {
                String line = lines[i];
                if (line.isEmpty()) continue;
                int idx = line.indexOf(":");
                if (idx > 0) {
                    String k = line.substring(0, idx).trim();
                    String v = line.substring(idx + 1).trim();
                    headers.put(k, v);
                }
            }

            // Parse body if Content-Length present
            int contentLen = 0;
            if (headers.containsKey("Content-Length")) {
                try { contentLen = Integer.parseInt(headers.get("Content-Length")); }
                catch (NumberFormatException e) { throw new IOException("invalid Content-Length"); }
            }

            byte[] body = new byte[contentLen];
            int read = 0;
            while (read < contentLen) {
                int r = bin.read(body, read, contentLen - read);
                if (r == -1) throw new EOFException("unexpected EOF while reading body");
                read += r;
            }

            return new HttpRequest(method, path, version, headers, body);
        }

        // Extract Lamport clock header if present
        public OptionalLong lamportHeader() {
            String v = headers.get("X-Lamport");
            if (v == null) return OptionalLong.empty();
            try { return OptionalLong.of(Long.parseLong(v)); }
            catch (NumberFormatException e) { return OptionalLong.empty(); }
        }
    }

    //  HTTP Response
    public static class HttpResponse {
        public int statusCode;
        public String reason;
        public Map<String, String> headers = new LinkedHashMap<>();
        public byte[] body = new byte[0];

        public HttpResponse(int statusCode, String reason) {
            this.statusCode = statusCode;
            this.reason = reason;
        }

        // Create a response with optional body
        public static HttpResponse of(int code, String reason, String bodyText, String contentType) {
            HttpResponse r = new HttpResponse(code, reason);
            if (bodyText != null) {
                r.body = bodyText.getBytes(StandardCharsets.UTF_8);
                r.headers.put("Content-Length", Integer.toString(r.body.length));
                if (contentType != null) r.headers.put("Content-Type", contentType);
            } else {
                r.headers.put("Content-Length", "0");
            }
            return r;
        }

        // Serialize response to OutputStream
        public void write(OutputStream out) throws IOException {
            String statusLine = "HTTP/1.1 " + statusCode + " " + reason + CRLF;
            out.write(statusLine.getBytes(StandardCharsets.US_ASCII));
            for (Map.Entry<String, String> e : headers.entrySet()) {
                String h = e.getKey() + ": " + e.getValue() + CRLF;
                out.write(h.getBytes(StandardCharsets.US_ASCII));
            }
            out.write(CRLF.getBytes(StandardCharsets.US_ASCII)); // end headers
            if (body.length > 0) {
                out.write(body);
            }
            out.flush();
        }
    }
}