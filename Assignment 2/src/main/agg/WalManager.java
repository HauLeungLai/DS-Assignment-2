package agg;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

import common.LamportClock;

public class WalManager implements Closeable {
    private final File file;                 // WAL file on disk
    private final ReentrantLock lock = new ReentrantLock(); // lock for thread safety
    private final OutputStream out;          // append stream to WAL file

    public WalManager(File file) throws IOException {
        this.file = file;
        this.out = new BufferedOutputStream(new FileOutputStream(file, true)); // append mode
    }

    // Append a PUT entry into WAL
    public void appendPut(long lamport, String contentServerId, String stationId, String rawJson) throws IOException {
        String line = String.format(
                "PUT|%d|%s|%s|%s|%s\n",
                lamport, esc(contentServerId), esc(stationId), esc(rawJson), Instant.now().toString()
        );
        writeLine(line);
    }

    // Append an EXPIRE entry into WAL
    public void appendExpire(long lamport, String contentServerId) throws IOException {
        String line = String.format(
                "EXPIRE|%d|%s|%s\n",
                lamport, esc(contentServerId), Instant.now().toString()
        );
        writeLine(line);
    }

    // Low-level write with flushing + fsync
    private void writeLine(String line) throws IOException {
        lock.lock();
        try {
            out.write(line.getBytes(StandardCharsets.UTF_8));
            out.flush();
            if (out instanceof FileOutputStream fos) {
                fos.getFD().sync(); // ensure durability
            }
        } finally {
            lock.unlock();
        }
    }

    // Replay WAL into memory after crash/restart
    public void replay(StateStore store, LamportClock clock) throws IOException {
        if (!file.exists()) return;

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\|", -1);
                if (parts.length < 2) continue; // skip malformed lines

                try {
                    String type = parts[0];
                    if ("PUT".equals(type) && parts.length >= 5) {
                        long L = Long.parseLong(parts[1]);
                        String csId = unesc(parts[2]);
                        String stId = unesc(parts[3]);
                        String rawJson = unesc(parts[4]);
                        clock.onReceive(L); // update Lamport clock
                        store.applyPut(stId, rawJson, csId, L);
                    } else if ("EXPIRE".equals(type) && parts.length >= 3) {
                        long L = Long.parseLong(parts[1]);
                        String csId = unesc(parts[2]);
                        clock.onReceive(L); // update Lamport clock
                        store.removeAllFromContentServer(csId);
                    }
                } catch (Exception ignored) {
                    // skip corrupted WAL entries
                }
            }
        }
    }

    // Escape special characters for safe logging
    private static String esc(String s) {
        return s.replace("|", "\\|").replace("\n", "\\n");
    }

    // Reverse escape for replay
    private static String unesc(String s) {
        return s.replace("\\|", "|").replace("\\n", "\n");
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}