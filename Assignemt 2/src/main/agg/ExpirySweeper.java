package agg;

import common.LamportClock;

import java.io.IOException;
import java.time.*;
import java.util.Map;

public class ExpirySweeper implements Runnable {
    private final StateStore store;       // reference to in-memory state
    private final WalManager wal;         // WAL for recording expiry events
    private final LamportClock clock;     // Lamport clock for ordering
    private final long ttlMillis;         // time-to-live threshold in ms
    private volatile boolean running = true; // flag for stopping the loop

    public ExpirySweeper(StateStore store, WalManager wal, LamportClock clock, Duration ttl) {
        this.store = store;
        this.wal = wal;
        this.clock = clock;
        this.ttlMillis = ttl.toMillis();
    }

    // Stop the sweeper thread
    public void shutdown() { running = false; }

    @Override
    public void run() {
        while (running) {
            try {
                // Snapshot of last-seen timestamps for each content server
                Map<String, Instant> lastSeen = store.lastSeenSnapshot();
                Instant now = Instant.now();

                for (Map.Entry<String, Instant> e : lastSeen.entrySet()) {
                    if (e.getValue() == null) continue;

                    // Calculate age of last update
                    long age = Duration.between(e.getValue(), now).toMillis();

                    // Expire entries older than TTL
                    if (age > ttlMillis) {
                        clock.tick(); // increment Lamport clock
                        wal.appendExpire(clock.peek(), e.getKey()); // log expiry
                        store.removeAllFromContentServer(e.getKey()); // remove data
                    }
                }

                // Sleep for 1 second between sweeps
                Thread.sleep(1000);

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException ioe) {
                // Ignore/log WAL write issues but keep running
            }
        }
    }
}