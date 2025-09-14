package agg;

import common.LamportClock;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PutWorker implements Runnable {

    // Represents a single PUT request task
    public static class PutTask implements Comparable<PutTask> {
        final long lamport;                    // Lamport timestamp from client
        final String contentServerId;          // ID of content server
        final String stationId;                // Station identifier
        final String rawJson;                  // Weather data in JSON
        final long arrivalSeq;                 // Sequence to break ties
        final CompletableFuture<Result> resultFuture; // For async result

        public PutTask(long lamport, String contentServerId, String stationId,
                       String rawJson, long arrivalSeq, CompletableFuture<Result> fut) {
            this.lamport = lamport;
            this.contentServerId = contentServerId;
            this.stationId = stationId;
            this.rawJson = rawJson;
            this.arrivalSeq = arrivalSeq;
            this.resultFuture = fut;
        }

        @Override
        public int compareTo(PutTask o) {
            // Order primarily by Lamport timestamp
            int c = Long.compare(this.lamport, o.lamport);
            if (c != 0) return c;
            // Break ties by content server ID
            c = Objects.requireNonNullElse(this.contentServerId, "")
                    .compareTo(Objects.requireNonNullElse(o.contentServerId, ""));
            if (c != 0) return c;
            // Finally break ties by arrival order
            return Long.compare(this.arrivalSeq, o.arrivalSeq);
        }
    }

    // Result returned after processing a PUT
    public static class Result {
        public final boolean created;          // true if new station created
        public final long appliedLamport;      // Lamport timestamp applied
        public Result(boolean created, long L) {
            this.created = created;
            this.appliedLamport = L;
        }
    }

    private final StateStore store;            // In-memory state store
    private final WalManager wal;              // Write-Ahead Log manager
    private final LamportClock clock;          // Shared Lamport clock
    private final BlockingQueue<PutTask> queue;// Priority queue for tasks
    private volatile boolean running = true;

    public PutWorker(StateStore store, WalManager wal, LamportClock clock) {
        this.store = store;
        this.wal = wal;
        this.clock = clock;
        this.queue = new PriorityBlockingQueue<>();
    }

    private final AtomicLong arrivalSeq = new AtomicLong();

    // Submit a new PUT request into the queue
    public CompletableFuture<Result> submit(long lamport, String contentServerId, String stationId, String rawJson) {
        CompletableFuture<Result> fut = new CompletableFuture<>();
        PutTask t = new PutTask(lamport, contentServerId, stationId, rawJson,
                arrivalSeq.incrementAndGet(), fut);
        queue.add(t);
        return fut;
    }

    // stop the worker
    public void shutdown() { running = false; }

    @Override
    public void run() {
        while (running) {
            try {
                // Take next task from queue (blocking if empty)
                PutTask t = queue.take();

                // Synchronize Lamport clock with request
                clock.onReceive(t.lamport);
                long L = clock.tick(); // Tick for applying update

                // Log the PUT before applying (WAL first)
                wal.appendPut(L, t.contentServerId, t.stationId, t.rawJson);

                // Apply to state store
                boolean created = store.applyPut(t.stationId, t.rawJson, t.contentServerId, L);

                // Complete the future with result
                t.resultFuture.complete(new Result(created, L));

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }
}