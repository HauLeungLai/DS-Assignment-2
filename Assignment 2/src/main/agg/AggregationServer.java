package agg;

import common.LamportClock;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AggregationServer {
    // Server configuration
    private final int port;
    private final ExecutorService pool;

    // Core components
    private final LamportClock clock = new LamportClock();   // Lamport clock for ordering
    private final StateStore store = new StateStore();       // In-memory data store
    private final WalManager wal;                           // Write-Ahead Log for crash recovery
    private final PutWorker putWorker;                      // Worker that serializes PUT requests
    private final Thread putThread;                         // Thread running the PutWorker
    private final Thread sweeperThread;                     // Thread running the expiry sweeper
    private final Router router;                            // Routes HTTP requests (GET/PUT)

    public AggregationServer(int port, int handlers) throws IOException {
        this.port = port;
        this.pool = Executors.newFixedThreadPool(Math.max(2, handlers));

        // Initialize WAL and replay log for crash recovery
        this.wal = new WalManager(new File("wal.log"));
        this.wal.replay(store, clock);

        // Initialize router and workers
        this.putWorker = new PutWorker(store, wal, clock);
        this.router = new Router(store, clock, putWorker);

        // Start background worker thread for handling PUT queue
        this.putThread = new Thread(putWorker, "put-worker");
        this.putThread.setDaemon(true);
        this.putThread.start();

        // Start background sweeper thread for 30s expiry
        this.sweeperThread = new Thread(
                new ExpirySweeper(store, wal, clock, Duration.ofSeconds(30)),
                "expiry-sweeper"
        );
        this.sweeperThread.setDaemon(true);
        this.sweeperThread.start();
    }

    public void start() throws IOException {
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("AggregationServer listening on port " + port);
            while (true) {
                // Accept client connections and hand off to thread pool
                Socket client = server.accept();
                pool.submit(new ClientHandler(client, router, clock));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Default port 4567 unless overridden
        int port = 4567;
        if (args.length >= 1) port = Integer.parseInt(args[0]);

        // Create server with thread pool proportional to CPU cores
        new AggregationServer(
                port,
                Runtime.getRuntime().availableProcessors() * 2
        ).start();
    }
}