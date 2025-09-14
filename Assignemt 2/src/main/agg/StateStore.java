package agg;

import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StateStore {
    // Represents a single station weather record
    public static class WeatherRecord {
        public final String stationId;              // station identifier
        public final String rawJson;                // raw JSON string for the station
        public final String sourceContentServerId;  // content server ID that sent it
        public final long lamportApplied;           // Lamport timestamp applied
        public final Instant updatedAt;             // time this record was updated

        public WeatherRecord(String stationId, String rawJson, String sourceContentServerId,
                             long lamportApplied, Instant updatedAt) {
            this.stationId = stationId;
            this.rawJson = rawJson;
            this.sourceContentServerId = sourceContentServerId;
            this.lamportApplied = lamportApplied;
            this.updatedAt = updatedAt;
        }
    }

    // Maps stationId → WeatherRecord
    private final Map<String, WeatherRecord> stations = new ConcurrentHashMap<>();
    // Maps contentServerId -> last seen timestamp
    private final Map<String, Instant> lastSeenByContent = new ConcurrentHashMap<>();
    // Maps contentServerId -> set of stationIds uploaded by that server
    private final Map<String, Set<String>> stationsByContent = new ConcurrentHashMap<>();
    // Lock for protecting snapshot/updates
    private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock();

    // Track which content servers have been seen this run
    // (used to decide between 201 Created vs 200 OK)
    private final Set<String> seenContentServersThisRun = ConcurrentHashMap.newKeySet();

    // Returns true if no stations stored
    public boolean isEmpty() {
        rw.readLock().lock();
        try { return stations.isEmpty(); }
        finally { rw.readLock().unlock(); }
    }

    // Return a snapshot of all station raw JSON strings
    public List<String> snapshotRawJson() {
        rw.readLock().lock();
        try {
            List<String> list = new ArrayList<>(stations.size());
            for (WeatherRecord r : stations.values()) list.add(r.rawJson);
            return list;
        } finally { rw.readLock().unlock(); }
    }

    // Apply a new PUT record into the store
    // Returns true if this content server is seen for the first time (-> 201 Created)
    public boolean applyPut(String stationId, String rawJson, String contentServerId, long lamport) {
        rw.writeLock().lock();
        try {
            WeatherRecord rec = new WeatherRecord(stationId, rawJson, contentServerId, lamport, Instant.now());
            stations.put(stationId, rec);
            lastSeenByContent.put(contentServerId, Instant.now());
            stationsByContent.computeIfAbsent(contentServerId, k -> ConcurrentHashMap.newKeySet())
                    .add(stationId);
            boolean firstTime = seenContentServersThisRun.add(contentServerId);
            return firstTime;
        } finally { rw.writeLock().unlock(); }
    }

    // Remove all stations belonging to a given content server (expired)
    public List<String> removeAllFromContentServer(String contentServerId) {
        List<String> removed = new ArrayList<>();
        rw.writeLock().lock();
        try {
            Set<String> ids = stationsByContent.getOrDefault(contentServerId, Collections.emptySet());
            for (String sid : ids) {
                if (stations.remove(sid) != null) removed.add(sid);
            }
            stationsByContent.remove(contentServerId);
            lastSeenByContent.remove(contentServerId);
        } finally { rw.writeLock().unlock(); }
        return removed;
    }

    // Return a copy of last-seen timestamps for all content servers
    public Map<String, Instant> lastSeenSnapshot() {
        return new HashMap<>(lastSeenByContent);
    }

    // Refresh the last-seen timestamp for a given content server
    public void refreshLastSeen(String contentServerId) {
        lastSeenByContent.put(contentServerId, Instant.now());
    }
}