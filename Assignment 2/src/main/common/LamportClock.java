package common;

import java.util.concurrent.atomic.AtomicLong;

public class LamportClock {
    private final AtomicLong value = new AtomicLong(0L); // clock value

    // Read the current clock value (no change)
    public long peek(){
        return value.get();
    }

    // Advance the clock by 1 (local event)
    public long tick() {
        return value.incrementAndGet();
    }

    // When sending a message: advance and return new value
    public long onSend(){
        return tick();
    }

    // When receiving a message: update clock with remote value
    // Lamport rule: max(local, remote) + 1
    public long onReceive(long remote){
        return value.updateAndGet(local -> Math.max(local, remote) + 1);
    }
}