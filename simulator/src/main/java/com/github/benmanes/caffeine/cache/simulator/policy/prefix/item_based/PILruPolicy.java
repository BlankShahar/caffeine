package com.github.benmanes.caffeine.cache.simulator.policy.prefix.item_based;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.ItemBasedChunkManager;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateUnderflowDelay;

@Policy.PolicySpec(name = "prefix.item-based.LRU")
public final class PILruPolicy implements Policy {
    // --- Hysteresis ---
    private static final double GROW_RATIO = 0.05;   // grow if desired >= 1.05 * current
    private static final double SHRINK_RATIO = 0.05; // shrink if desired <= 0.95 * current

    final PolicyStats policyStats;
    final long maximumCacheSize;

    long currentCacheSize;
    long currentTime;

    /**
     * Fast key -> node lookup
     */
    final Long2ObjectOpenHashMap<Prefix> data;
    /**
     * LRU order: head = MRU, tail = LRU
     */
    final Prefix head;
    final Prefix tail;

    final ItemBasedChunkManager chunk_manager;

    public PILruPolicy(Config config) {
        var settings = new BasicSettings(config);
        this.policyStats = new PolicyStats(name());
        this.maximumCacheSize = settings.maximumSize();
        this.currentCacheSize = 0L;
        this.currentTime = 0L;

        this.data = new Long2ObjectOpenHashMap<>();
        // Sentinel nodes for simpler list ops
        this.head = new Prefix(-1, 0);
        this.tail = new Prefix(-1, 0);
        head.next = tail;
        tail.prev = head;
        chunk_manager = new ItemBasedChunkManager();
    }

    @Override
    public void record(AccessEvent event) {
        currentTime++;
        switch (event.operation()) {
            case READ:
                onRead(event);
                break;
            case WRITE:
                onDelete(event);
                onRead(event);
                break;
            case DELETE:
                onDelete(event);
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation: " + event.operation());
        }
    }

    private void onDelete(AccessEvent event) {
        Prefix prefix = data.remove(event.key());
        if (prefix != null) {
            detach(prefix);
            currentCacheSize -= prefix.size;
            policyStats.recordOperation();
            policyStats.recordEviction();
        }
    }

    private void recordStats(long fullItemSize, long cachedSize, double retrievalDelay) {
        double delay = calculateUnderflowDelay(retrievalDelay, fullItemSize, cachedSize, Consts.BANDWIDTH);
        policyStats.addDelay(delay);

        double latency = calculateLatency(retrievalDelay, fullItemSize, cachedSize, Consts.BANDWIDTH);
        policyStats.addLatency(latency);
    }

    private void onRead(AccessEvent event) {
        long key = event.key();
        Prefix prefix = data.get(key);

        // record stats with the *current* cached size
        if (event.operation() == AccessEvent.Operation.READ)
            recordStats(event.itemSize(), (prefix != null ? prefix.size : 0), event.retrievalDelay());

        // compute desired before adding this sample (so this sample affects the next call)
        long desired = Math.min(event.itemSize(), chunk_manager.getChunkSize(key, event.itemSize()));
        chunk_manager.addDelay(key, event.retrievalDelay());

        if (prefix != null) {
            // Hit: recency update
            policyStats.recordHit();
            prefix.lastAccessTime = currentTime;
            moveToHead(prefix);
            policyStats.recordOperation();

            // Resize in place (may fully evict if desired == 0)
            maybeResize(prefix, desired);
            return;
        }

        // Miss: if desired == 0 → do not cache this item at all
        if (desired == 0) {
            policyStats.recordMiss();
            return;
        }

        policyStats.recordMiss();

        // Too big to admit
        if (desired > maximumCacheSize) {
            return;
        }

        // Evict until it fits
        while (currentCacheSize + desired > maximumCacheSize && !isEmpty()) {
            Prefix victim = removeLRU();
            data.remove(victim.key);
            currentCacheSize -= victim.size;
            policyStats.recordEviction();
        }

        // Admit with desired size
        Prefix newPrefix = new Prefix(key, desired);
        newPrefix.lastAccessTime = currentTime;
        addToHead(newPrefix);
        data.put(key, newPrefix);
        currentCacheSize += desired;
        policyStats.recordOperation();
        policyStats.recordAdmission();
    }

    // === Resizing logic ===
    private void maybeResize(Prefix prefix, long desired) {
        long cur = prefix.size;

        // If desired == 0 ⇒ fully evict this prefix
        if (desired == 0) {
            detach(prefix);
            data.remove(prefix.key);
            currentCacheSize -= cur;
            policyStats.recordOperation();
            policyStats.recordEviction(); // full eviction
            return;
        }

        if (desired > maximumCacheSize) {
            desired = maximumCacheSize; // cap, but still attempt to grow if possible
        }

        // Grow if significantly larger (≥ +5%)
        if (desired >= (long) Math.ceil(cur * (1.0 + GROW_RATIO))) {
            long delta = desired - cur;

            // Make room (never evict this key)
            while (currentCacheSize + delta > maximumCacheSize) {
                Prefix victim = removeLRUExcept(prefix.key);
                if (victim == null) break; // only this item left; cannot grow further
                data.remove(victim.key);
                currentCacheSize -= victim.size;
                policyStats.recordEviction();
            }
            if (currentCacheSize + delta <= maximumCacheSize) {
                prefix.size = desired;
                currentCacheSize += delta;
                policyStats.recordOperation();
                policyStats.recordAdmission(); // count bytes added
            }
            return;
        }

        // Shrink if significantly smaller (≤ −5%)
        if (desired <= (long) Math.floor(cur * (1.0 - SHRINK_RATIO))) {
            long newSize = desired; // desired > 0 here
            long delta = cur - newSize;
            if (delta > 0) {
                prefix.size = newSize;
                currentCacheSize -= delta;
                policyStats.recordOperation();
                policyStats.recordEviction(); // count bytes removed
            }
        }
    }

    /**
     * Remove and return the LRU node that is NOT 'exceptKey'.
     */
    private Prefix removeLRUExcept(long exceptKey) {
        Prefix cur = tail.prev;
        while (cur != null && cur != head) {
            if (cur.key != exceptKey) {
                Prefix victim = cur;
                detach(victim);
                return victim;
            }
            cur = cur.prev;
        }
        return null;
    }

    // ==== Doubly-linked list helpers (head = MRU, tail = LRU) ====
    private boolean isEmpty() {
        return head.next == tail;
    }

    private void moveToHead(Prefix prefix) {
        detach(prefix);
        addToHead(prefix);
    }

    private void addToHead(Prefix prefix) {
        prefix.next = head.next;
        prefix.prev = head;
        head.next.prev = prefix;
        head.next = prefix;
    }

    private void detach(Prefix prefix) {
        prefix.prev.next = prefix.next;
        prefix.next.prev = prefix.prev;
        prefix.prev = null;
        prefix.next = null;
    }

    /**
     * Removes and returns LRU node (at tail.prev).
     */
    private Prefix removeLRU() {
        Prefix lru = tail.prev;
        if (lru == head) return null;
        detach(lru);
        return lru;
    }

    @Override
    public void finished() {
        Policy.super.finished();
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    @Override
    public String name() {
        return Policy.super.name();
    }

    // ==== Node ====
    static final class Prefix {
        final long key;
        long size;                // mutable for in-place resizing
        long lastAccessTime;
        Prefix prev;
        Prefix next;

        Prefix(long key, long size) {
            this.key = key;
            this.size = size;
            this.lastAccessTime = 0L;
        }
    }
}
