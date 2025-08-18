package com.github.benmanes.caffeine.cache.simulator.policy.prefix.cluster_based;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.ClusterBasedChunkManager;
import com.github.benmanes.caffeine.cache.simulator.policy.size_aware.SearchableMinHeap;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;

import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateUnderflowDelay;

@Policy.PolicySpec(name = "prefix.cluster-based.LRFU")
public final class PCLrfuPolicy implements Policy {
    private static final double LAMBDA = 2.0; // Decay rate in time units

    private final PolicyStats policyStats;
    private final SearchableMinHeap<Long, Prefix> heap;
    private final long maximumCacheSize;
    private long currentCacheSize;
    private long currentTime;
    final ClusterBasedChunkManager chunk_manager;

    public PCLrfuPolicy(Config config) {
        var settings = new BasicSettings(config);
        this.maximumCacheSize = settings.maximumSize();
        this.policyStats = new PolicyStats(name());
        this.heap = new SearchableMinHeap<>((int) maximumCacheSize, this::compareNodes);
        this.currentCacheSize = 0;
        this.currentTime = 0;
        this.chunk_manager = new ClusterBasedChunkManager();
    }

    @Override
    public void record(AccessEvent event) {
        currentTime++;

        switch (event.operation()) {
            case READ:
                onRead(event);
                break;
            case WRITE:
                onWrite(event);
                break;
            case DELETE:
                onDelete(event);
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation: " + event.operation());
        }
    }

    private void onWrite(AccessEvent event) {
        policyStats.recordOperation();
        onDelete(event);
        onRead(event);
    }

    private void onDelete(AccessEvent event) {
        var existingPrefix = heap.get(event.key());
        if (existingPrefix != null) {
            // prefix exists, remove it
            heap.remove(existingPrefix.key);
            policyStats.recordOperation();
            currentCacheSize -= existingPrefix.size;
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
        policyStats.recordOperation();
        currentTime++;

        Prefix prefix = heap.get(event.key());
        recordStats(event.itemSize(), prefix != null ? prefix.size : 0, event.retrievalDelay());
        event.itemSize = Math.min(event.itemSize(), chunk_manager.getChunkSize(event.key(), event.itemSize()));
        chunk_manager.addDelay(event.key(), event.retrievalDelay());

        if (prefix == null) {
            prefix = new Prefix(event.key(), event.itemSize(), currentTime);
        }
        updateScore(prefix);

        if (prefix.size > maximumCacheSize) {
            policyStats.recordRejection();
            return;
        }

        if (prefix.isEmpty()) {
            while (currentCacheSize + prefix.size > maximumCacheSize) {
                evict();
            }
            currentCacheSize += prefix.size;
            policyStats.recordAdmission();
        }

        if (prefix.isEmpty() && heap.contains(prefix.key)) heap.remove(prefix.key);
        else heap.upsert(prefix.key, prefix);
        policyStats.recordOperation();
    }

    private void updateScore(Prefix prefix) {
        double decay = Math.pow(0.5, (double) (currentTime - prefix.lastAccessTime) / LAMBDA);
        prefix.score = prefix.score * decay + 1;
        prefix.lastAccessTime = currentTime;
    }

    private void evict() {
        Prefix victim = heap.extractMin().value();
        currentCacheSize -= victim.size;
        policyStats.recordEviction();
    }

    private int compareNodes(long k1, long k2) {
        Prefix p1 = heap.get(k1);
        Prefix p2 = heap.get(k2);
        if (p1 == null || p2 == null) {
            throw new IllegalStateException("Node not found in heap: " + k1 + " or " + k2);
        }
        return Double.compare(p1.score, p2.score);
    }


    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    @Override
    public void finished() {
        Policy.super.finished();
    }

    static final class Prefix {
        final long key;
        final long size;
        long lastAccessTime;
        double score;

        Prefix(long key, long size, long now) {
            this.key = key;
            this.size = size;
            this.lastAccessTime = now;
            this.score = 0;
        }

        boolean isEmpty() {
            return size == 0;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("key", key)
                    .add("size", size)
                    .add("score", score)
                    .toString();
        }
    }
}
