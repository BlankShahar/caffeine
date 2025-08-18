package com.github.benmanes.caffeine.cache.simulator.policy.prefix.cluster_based;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.ClusterBasedChunkManager;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateUnderflowDelay;
import static com.google.common.base.Preconditions.checkState;

@Policy.PolicySpec(name = "prefix.cluster-based.Arc")
public final class PCArcPolicy implements Policy {

    private enum Q {T1, B1, T2, B2}

    private final Prefix headT1 = new Prefix(0);
    private final Prefix headT2 = new Prefix(0);
    private final Prefix headB1 = new Prefix(0);
    private final Prefix headB2 = new Prefix(0);

    private final Long2ObjectMap<Prefix> data = new Long2ObjectOpenHashMap<>();
    private final PolicyStats policyStats = new PolicyStats(name());
    private final long maximumCacheSize;
    private long p;

    private long sizeT1, sizeT2, sizeB1, sizeB2;
    private int currentTime = 0;

    private final ClusterBasedChunkManager chunk_manager;

    public PCArcPolicy(Config cfg) {
        maximumCacheSize = new BasicSettings(cfg).maximumSize();
        p = 0;
        chunk_manager = new ClusterBasedChunkManager();
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
        onDelete(event);
        onRead(event);
    }

    private void onDelete(AccessEvent event) {
        var existingItem = data.get(event.key());
        if (existingItem != null) {
            // item exists, remove it
            data.remove(existingItem.key);
            if (existingItem.q == Q.T1) sizeT1 -= existingItem.size;
            else if (existingItem.q == Q.T2) sizeT2 -= existingItem.size;
            else if (existingItem.q == Q.B1) sizeB1 -= existingItem.size;
            else if (existingItem.q == Q.B2) sizeB2 -= existingItem.size;
            existingItem.remove();
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
        currentTime++;

        Prefix prefix = data.get(event.key());
        recordStats(event.itemSize(), prefix != null ? prefix.size : 0, event.retrievalDelay());

        event.itemSize = Math.min(event.itemSize(), chunk_manager.getChunkSize(event.key(), event.itemSize()));
        chunk_manager.addDelay(event.key(), event.retrievalDelay());

        if (prefix == null) {
            onMiss(event);
            return;
        }

        if (prefix.q == Q.T1 || prefix.q == Q.T2) {
            onHit(prefix, event);
        } else if (prefix.q == Q.B1) {
            onHitB1(prefix, event);
        } else if (prefix.q == Q.B2) {
            onHitB2(prefix, event);
        }
    }

    private void onHit(Prefix n, AccessEvent e) {
        if (n.q == Q.T1) {
            sizeT1 -= n.size;
            sizeT2 += n.size;
        }
        n.remove();
        policyStats.recordOperation();
        n.q = Q.T2;
        n.appendToTail(headT2);
        policyStats.recordOperation();
        policyStats.recordHit();
    }

    private void onHitB1(Prefix n, AccessEvent e) {
        policyStats.recordMiss();

        p = Math.min(maximumCacheSize, p + n.size);
        if (n.size <= (maximumCacheSize - sizeT1)) {
            evictToMakeSpace(Q.T2, n.size);
            moveFromGhostToT2(n);
        } else {
            evictGhost(n);
        }
    }

    private void onHitB2(Prefix n, AccessEvent e) {
        policyStats.recordMiss();

        p = Math.max(0, p - n.size);
        if (n.size <= (maximumCacheSize - sizeT1)) {
            evictToMakeSpace(Q.T2, n.size);
            moveFromGhostToT2(n);
        } else {
            evictGhost(n);
        }
    }

    private void moveFromGhostToT2(Prefix n) {
        if (n.q == Q.B1) sizeB1 -= n.size;
        else sizeB2 -= n.size;
        n.remove();
        policyStats.recordOperation();
        n.q = Q.T2;
        n.appendToTail(headT2);
        policyStats.recordOperation();
        sizeT2 += n.size;
    }

    private void onMiss(AccessEvent event) {
        long size = event.itemSize();
        policyStats.recordMiss();

        if (size > maximumCacheSize) {
            return;
        }

        long L1 = sizeT1 + sizeB1;
        long L2 = sizeT2 + sizeB2;

        if (L1 == maximumCacheSize) {
            if (sizeT1 < maximumCacheSize) {
                evictGhost(headB1.next);
            } else {
                evictResident(headT1.next);
            }
        } else if (L1 < maximumCacheSize && (L1 + L2) >= maximumCacheSize) {
            if ((L1 + L2) >= 2 * maximumCacheSize) {
                evictGhost(headB2.next);
            }
        }

        if (size <= (maximumCacheSize - sizeT2)) {
            evictToMakeSpace(Q.T1, size);
            Prefix n = new Prefix(event.key(), size);
            n.q = Q.T1;
            n.appendToTail(headT1);
            policyStats.recordOperation();
            data.put(event.key(), n);
            sizeT1 += size;
        }
    }

    private void evictToMakeSpace(Q target, long needed) {
        long available = (target == Q.T1) ? (maximumCacheSize - sizeT2) : (maximumCacheSize - sizeT1);
        long usage = (target == Q.T1) ? sizeT1 : sizeT2;

        while (usage + needed > available) {
            Prefix victim = (target == Q.T1) ? headT1.next : headT2.next;
            if (victim == victim.next || victim.size == 0) break;
            evictResident(victim);
            usage = (target == Q.T1) ? sizeT1 : sizeT2;
            available = (target == Q.T1) ? (maximumCacheSize - sizeT2) : (maximumCacheSize - sizeT1);
        }
    }

    private void evictResident(Prefix v) {
        v.remove();
        policyStats.recordOperation();
        if (v.q == Q.T1) {
            v.q = Q.B1;
            v.appendToTail(headB1);
            policyStats.recordOperation();
            sizeT1 -= v.size;
            sizeB1 += v.size;
        } else {
            v.q = Q.B2;
            v.appendToTail(headB2);
            policyStats.recordOperation();
            sizeT2 -= v.size;
            sizeB2 += v.size;
        }
        policyStats.recordEviction();
    }

    private void evictGhost(Prefix g) {
        g.remove();
        data.remove(g.key);
        if (g.q == Q.B1) sizeB1 -= g.size;
        else sizeB2 -= g.size;
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    @Override
    public void finished() {
        policyStats.setPercentAdaption((sizeT1 / (double) maximumCacheSize) - 0.5);
        checkState(sizeT1 + sizeT2 <= maximumCacheSize);
    }

    @Override
    public String name() {
        return Policy.super.name();
    }

    static final class Prefix {
        final long key;
        final long size;

        Prefix prev, next;
        Q q;

        Prefix(long size) {
            this(Long.MIN_VALUE, size);
        }

        Prefix(long key, long size) {
            this.key = key;
            this.size = size;
            this.prev = this.next = this;
        }

        void appendToTail(Prefix head) {
            Prefix tail = head.prev;
            tail.next = head.prev = this;
            this.prev = tail;
            this.next = head;
        }

        void remove() {
            prev.next = next;
            next.prev = prev;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("key", key).add("size", size).add("q", q).toString();
        }
    }
}
