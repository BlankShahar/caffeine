package com.github.benmanes.caffeine.cache.simulator.policy.size_aware;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import static com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations.calculateLatency;
import static com.google.common.base.Preconditions.checkState;

@Policy.PolicySpec(name = "size-aware.Arc")
public final class SAArcPolicy implements Policy {

  private enum Q {T1, B1, T2, B2}

  private final Node headT1 = new Node(0);
  private final Node headT2 = new Node(0);
  private final Node headB1 = new Node(0);
  private final Node headB2 = new Node(0);

  private final Long2ObjectMap<Node> data = new Long2ObjectOpenHashMap<>();
  private final PolicyStats stats = new PolicyStats(name());
  private final long maximumCacheSize;
  private long p;

  private long sizeT1, sizeT2, sizeB1, sizeB2;
  private int currentTime = 0;

  public SAArcPolicy(Config cfg) {
    maximumCacheSize = new BasicSettings(cfg).maximumSize();
    p = 0;
  }

  @Override
  public void record(AccessEvent e) {
    currentTime++;
    stats.recordOperation();

    Node n = data.get(e.key());
    if (n == null) {
      onMiss(e);
      return;
    }

    if (n.q == Q.T1 || n.q == Q.T2) {
      onHit(n, e);
    } else if (n.q == Q.B1) {
      onHitB1(n, e);
    } else if (n.q == Q.B2) {
      onHitB2(n, e);
    }
  }

  private void onHit(Node n, AccessEvent e) {
    if (n.q == Q.T1) {
      sizeT1 -= n.size;
      sizeT2 += n.size;
    }
    n.remove();
    n.q = Q.T2;
    n.appendToTail(headT2);

    stats.recordHit();
    double latency = calculateLatency(
      e.retrievalDelay(),
      e.itemSize() * Consts.CHUNK_SIZE,
      e.itemSize() * Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    stats.addLatency(latency);
  }

  private void onHitB1(Node n, AccessEvent e) {
    stats.recordMiss();
    stats.addDelay(e.retrievalDelay());
    double latency = calculateLatency(e.retrievalDelay(), e.itemSize() * Consts.CHUNK_SIZE, 0, Consts.BANDWIDTH);
    stats.addLatency(latency);

    p = Math.min(maximumCacheSize, p + n.size);
    if (n.size <= (maximumCacheSize - sizeT1)) {
      evictToMakeSpace(Q.T2, n.size);
      moveFromGhostToT2(n);
    } else {
      evictGhost(n);
    }
  }

  private void onHitB2(Node n, AccessEvent e) {
    stats.recordMiss();
    stats.addDelay(e.retrievalDelay());
    double latency = calculateLatency(e.retrievalDelay(), e.itemSize() * Consts.CHUNK_SIZE,0, Consts.BANDWIDTH);
    stats.addLatency(latency);

    p = Math.max(0, p - n.size);
    if (n.size <= (maximumCacheSize - sizeT1)) {
      evictToMakeSpace(Q.T2, n.size);
      moveFromGhostToT2(n);
    } else {
      evictGhost(n);
    }
  }

  private void moveFromGhostToT2(Node n) {
    if (n.q == Q.B1) sizeB1 -= n.size;
    else sizeB2 -= n.size;
    n.remove();
    n.q = Q.T2;
    n.appendToTail(headT2);
    sizeT2 += n.size;
  }

  private void onMiss(AccessEvent event) {
    long size = event.itemSize(); // (long) Math.ceil(e.itemSize() / (Consts.CHUNK_SIZE * 1024 * 1024));
    stats.recordMiss();
    stats.addDelay(event.retrievalDelay());

    double latency = calculateLatency(event.retrievalDelay(), event.itemSize() * Consts.CHUNK_SIZE, 0, Consts.BANDWIDTH);
    stats.addLatency(latency);

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
      Node n = new Node(event.key(), size);
      n.q = Q.T1;
      n.appendToTail(headT1);
      data.put(event.key(), n);
      sizeT1 += size;
    }
  }

  private void evictToMakeSpace(Q target, long needed) {
    long available = (target == Q.T1) ? (maximumCacheSize - sizeT2) : (maximumCacheSize - sizeT1);
    long usage = (target == Q.T1) ? sizeT1 : sizeT2;

    while (usage + needed > available) {
      Node victim = (target == Q.T1) ? headT1.next : headT2.next;
      if (victim == victim.next || victim.size == 0) break;
      evictResident(victim);
      usage = (target == Q.T1) ? sizeT1 : sizeT2;
      available = (target == Q.T1) ? (maximumCacheSize - sizeT2) : (maximumCacheSize - sizeT1);
    }
  }

  private void evictResident(Node v) {
    v.remove();
    if (v.q == Q.T1) {
      v.q = Q.B1;
      v.appendToTail(headB1);
      sizeT1 -= v.size;
      sizeB1 += v.size;
    } else {
      v.q = Q.B2;
      v.appendToTail(headB2);
      sizeT2 -= v.size;
      sizeB2 += v.size;
    }
    stats.recordEviction();
  }

  private void evictGhost(Node g) {
    g.remove();
    data.remove(g.key);
    if (g.q == Q.B1) sizeB1 -= g.size;
    else sizeB2 -= g.size;
  }

  @Override
  public PolicyStats stats() {
    return stats;
  }

  @Override
  public void finished() {
    stats.setPercentAdaption((sizeT1 / (double) maximumCacheSize) - 0.5);
    checkState(sizeT1 + sizeT2 <= maximumCacheSize);
  }

  @Override
  public String name() {
    return Policy.super.name();
  }

  static final class Node {
    final long key;
    final long size;

    Node prev, next;
    Q q;

    Node(long size) {
      this(Long.MIN_VALUE, size);
    }

    Node(long key, long size) {
      this.key = key;
      this.size = size;
      this.prev = this.next = this;
    }

    void appendToTail(Node head) {
      Node tail = head.prev;
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
