package com.github.benmanes.caffeine.cache.simulator.policy.prefix.source_based;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.ChunkManager;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.SourceBasedChunkManager;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateUnderflowDelay;
import static com.google.common.base.Preconditions.checkState;

@Policy.PolicySpec(name = "prefix.source-based.Arc")
public final class PSArcPolicy implements Policy {

  private enum Q {T1, B1, T2, B2}

  private final Prefix headT1 = new Prefix(0);
  private final Prefix headT2 = new Prefix(0);
  private final Prefix headB1 = new Prefix(0);
  private final Prefix headB2 = new Prefix(0);

  private final Long2ObjectMap<Prefix> data = new Long2ObjectOpenHashMap<>();
  private final PolicyStats policyStats = new PolicyStats(name());

  private final long maximumCacheSize;
  private final ChunkManager chunk_manager;

  /** ARC target size, in bytes, for the recency partition T1. */
  private long p;

  private long sizeT1;
  private long sizeT2;
  private long sizeB1;
  private long sizeB2;

  @SuppressWarnings("unused")
  private int currentTime;

  public PSArcPolicy(Config cfg) {
    maximumCacheSize = new BasicSettings(cfg).maximumSize();
    chunk_manager = new SourceBasedChunkManager();
    p = 0;
  }

  @Override
  public void record(AccessEvent event) {
    currentTime++;
    policyStats.recordOperation();

    switch (event.operation()) {
      case READ:
        onRead(event, true);
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
    removeEntry(data.get(event.key()), true);
    onRead(event, false);
  }

  private void onDelete(AccessEvent event) {
    removeEntry(data.get(event.key()), true);
  }

  private void onRead(AccessEvent event, boolean recordReadStats) {
    Prefix prefix = data.get(event.key());

    if (recordReadStats) {
      long cachedSize = isResident(prefix) ? prefix.size : 0L;
      recordStats(event.itemSize(), cachedSize, event.retrievalDelay());
    }

    long prefixSize = Math.min(
      event.itemSize(),
      chunk_manager.getChunkSize(event.key(), event.itemSize()));

    if (prefix == null) {
      if (recordReadStats) {
        policyStats.recordMiss();
      }
      onColdMiss(event.key(), prefixSize);
      return;
    }

    switch (prefix.q) {
      case T1:
      case T2:
        if (recordReadStats) {
          policyStats.recordHit();
        }
        onResidentHit(prefix);
        break;

      case B1:
        if (recordReadStats) {
          policyStats.recordMiss();
        }
        onGhostHitB1(prefix);
        break;

      case B2:
        if (recordReadStats) {
          policyStats.recordMiss();
        }
        onGhostHitB2(prefix);
        break;

      default:
        throw new IllegalStateException("Unknown ARC queue: " + prefix.q);
    }
  }

  private void recordStats(long fullItemSize, long cachedSize, double retrievalDelay) {
    double delay = calculateUnderflowDelay(
      retrievalDelay, fullItemSize, cachedSize, Consts.BANDWIDTH);
    policyStats.addDelay(delay);

    double latency = calculateLatency(
      retrievalDelay, fullItemSize, cachedSize, Consts.BANDWIDTH);
    policyStats.addLatency(latency);
  }

  private void onResidentHit(Prefix n) {
    if (n.q == Q.T2) {
      n.remove();
      n.appendToTail(headT2);
      return;
    }

    sizeT1 -= n.size;
    sizeT2 += n.size;

    n.remove();
    n.q = Q.T2;
    n.appendToTail(headT2);
  }

  private void onGhostHitB1(Prefix n) {
    increaseP(n.size);

    unlinkGhost(n);

    if (!makeResidentSpace(n.size, Q.B1)) {
      data.remove(n.key);
      return;
    }

    n.q = Q.T2;
    n.appendToTail(headT2);
    sizeT2 += n.size;

    trimGhosts();
  }

  private void onGhostHitB2(Prefix n) {
    decreaseP(n.size);

    unlinkGhost(n);

    if (!makeResidentSpace(n.size, Q.B2)) {
      data.remove(n.key);
      return;
    }

    n.q = Q.T2;
    n.appendToTail(headT2);
    sizeT2 += n.size;

    trimGhosts();
  }

  private void onColdMiss(long itemKey, long itemSize) {
    if ((maximumCacheSize == 0) || (itemSize <= 0) || (itemSize > maximumCacheSize)) {
      return;
    }

    if (!makeResidentSpace(itemSize, null)) {
      return;
    }

    Prefix n = new Prefix(itemKey, itemSize);
    n.q = Q.T1;
    n.appendToTail(headT1);

    data.put(itemKey, n);
    sizeT1 += itemSize;

    trimGhosts();
  }

  /**
   * Frees resident cache space until {@code needed} bytes can be inserted.
   *
   * <p>The ARC replacement rule uses {@code p} as the byte target for T1.
   * If the incoming access came from B2, ARC gives extra pressure to evict
   * from T1 when T1 is at/above {@code p}.
   */
  private boolean makeResidentSpace(long needed, Q incomingGhostQueue) {
    if ((needed <= 0) || (needed > maximumCacheSize)) {
      return false;
    }

    while (residentSize() + needed > maximumCacheSize) {
      if (!replace(incomingGhostQueue)) {
        return false;
      }
    }

    return true;
  }

  private boolean replace(Q incomingGhostQueue) {
    if (residentSize() == 0) {
      return false;
    }

    boolean evictFromT1;
    if (isEmpty(headT1)) {
      evictFromT1 = false;
    } else if (isEmpty(headT2)) {
      evictFromT1 = true;
    } else {
      evictFromT1 = (sizeT1 > p)
        || ((incomingGhostQueue == Q.B2) && (sizeT1 >= p));
    }

    Prefix victim = evictFromT1 ? headT1.next : headT2.next;
    evictResident(victim);
    return true;
  }

  private void evictResident(Prefix v) {
    v.remove();

    if (v.q == Q.T1) {
      sizeT1 -= v.size;
      sizeB1 += v.size;

      v.q = Q.B1;
      v.appendToTail(headB1);
    } else if (v.q == Q.T2) {
      sizeT2 -= v.size;
      sizeB2 += v.size;

      v.q = Q.B2;
      v.appendToTail(headB2);
    } else {
      throw new IllegalStateException("Cannot evict non-resident entry: " + v);
    }

    policyStats.recordEviction();
  }

  private void unlinkGhost(Prefix n) {
    n.remove();

    if (n.q == Q.B1) {
      sizeB1 -= n.size;
    } else if (n.q == Q.B2) {
      sizeB2 -= n.size;
    } else {
      throw new IllegalStateException("Not a ghost entry: " + n);
    }
  }

  private void evictGhost(Prefix g) {
    g.remove();
    data.remove(g.key);

    if (g.q == Q.B1) {
      sizeB1 -= g.size;
    } else if (g.q == Q.B2) {
      sizeB2 -= g.size;
    } else {
      throw new IllegalStateException("Cannot evict non-ghost entry: " + g);
    }
  }

  private void removeEntry(Prefix n, boolean countResidentEviction) {
    if (n == null) {
      return;
    }

    n.remove();
    data.remove(n.key);

    if (n.q == Q.T1) {
      sizeT1 -= n.size;
      if (countResidentEviction) {
        policyStats.recordEviction();
      }
    } else if (n.q == Q.T2) {
      sizeT2 -= n.size;
      if (countResidentEviction) {
        policyStats.recordEviction();
      }
    } else if (n.q == Q.B1) {
      sizeB1 -= n.size;
    } else if (n.q == Q.B2) {
      sizeB2 -= n.size;
    } else {
      throw new IllegalStateException("Unknown ARC queue: " + n.q);
    }
  }

  /**
   * Keeps ARC's directory bounded. Resident bytes are bounded by C, and
   * resident + ghost metadata is bounded by approximately 2C.
   */
  private void trimGhosts() {
    while ((sizeT1 + sizeB1 > maximumCacheSize) && !isEmpty(headB1)) {
      evictGhost(headB1.next);
    }

    while ((sizeT2 + sizeB2 > maximumCacheSize) && !isEmpty(headB2)) {
      evictGhost(headB2.next);
    }

    long directoryLimit = (maximumCacheSize > Long.MAX_VALUE / 2)
      ? Long.MAX_VALUE
      : 2 * maximumCacheSize;

    while (directorySize() > directoryLimit) {
      if (!isEmpty(headB2)) {
        evictGhost(headB2.next);
      } else if (!isEmpty(headB1)) {
        evictGhost(headB1.next);
      } else {
        break;
      }
    }
  }

  private void increaseP(long unitSize) {
    long delta = adaptiveDelta(sizeB2, sizeB1, unitSize);
    p = Math.min(maximumCacheSize, saturatedAdd(p, delta));
  }

  private void decreaseP(long unitSize) {
    long delta = adaptiveDelta(sizeB1, sizeB2, unitSize);
    p = (delta >= p) ? 0 : (p - delta);
  }

  /**
   * Byte-sized version of ARC's:
   *
   * <pre>
   * max(|B_other| / |B_current|, 1)
   * </pre>
   *
   * scaled by the current prefix size.
   */
  private static long adaptiveDelta(long otherGhostSize, long currentGhostSize, long unitSize) {
    if ((unitSize <= 0) || (currentGhostSize <= 0)) {
      return Math.max(1L, unitSize);
    }

    double scaled = ((double) otherGhostSize / (double) currentGhostSize) * (double) unitSize;
    if (scaled >= Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }

    long delta = (long) Math.ceil(scaled);
    return Math.max(unitSize, Math.max(1L, delta));
  }

  private static long saturatedAdd(long a, long b) {
    long r = a + b;
    if (((a ^ r) & (b ^ r)) < 0) {
      return Long.MAX_VALUE;
    }
    return r;
  }

  private static boolean isResident(Prefix n) {
    return (n != null) && ((n.q == Q.T1) || (n.q == Q.T2));
  }

  private static boolean isEmpty(Prefix head) {
    return head.next == head;
  }

  private long residentSize() {
    return sizeT1 + sizeT2;
  }

  private long directorySize() {
    return sizeT1 + sizeT2 + sizeB1 + sizeB2;
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  @Override
  public void finished() {
    policyStats.setPercentAdaption(
      maximumCacheSize == 0 ? 0.0 : (sizeT1 / (double) maximumCacheSize) - 0.5);

    checkState(sizeT1 >= 0);
    checkState(sizeT2 >= 0);
    checkState(sizeB1 >= 0);
    checkState(sizeB2 >= 0);
    checkState(sizeT1 + sizeT2 <= maximumCacheSize);
  }

  @Override
  public String name() {
    return Policy.super.name();
  }

  static final class Prefix {
    final long key;
    final long size;

    Prefix prev;
    Prefix next;
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
        .add("key", key)
        .add("size", size)
        .add("q", q)
        .toString();
    }
  }
}
