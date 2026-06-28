package com.github.benmanes.caffeine.cache.simulator.policy.prefix.competitor;

import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateUnderflowDelay;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.typesafe.config.Config;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/**
 * Segment-Based Proxy Caching of Multimedia Streams (Wu, Yu, Wolf).
 *
 * <p>The implementation keeps the paper's key invariants: an object is cached only as a contiguous
 * prefix; the first {@code Kmin} exponential segments are managed as a protected initial unit; later
 * segments are admitted only if their value exceeds a low-valued victim, where value is estimated as
 * reference-frequency divided by segment distance.</p>
 */
@Policy.PolicySpec(name = "prefix.competitor.SegmentBased")
public final class SegmentBasedPolicy implements Policy {
  private final Long2ObjectOpenHashMap<CachedObject> data;
  private final LinkedHashMap<Long, CachedObject> initialLru;
  private final LinkedHashMap<Long, CachedObject> laterLru;
  private final PolicyStats policyStats;

  private final long maximumSize;
  private final long initialMaximumSize;
  private final long laterMaximumSize;
  private final long blockSize;
  private final int kMin;
  private final int maxVictimScan;

  private long currentInitialSize;
  private long currentLaterSize;
  private long sequenceTime;
  private long delayedStarts;

  public SegmentBasedPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.maximumSize = settings.maximumSize();
    this.policyStats = new PolicyStats(name());
    this.policyStats.addMetric("Delayed Starts", () -> delayedStarts);
    this.policyStats.addPercentMetric("Delayed Start Rate", () ->
        (policyStats.requestCount() == 0) ? 0.0 : (double) delayedStarts / policyStats.requestCount());
    this.policyStats.addMetric("Initial Bytes", () -> currentInitialSize);
    this.policyStats.addMetric("Later Bytes", () -> currentLaterSize);

    this.blockSize = config.getLong("prefix.segment-based.block-size");
    this.kMin = config.getInt("prefix.segment-based.k-min");
    this.maxVictimScan = config.getInt("prefix.segment-based.max-victim-scan");
    double initialRatio = config.getDouble("prefix.segment-based.initial-cache-ratio");
    this.initialMaximumSize = Math.max(0L, Math.round(maximumSize * initialRatio));
    this.laterMaximumSize = Math.max(0L, maximumSize - initialMaximumSize);

    this.data = new Long2ObjectOpenHashMap<>();
    this.initialLru = new LinkedHashMap<>(16, 0.75f, true);
    this.laterLru = new LinkedHashMap<>(16, 0.75f, true);
  }

  @Override
  public void record(AccessEvent event) {
    sequenceTime++;
    switch (event.operation()) {
      case READ:
        onRead(event);
        break;
      case WRITE:
        onWrite(event);
        break;
      case DELETE:
        onDelete(event.key());
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + event.operation());
    }
  }

  private void onWrite(AccessEvent event) {
    onDelete(event.key());
    onRead(event);
  }

  private void onRead(AccessEvent event) {
    long key = event.key();
    long now = now(event);
    long itemSize = event.itemSize();
    CachedObject object = data.get(key);
    long cachedBytes = (object == null) ? 0L : object.cachedBytes();
    boolean hasCachedPrefix = cachedBytes > 0L;

    recordDelayStats(itemSize, cachedBytes, event.retrievalDelay());
    if (!hasCachedPrefix) {
      policyStats.recordMiss();
      delayedStarts++;
    } else {
      policyStats.recordHit();
      object.itemSize = Math.max(object.itemSize, itemSize);
      initialLru.get(key);
      if (object.hasLaterSegments()) {
        laterLru.get(key);
      }
    }

    if (object == null) {
      object = new CachedObject(key, itemSize);
      data.put(key, object);
    }

    int firstMissing = object.largestSegment + 1;
    int lastSegment = lastSegment(itemSize);
    for (int segment = firstMissing; segment <= lastSegment; segment++) {
      if ((segment > 0) && (object.largestSegment != (segment - 1))) {
        break; // Preserve contiguous-prefix invariant.
      }
      if (segment < kMin) {
        admitInitialUnit(object);
      } else {
        admitLaterSegment(object, segment, now);
      }
    }

    object.lastRequestTime = now;
    if (object.cachedBytes() == 0L) {
      data.remove(object.key);
    }
    policyStats.recordOperation();
  }

  private void admitInitialUnit(CachedObject object) {
    long initialBytes = initialUnitBytes(object.itemSize);
    if (initialBytes == 0L || initialBytes > initialMaximumSize) {
      return;
    }
    if (object.initialBytes > 0L) {
      object.largestSegment = Math.max(object.largestSegment, Math.min(kMin - 1, lastSegment(object.itemSize)));
      initialLru.get(object.key);
      return;
    }
    while ((currentInitialSize + initialBytes) > initialMaximumSize && !initialLru.isEmpty()) {
      evictInitialVictim();
    }
    if ((currentInitialSize + initialBytes) > initialMaximumSize) {
      return;
    }
    object.initialBytes = initialBytes;
    object.largestSegment = Math.max(object.largestSegment, Math.min(kMin - 1, lastSegment(object.itemSize)));
    initialLru.put(object.key, object);
    currentInitialSize += initialBytes;
    policyStats.recordAdmission();
  }

  private void admitLaterSegment(CachedObject object, int segment, long now) {
    if (object.initialBytes == 0L || object.lastRequestTime < 0L) {
      return; // Later segments are not eligible on first reference.
    }
    long segmentBytes = segmentBytes(segment, object.itemSize);
    if (segmentBytes == 0L || segmentBytes > laterMaximumSize) {
      return;
    }
    double candidateValue = value(object, segment, now);
    while ((currentLaterSize + segmentBytes) > laterMaximumSize) {
      CachedObject victim = findLaterVictim(now);
      if (victim == null || !hasLowerValuedVictim(candidateValue, victim, now)) {
        policyStats.recordRejection();
        return;
      }
      evictLargestLaterSegment(victim);
    }
    object.laterBytes += segmentBytes;
    object.largestSegment = segment;
    laterLru.put(object.key, object);
    currentLaterSize += segmentBytes;
    policyStats.recordAdmission();
  }

  private CachedObject findLaterVictim(long now) {
    Iterator<Map.Entry<Long, CachedObject>> iterator = laterLru.entrySet().iterator();
    CachedObject best = null;
    double bestValue = Double.POSITIVE_INFINITY;
    int scanned = 0;
    while (iterator.hasNext() && scanned++ < maxVictimScan) {
      CachedObject candidate = iterator.next().getValue();
      double value = value(candidate, candidate.largestSegment, now);
      if (value < bestValue) {
        best = candidate;
        bestValue = value;
      }
    }
    return best;
  }

  private boolean hasLowerValuedVictim(double candidateValue, CachedObject victim, long now) {
    return candidateValue > value(victim, victim.largestSegment, now);
  }

  private void evictInitialVictim() {
    CachedObject victim = initialLru.entrySet().iterator().next().getValue();
    removeObject(victim.key);
  }

  private void evictLargestLaterSegment(CachedObject victim) {
    long bytes = segmentBytes(victim.largestSegment, victim.itemSize);
    victim.laterBytes -= bytes;
    currentLaterSize -= bytes;
    policyStats.recordEviction();

    victim.largestSegment--;
    if (victim.largestSegment < kMin) {
      laterLru.remove(victim.key);
      victim.largestSegment = Math.min(kMin - 1, lastSegment(victim.itemSize));
      victim.laterBytes = 0L;
    } else {
      laterLru.put(victim.key, victim);
    }
  }

  private void onDelete(long key) {
    removeObject(key);
  }

  private void removeObject(long key) {
    CachedObject object = data.remove(key);
    if (object == null) {
      return;
    }
    initialLru.remove(key);
    laterLru.remove(key);
    currentInitialSize -= object.initialBytes;
    currentLaterSize -= object.laterBytes;
    policyStats.recordEviction();
    policyStats.recordOperation();
  }

  private void recordDelayStats(long itemSize, long cachedBytes, double retrievalDelay) {
    double delay = calculateUnderflowDelay(retrievalDelay, itemSize, cachedBytes, Consts.BANDWIDTH);
    double latency = calculateLatency(retrievalDelay, itemSize, cachedBytes, Consts.BANDWIDTH);
    policyStats.addDelay(delay);
    policyStats.addLatency(latency);
  }

  private double value(CachedObject object, int segment, long now) {
    long elapsed = Math.max(1L, now - Math.max(0L, object.lastRequestTime));
    int distance = Math.max(1, segment);
    return 1.0 / (elapsed * (double) distance);
  }

  private long now(AccessEvent event) {
    return (event.timestamp() >= 0L) ? event.timestamp() : sequenceTime;
  }

  private long initialUnitBytes(long itemSize) {
    return Math.min(itemSize, bytesThroughSegment(kMin - 1, itemSize));
  }

  private long segmentBytes(int segment, long itemSize) {
    return bytesThroughSegment(segment, itemSize) - bytesThroughSegment(segment - 1, itemSize);
  }

  private long bytesThroughSegment(int segment, long itemSize) {
    if (segment < 0) {
      return 0L;
    }
    long blocks = (segment == 0) ? 1L : saturatedShift(segment);
    return Math.min(itemSize, multiplySaturated(blocks, blockSize));
  }

  private int lastSegment(long itemSize) {
    if (itemSize <= blockSize) {
      return 0;
    }
    long blocks = (itemSize + blockSize - 1) / blockSize;
    return 64 - Long.numberOfLeadingZeros(blocks - 1);
  }

  private long saturatedShift(int shift) {
    return (shift >= 62) ? Long.MAX_VALUE : (1L << shift);
  }

  private long multiplySaturated(long a, long b) {
    if (a == 0L || b == 0L) {
      return 0L;
    }
    if (a > Long.MAX_VALUE / b) {
      return Long.MAX_VALUE;
    }
    return a * b;
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  private static final class CachedObject {
    final long key;
    long lastRequestTime = -1L;
    long initialBytes;
    long laterBytes;
    long itemSize;
    int largestSegment = -1;

    CachedObject(long key, long itemSize) {
      this.key = key;
      this.itemSize = itemSize;
    }

    long cachedBytes() {
      return initialBytes + laterBytes;
    }

    boolean hasLaterSegments() {
      return laterBytes > 0L;
    }
  }
}
