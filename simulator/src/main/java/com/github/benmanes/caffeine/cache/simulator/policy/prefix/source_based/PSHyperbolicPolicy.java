package com.github.benmanes.caffeine.cache.simulator.policy.prefix.source_based;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.countmin4.PeriodicResetCountMin4;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.ChunkManager;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.SourceBasedChunkManager;
import com.github.benmanes.caffeine.cache.simulator.policy.size_aware.SearchableMinHeap;
import com.typesafe.config.Config;

import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateUnderflowDelay;

@Policy.PolicySpec(name = "prefix.source-based.Hyperbolic")
public final class PSHyperbolicPolicy implements Policy {
  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Prefix> minHeap;
  private final PeriodicResetCountMin4 sketch;
  final long maximumCacheSize;
  long currentCacheSize;
  long currentTime;
  final ChunkManager chunk_manager;

  public PSHyperbolicPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());
    this.minHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::compareItems);
    this.sketch = new PeriodicResetCountMin4(settings.config());
    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0;
    this.currentTime = 0;
    this.chunk_manager = new SourceBasedChunkManager();
  }

  @Override
  public void record(AccessEvent event) {
    currentTime++;
    if (currentTime % sketch.period == 0)
      minHeap.makeHeap();

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

  private void handleRequestsFrequency(long itemKey) {
    sketch.increment(itemKey);
    Prefix prefix = minHeap.get(itemKey);

    if (prefix != null) {
      prefix.frequency = sketch.frequency(itemKey);
      minHeap.upsert(itemKey, prefix);
      policyStats.recordOperation();
    }
  }

  private void onWrite(AccessEvent event) {
    onDelete(event);
    onRead(event);
  }

  private void onDelete(AccessEvent event) {
    var existingPrefix = minHeap.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      minHeap.remove(existingPrefix.key);
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
    if (currentCacheSize >= maximumCacheSize)
      sketch.ensureCapacity(2_000_000);
    handleRequestsFrequency(event.key());

    long itemKey = event.key();

    currentTime++;

    Prefix prefix = minHeap.get(itemKey);
    if (event.operation() == AccessEvent.Operation.READ)
      recordStats(event.itemSize(), prefix != null ? prefix.size : 0, event.retrievalDelay());

    event.itemSize = Math.min(event.itemSize(), chunk_manager.getChunkSize(event.key(), event.itemSize()));
    long itemSize = event.itemSize();

    if (prefix != null) {
      // Hit
      policyStats.recordHit();
      prefix.lastAccessTime = currentTime;
      minHeap.upsert(itemKey, prefix);
      policyStats.recordOperation();

    } else {
      // Miss
      policyStats.recordMiss();

      if (itemSize > maximumCacheSize) {
        return; // Item is too large to fit the cache
      }

      while (currentCacheSize + itemSize > maximumCacheSize && !minHeap.isEmpty()) {
        Prefix victim = minHeap.extractMin().value();
        currentCacheSize -= victim.size;
        policyStats.recordEviction();
      }

      Prefix newPrefix = new Prefix(itemKey, itemSize, currentTime);
      minHeap.upsert(itemKey, newPrefix);
      policyStats.recordOperation();
      currentCacheSize += itemSize;
      policyStats.recordAdmission();
    }
  }

  public int compareItems(long itemKey1, long itemKey2) {
    Prefix p1 = minHeap.get(itemKey1);
    Prefix p2 = minHeap.get(itemKey2);
    assert p1 != null && p2 != null;
    return Double.compare(p1.hyperbolicScore(currentTime), p2.hyperbolicScore(currentTime));
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

  static final class Prefix {
    final long key;
    final long size;
    long lastAccessTime;
    long frequency;

    Prefix(long key, long size, long accessTime) {
      this.key = key;
      this.size = size;
      this.lastAccessTime = accessTime;
      this.frequency = 1;
    }

    double hyperbolicScore(long currentTime) {
      double recency = 1.0 / (currentTime - lastAccessTime + 1);
      return frequency * recency;
    }
  }
}
