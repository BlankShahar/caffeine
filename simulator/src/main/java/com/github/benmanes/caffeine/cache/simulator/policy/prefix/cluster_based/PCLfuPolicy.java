package com.github.benmanes.caffeine.cache.simulator.policy.prefix.cluster_based;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.countmin4.PeriodicResetCountMin4;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.ClusterBasedChunkManager;
import com.github.benmanes.caffeine.cache.simulator.policy.size_aware.SearchableMinHeap;
import com.typesafe.config.Config;

import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateUnderflowDelay;


@Policy.PolicySpec(name = "prefix.cluster-based.LFU")
public final class PCLfuPolicy implements Policy {
  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Prefix> minHeap;
  private final PeriodicResetCountMin4 sketch;
  final long maximumCacheSize;
  long currentCacheSize;
  int currentTime;
  final ClusterBasedChunkManager chunk_manager;
  final double RESIZE_RATIO = 0.05;

  public PCLfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());
    this.minHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::compareItems);
    this.sketch = new PeriodicResetCountMin4(settings.config());
    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0;
    this.currentTime = 0;
    this.chunk_manager = new ClusterBasedChunkManager();
  }

  @Override
  public void record(AccessEvent event) {
    currentTime++;
    if (currentTime % sketch.period == 0) minHeap.makeHeap();

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
    var existingPrefix = minHeap.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      minHeap.remove(existingPrefix.key);
      policyStats.recordOperation();
      currentCacheSize -= existingPrefix.size;
      policyStats.recordEviction();
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

  private void recordStats(long fullItemSize, long cachedSize, double retrievalDelay) {
    double delay = calculateUnderflowDelay(retrievalDelay, fullItemSize, cachedSize, Consts.BANDWIDTH);
    policyStats.addDelay(delay);

    double latency = calculateLatency(retrievalDelay, fullItemSize, cachedSize, Consts.BANDWIDTH);
    policyStats.addLatency(latency);
  }

  private void onRead(AccessEvent event) {
    if (currentCacheSize >= maximumCacheSize) sketch.ensureCapacity(2_000_000);
    handleRequestsFrequency(event.key());

    long itemKey = event.key();

    Prefix prefix = minHeap.get(itemKey);
    if (event.operation() == AccessEvent.Operation.READ)
      recordStats(event.itemSize(), prefix != null ? prefix.size : 0, event.retrievalDelay());

    event.itemSize = Math.min(event.itemSize(), chunk_manager.getChunkSize(event.key(), event.itemSize()));
    long size = event.itemSize();
    chunk_manager.addDelay(event.key(), event.retrievalDelay());

    if (prefix != null) {
      // Hit
      policyStats.recordHit();
      if (size >= prefix.size * (1 + RESIZE_RATIO) || size <= prefix.size * (1 - RESIZE_RATIO))
        resizePrefix(prefix, size);
      minHeap.upsert(itemKey, prefix);
      policyStats.recordOperation();

    } else {
      // Miss
      policyStats.recordMiss();

      // There's no enough space in the cache to insert the item
      if (size > maximumCacheSize) {
        return;
      }

      // Evict items until there's enough space
      while (currentCacheSize + size > maximumCacheSize && !minHeap.isEmpty()) {
        Prefix victim = minHeap.extractMin().value();
        currentCacheSize -= victim.size;
        policyStats.recordEviction();
      }

      // Insert new item
      Prefix newPrefix = new Prefix(itemKey, size);
      minHeap.upsert(itemKey, newPrefix);
      policyStats.recordOperation();
      currentCacheSize += size;
      policyStats.recordAdmission();
    }
  }

  public int compareItems(long itemKey1, long itemKey2) {
    return Long.compare(sketch.frequency(itemKey1), sketch.frequency(itemKey2));
  }

  private void resizePrefix(Prefix prefix, long newSize) {
    if (newSize == 0) { // shrink to zero -> remove
      minHeap.remove(prefix.key);
      currentCacheSize -= prefix.size;
      policyStats.recordOperation();
      policyStats.recordEviction();
    }

    if (newSize < prefix.size) { // shrink
      long delta = prefix.size - newSize;
      prefix.size = newSize;
      currentCacheSize -= delta;
    }

    if (newSize > prefix.size) { // grow
      long delta = newSize - prefix.size;
      while (currentCacheSize + delta > maximumCacheSize && !minHeap.isEmpty()) { // make space
        Prefix victim = minHeap.extractMin().value();
        currentCacheSize -= victim.size;
        policyStats.recordEviction();
      }
      prefix.size = newSize;
      currentCacheSize += delta;
    }
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

  static public class Prefix {
    public final long key;
    public long size;
    public long frequency;

    public Prefix(long key, long size) {
      this.key = key;
      this.size = size;
      this.frequency = 1;
    }
  }
}
