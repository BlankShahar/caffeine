package com.github.benmanes.caffeine.cache.simulator.policy.size_aware;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.countmin4.PeriodicResetCountMin4;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.typesafe.config.Config;

import static com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent.Operation.READ;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;


@Policy.PolicySpec(name = "size-aware.LFU")
public final class SALfuPolicy implements Policy {
  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Item> minHeap;
  private final PeriodicResetCountMin4 sketch;
  final long maximumCacheSize;
  long currentCacheSize;
  int currentTime;

  public SALfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());
    this.minHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::compareItems);
    this.sketch = new PeriodicResetCountMin4(settings.config());
    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0;
    this.currentTime = 0;
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
    Item prefix = minHeap.get(itemKey);

    if (prefix != null) {
      prefix.frequency = sketch.frequency(itemKey);
      minHeap.upsert(itemKey, prefix);
      policyStats.recordOperation();
    }
  }

  private void onRead(AccessEvent event) {
    if (currentCacheSize >= maximumCacheSize)
      sketch.ensureCapacity(2_000_000);
    handleRequestsFrequency(event.key());

    long itemKey = event.key();
    long itemSize = event.itemSize();
    double retrievalDelay = event.retrievalDelay();

    Item item = minHeap.get(itemKey);
    if (item != null) {
      // Hit
      policyStats.recordHit();
      minHeap.upsert(itemKey, item);
      policyStats.recordOperation();

      if (event.operation() == READ) {
        double latency = calculateLatency(
          event.retrievalDelay(),
          event.itemSize(),
          event.itemSize(),
          Consts.BANDWIDTH
        );
        policyStats.addLatency(latency);
      }

    } else {
      // Miss
      policyStats.recordMiss();

      if (event.operation() == READ) {
        policyStats.addDelay(retrievalDelay);
        double latency = calculateLatency(
          event.retrievalDelay(),
          event.itemSize(),
          0,
          Consts.BANDWIDTH
        );
        policyStats.addLatency(latency);
      }

      // There's no enough space in the cache to insert the item
      if (itemSize > maximumCacheSize) {
        return;
      }

      // Evict items until there's enough space
      while (currentCacheSize + itemSize > maximumCacheSize && !minHeap.isEmpty()) {
        Item victim = minHeap.extractMin().value();
        currentCacheSize -= victim.size;
        policyStats.recordEviction();
      }

      // Insert new item
      Item newItem = new Item(itemKey, itemSize);
      minHeap.upsert(itemKey, newItem);
      policyStats.recordOperation();
      currentCacheSize += itemSize;
      policyStats.recordAdmission();
    }
  }

  public int compareItems(long itemKey1, long itemKey2) {
    return Long.compare(sketch.frequency(itemKey1), sketch.frequency(itemKey2));
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

  static public class Item {
    public final long key;
    public final long size;
    public long frequency;

    public Item(long key, long size) {
      this.key = key;
      this.size = size;
      this.frequency = 1;
    }
  }
}
