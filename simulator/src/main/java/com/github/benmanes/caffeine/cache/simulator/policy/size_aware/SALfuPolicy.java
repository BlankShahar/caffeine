package com.github.benmanes.caffeine.cache.simulator.policy.size_aware;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.typesafe.config.Config;

import static com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent.Operation.READ;
import static com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations.calculateLatency;


@Policy.PolicySpec(name = "size-aware.LFU")
public final class SALfuPolicy implements Policy {
  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Item> minHeap;
  final long maximumCacheSize;
  long currentCacheSize;

  public SALfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());
    this.minHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::compareItems);
    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0;
  }

  @Override
  public void record(AccessEvent event) {
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
    var existingPrefix = minHeap.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      minHeap.remove(existingPrefix.key);
      currentCacheSize -= existingPrefix.size;
      policyStats.recordEviction();
      policyStats.recordOperation();
    }
  }

  private void onRead(AccessEvent event) {
    long itemKey = event.key();
    long itemSize = event.itemSize();
    double retrievalDelay = event.retrievalDelay();

    policyStats.recordOperation();

    Item item = minHeap.get(itemKey);
    if (item != null) {
      // Hit
      policyStats.recordHit();
      item.frequency++;
      minHeap.upsert(itemKey, item);

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
      currentCacheSize += itemSize;
      policyStats.recordAdmission();
    }
  }

  public int compareItems(long itemKey1, long itemKey2) {
    Item p1 = minHeap.get(itemKey1);
    Item p2 = minHeap.get(itemKey2);
    assert p1 != null && p2 != null;
    return Long.compare(p1.frequency, p2.frequency);
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
