package com.github.benmanes.caffeine.cache.simulator.policy.size_aware;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.NormalSource;
import com.typesafe.config.Config;

import static com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent.Operation.READ;
import static com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations.calculateLatency;

@Policy.PolicySpec(name = "size-aware.LRU")
public final class SALruPolicy implements Policy {
  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Item> minHeap;
  final long maximumCacheSize;
  long currentCacheSize;
  long currentTime;

  public SALruPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());
    this.minHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::compareItems);
    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0;
    this.currentTime = 0;
  }

  @Override
  public void record(AccessEvent event) {
    currentTime++;

    if (Consts.CHUNK_SIZE > 0) {
      NormalSource source = Consts.SOURCES.get((int) (event.key() % Consts.SOURCES.size()));
      event.itemSize = Math.min(source.chunkSize, event.itemSize);
    }
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

  private void onRead(AccessEvent event) {
    long itemKey = event.key();
    long itemSize = event.itemSize();
    double retrievalDelay = event.retrievalDelay();

    Item item = minHeap.get(itemKey);
    if (item != null) {
      // Hit
      policyStats.recordHit();
      item.lastAccessTime = currentTime;
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

      if (itemSize > maximumCacheSize) {
        return; // Item is too large to fit the cache
      }

      while (currentCacheSize + itemSize > maximumCacheSize && !minHeap.isEmpty()) {
        Item victim = minHeap.extractMin().value();
        currentCacheSize -= victim.size;
        policyStats.recordEviction();
      }

      Item newItem = new Item(itemKey, itemSize, currentTime);
      minHeap.upsert(itemKey, newItem);
      policyStats.recordOperation();
      currentCacheSize += itemSize;
      policyStats.recordAdmission();
    }
  }

  public int compareItems(long itemKey1, long itemKey2) {
    Item p1 = minHeap.get(itemKey1);
    Item p2 = minHeap.get(itemKey2);
    assert p1 != null && p2 != null;
    return Long.compare(p1.lastAccessTime, p2.lastAccessTime);
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

  static final class Item {
    final long key;
    final long size;
    long lastAccessTime;

    Item(long key, long size, long accessTime) {
      this.key = key;
      this.size = size;
      this.lastAccessTime = accessTime;
    }
  }
}
