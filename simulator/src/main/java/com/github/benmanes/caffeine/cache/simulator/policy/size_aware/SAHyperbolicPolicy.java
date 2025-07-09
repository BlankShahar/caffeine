package com.github.benmanes.caffeine.cache.simulator.policy.size_aware;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.typesafe.config.Config;

import static com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations.calculateLatency;

@Policy.PolicySpec(name = "size-aware.Hyperbolic")
public final class SAHyperbolicPolicy implements Policy {
  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Item> minHeap;
  final long maximumCacheSize;
  long currentCacheSize;
  long currentTime;

  public SAHyperbolicPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());
    this.minHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::compareItems);
    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0;
    this.currentTime = 0;
  }

  @Override
  public void record(AccessEvent event) {
    long itemKey = event.key();
    long itemSize = event.itemSize(); // (long) Math.ceil(event.itemSize() / (Consts.CHUNK_SIZE * 1024 * 1024));
    double retrievalDelay = event.retrievalDelay();

    policyStats.recordOperation();
    currentTime++;

    Item item = minHeap.get(itemKey);
    if (item != null) {
      // Hit
      policyStats.recordHit();
      item.frequency++;
      item.lastAccessTime = currentTime;
      minHeap.upsert(itemKey, item);
      double latency = calculateLatency(
        event.retrievalDelay(),
        event.itemSize() * Consts.CHUNK_SIZE,
        event.itemSize() * Consts.CHUNK_SIZE,
        Consts.BANDWIDTH
      );
      policyStats.addLatency(latency);
    } else {
      // Miss
      policyStats.recordMiss();
      policyStats.addDelay(retrievalDelay);
      double latency = calculateLatency(
        event.retrievalDelay(),
        event.itemSize() * Consts.CHUNK_SIZE,
        0,
        Consts.BANDWIDTH
      );
      policyStats.addLatency(latency);

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
      currentCacheSize += itemSize;
      policyStats.recordAdmission();
    }
  }

  public int compareItems(long itemKey1, long itemKey2) {
    Item p1 = minHeap.get(itemKey1);
    Item p2 = minHeap.get(itemKey2);
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

  static final class Item {
    final long key;
    final long size;
    long lastAccessTime;
    long frequency;

    Item(long key, long size, long accessTime) {
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
