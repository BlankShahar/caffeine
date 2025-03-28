package com.github.benmanes.caffeine.cache.simulator.policy.size_aware;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;


@Policy.PolicySpec(name = "size-aware.Lfu")
public final class LfuPolicy implements Policy {
  final PolicyStats policyStats;
  final Long2ObjectMap<Item> data;
  final SearchableMinHeap<Long, Item> minHeap;
  final long maximumCacheSize;
  long currentCacheSize;

  public LfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());
    this.data = new Long2ObjectOpenHashMap<>();
    this.minHeap = new SearchableMinHeap<>((int) settings.maximumSize() * 1_000, this::compareItems);
    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0;
  }

  @Override
  public void record(AccessEvent event) {
    long itemKey = event.key();
    long itemSize = event.itemSize();
    double retrievalDelay = event.retrievalDelay();

    policyStats.recordOperation();

    Item item = data.get(itemKey);
    if (item != null) {
      // Hit
      policyStats.recordHit();
      minHeap.remove(itemKey); // Re-heapify after frequency change
      item.frequency++;
      minHeap.insert(itemKey, item);
    } else {
      // Miss
      policyStats.recordMiss();
      policyStats.addDelay(retrievalDelay);

      // There's no enough space in the cache to insert the item
      if (itemSize > maximumCacheSize) {
        return;
      }

      // Evict items until there's enough space
      while (currentCacheSize + itemSize > maximumCacheSize && !minHeap.isEmpty()) {
        Item victim = minHeap.extractMin().value();
        data.remove(victim.key);
        currentCacheSize -= victim.size;
        policyStats.recordEviction();
      }

      // Insert new item
      Item newItem = new Item(itemKey, itemSize);
      data.put(itemKey, newItem);
      minHeap.insert(itemKey, newItem);
      currentCacheSize += itemSize;
      policyStats.recordAdmission();
    }
  }

  public int compareItems(long itemKey1, long itemKey2) {
    Item p1 = data.get(itemKey1);
    Item p2 = data.get(itemKey2);
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
}
