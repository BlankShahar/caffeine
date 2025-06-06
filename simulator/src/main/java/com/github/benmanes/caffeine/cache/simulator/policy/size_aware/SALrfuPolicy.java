package com.github.benmanes.caffeine.cache.simulator.policy.size_aware;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

@Policy.PolicySpec(name = "size-aware.LRFU")
public final class SALrfuPolicy implements Policy {
  private static final double LAMBDA = 2.0; // Decay rate in time units

  private final Long2ObjectMap<Node> data;
  private final PolicyStats stats;
  private final SearchableMinHeap<Long, Node> heap;
  private final long maximumCacheSize;
  private long currentCacheSize;
  private long currentTime;

  public SALrfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.maximumCacheSize = settings.maximumSize();
    this.stats = new PolicyStats(name());
    this.data = new Long2ObjectOpenHashMap<>();
    this.heap = new SearchableMinHeap<>((int) maximumCacheSize, this::compareNodes);
    this.currentCacheSize = 0;
    this.currentTime = 0;
  }

  @Override
  public void record(AccessEvent event) {
    stats.recordOperation();
    currentTime++;

    Node node = data.get(event.key());
    if (node == null) {
      long chunksAmount = event.itemSize(); // (long) Math.ceil(event.itemSize() / (Consts.CHUNK_SIZE * 1024 * 1024));
      node = new Node(event.key(), chunksAmount, currentTime);
      data.put(event.key(), node);
    }
    if (!heap.contains(node.key)) {
      double delay = TimeCalculations.calculateUnderflowDelay(event.retrievalDelay(), node.sizeInMB(), node.cachedInMB(), Consts.BANDWIDTH);
      stats.addDelay(delay);
    }

    updateScore(node);

    if (node.size > maximumCacheSize) {
      stats.recordRejection();
      return;
    }

    if (node.isEmpty()) {
      while (currentCacheSize + node.size > maximumCacheSize) {
        evict();
      }
      node.chunks = node.size;
      currentCacheSize += node.size;
      stats.recordAdmission();
    }

    if (node.isEmpty() && heap.contains(node.key)) heap.remove(node.key);
    else heap.upsert(node.key, node);
  }

  private void updateScore(Node node) {
    double decay = Math.pow(0.5, (double) (currentTime - node.lastAccessTime) / LAMBDA);
    node.score = node.score * decay + 1;
    node.lastAccessTime = currentTime;
  }

  private void evict() {
    Node victim = heap.extractMin().value();
    currentCacheSize -= victim.chunks;
    victim.chunks = 0;
    stats.recordEviction();
  }

  private int compareNodes(long k1, long k2) {
    return Double.compare(data.get(k1).score, data.get(k2).score);
  }


  @Override
  public PolicyStats stats() {
    return stats;
  }

  @Override
  public void finished() {
    Policy.super.finished();
  }

  static final class Node {
    final long key;
    final long size;
    long chunks;
    long lastAccessTime;
    double score;

    Node(long key, long size, long now) {
      this.key = key;
      this.size = size;
      this.lastAccessTime = now;
      this.score = 0;
      this.chunks = 0;
    }

    boolean isEmpty() {
      return chunks == 0;
    }

    double sizeInMB() {
      return size * Consts.CHUNK_SIZE;
    }

    double cachedInMB() {
      return chunks * Consts.CHUNK_SIZE;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("chunks", chunks)
        .add("score", score)
        .toString();
    }
  }
}
