package com.github.benmanes.caffeine.cache.simulator.policy.size_aware;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;

import static com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent.Operation.READ;
import static com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations.calculateLatency;

@Policy.PolicySpec(name = "size-aware.LRFU")
public final class SALrfuPolicy implements Policy {
  private static final double LAMBDA = 2.0; // Decay rate in time units

  private final PolicyStats policyStats;
  private final SearchableMinHeap<Long, Node> heap;
  private final long maximumCacheSize;
  private long currentCacheSize;
  private long currentTime;

  public SALrfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.maximumCacheSize = settings.maximumSize();
    this.policyStats = new PolicyStats(name());
    this.heap = new SearchableMinHeap<>((int) maximumCacheSize, this::compareNodes);
    this.currentCacheSize = 0;
    this.currentTime = 0;
  }

  @Override
  public void record(AccessEvent event) {
    currentTime++;
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
    var existingPrefix = heap.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      heap.remove(existingPrefix.key);
      policyStats.recordOperation();
      currentCacheSize -= existingPrefix.size;
      policyStats.recordEviction();
    }
  }

  private void onRead(AccessEvent event) {
    policyStats.recordOperation();
    currentTime++;

    Node node = heap.get(event.key());
    if (node == null) {
      long currentSize = event.itemSize();
      node = new Node(event.key(), currentSize, currentTime);
    }
    if (!heap.contains(node.key)) {
      if (event.operation() == READ) {
        policyStats.addDelay(event.retrievalDelay());
        double latency = calculateLatency(
          event.retrievalDelay(),
          event.itemSize(),
          0,
          Consts.BANDWIDTH
        );
        policyStats.addLatency(latency);
      }
    } else {
      if (event.operation() == READ) {
        double latency = calculateLatency(
          event.retrievalDelay(),
          event.itemSize(),
          event.itemSize(),
          Consts.BANDWIDTH
        );
        policyStats.addLatency(latency);
      }
    }

    updateScore(node);

    if (node.size > maximumCacheSize) {
      policyStats.recordRejection();
      return;
    }

    if (node.isEmpty()) {
      while (currentCacheSize + node.size > maximumCacheSize) {
        evict();
      }
      currentCacheSize += node.size;
      policyStats.recordAdmission();
    }

    if (node.isEmpty() && heap.contains(node.key)) heap.remove(node.key);
    else heap.upsert(node.key, node);
    policyStats.recordOperation();
  }

  private void updateScore(Node node) {
    double decay = Math.pow(0.5, (double) (currentTime - node.lastAccessTime) / LAMBDA);
    node.score = node.score * decay + 1;
    node.lastAccessTime = currentTime;
  }

  private void evict() {
    Node victim = heap.extractMin().value();
    currentCacheSize -= victim.size;
    policyStats.recordEviction();
  }

  private int compareNodes(long k1, long k2) {
    Node p1 = heap.get(k1);
    Node p2 = heap.get(k2);
    if (p1 == null || p2 == null) {
      throw new IllegalStateException("Node not found in heap: " + k1 + " or " + k2);
    }
    return Double.compare(p1.score, p2.score);
  }


  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  @Override
  public void finished() {
    Policy.super.finished();
  }

  static final class Node {
    final long key;
    final long size;
    long lastAccessTime;
    double score;

    Node(long key, long size, long now) {
      this.key = key;
      this.size = size;
      this.lastAccessTime = now;
      this.score = 0;
    }

    boolean isEmpty() {
      return size == 0;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("size", size)
        .add("score", score)
        .toString();
    }
  }
}
