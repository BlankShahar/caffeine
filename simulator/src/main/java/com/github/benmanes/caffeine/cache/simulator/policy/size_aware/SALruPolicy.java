package com.github.benmanes.caffeine.cache.simulator.policy.size_aware;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import static com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent.Operation.READ;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;

@Policy.PolicySpec(name = "size-aware.LRU")
public final class SALruPolicy implements Policy {
  final PolicyStats policyStats;
  final long maximumCacheSize;

  long currentCacheSize;
  long currentTime;

  /**
   * Fast key -> node lookup
   */
  final Long2ObjectMap<Node> data;
  /**
   * LRU order: head = MRU, tail = LRU
   */
  final Node head;
  final Node tail;

  public SALruPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());
    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0L;
    this.currentTime = 0L;

    this.data = new Long2ObjectOpenHashMap<>();
    // Sentinel nodes for simpler list ops
    this.head = new Node(-1, 0);
    this.tail = new Node(-1, 0);
    head.next = tail;
    tail.prev = head;
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
    onDelete(event);
    onRead(event);
  }

  private void onDelete(AccessEvent event) {
    Node node = data.remove(event.key());
    if (node != null) {
      detach(node);
      currentCacheSize -= node.size;
      policyStats.recordOperation();
      policyStats.recordEviction(); // Delete counts as explicit removal/eviction in original code
    }
  }

  private void onRead(AccessEvent event) {
    long key = event.key();
    long size = event.itemSize();
    double retrievalDelay = event.retrievalDelay();

    Node node = data.get(key);
    if (node != null) {
      // Hit: move to MRU
      policyStats.recordHit();
      node.lastAccessTime = currentTime;
      moveToHead(node);
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
      return;
    }

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

    // Too big to admit
    if (size > maximumCacheSize) {
      return;
    }

    // Evict from the LRU end until it fits
    while (currentCacheSize + size > maximumCacheSize && !isEmpty()) {
      Node victim = removeLRU();
      data.remove(victim.key);
      currentCacheSize -= victim.size;
      policyStats.recordEviction();
    }

    // Admit
    Node newNode = new Node(key, size);
    newNode.lastAccessTime = currentTime;
    addToHead(newNode);
    data.put(key, newNode);
    currentCacheSize += size;
    policyStats.recordOperation();
    policyStats.recordAdmission();
  }

  // ==== Doubly-linked list helpers (head = MRU, tail = LRU) ====

  private boolean isEmpty() {
    return head.next == tail;
  }

  private void moveToHead(Node node) {
    detach(node);
    addToHead(node);
  }

  private void addToHead(Node node) {
    node.next = head.next;
    node.prev = head;
    head.next.prev = node;
    head.next = node;
  }

  private void detach(Node node) {
    node.prev.next = node.next;
    node.next.prev = node.prev;
    node.prev = null;
    node.next = null;
  }

  /**
   * Removes and returns LRU node (at tail.prev).
   */
  private Node removeLRU() {
    Node lru = tail.prev;
    if (lru == head) {
      return null; // should not happen; caller checks empty
    }
    detach(lru);
    return lru;
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

  // ==== Node (formerly Item) ====

  static final class Node {
    final long key;
    final long size;
    long lastAccessTime;
    Node prev;
    Node next;

    Node(long key, long size) {
      this.key = key;
      this.size = size;
      this.lastAccessTime = 0L;
    }
  }
}
