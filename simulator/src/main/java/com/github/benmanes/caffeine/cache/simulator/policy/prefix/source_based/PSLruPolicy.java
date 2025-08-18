package com.github.benmanes.caffeine.cache.simulator.policy.prefix.source_based;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.ChunkManager;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.SourceBasedChunkManager;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateUnderflowDelay;

@Policy.PolicySpec(name = "prefix.source-based.LRU")
public final class PSLruPolicy implements Policy {
  final PolicyStats policyStats;
  final long maximumCacheSize;

  long currentCacheSize;
  long currentTime;

  /**
   * Fast key -> node lookup
   */
  final Long2ObjectOpenHashMap<Prefix> data;
  /**
   * LRU order: head = MRU, tail = LRU
   */
  final Prefix head;
  final Prefix tail;

  final ChunkManager chunk_manager;

  public PSLruPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());
    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0L;
    this.currentTime = 0L;

    this.data = new Long2ObjectOpenHashMap<>();
    // Sentinel nodes for simpler list ops
    this.head = new Prefix(-1, 0);
    this.tail = new Prefix(-1, 0);
    head.next = tail;
    tail.prev = head;
    chunk_manager = new SourceBasedChunkManager();
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
    Prefix prefix = data.remove(event.key());
    if (prefix != null) {
      detach(prefix);
      currentCacheSize -= prefix.size;
      policyStats.recordOperation();
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
    long key = event.key();
    Prefix prefix = data.get(key);
    recordStats(event.itemSize(), prefix != null ? prefix.size : 0, event.retrievalDelay());

    long size = Math.min(event.itemSize(), chunk_manager.getChunkSize(key, event.itemSize()));

    if (prefix != null) {
      // Hit: move to MRU
      policyStats.recordHit();
      prefix.lastAccessTime = currentTime;
      moveToHead(prefix);
      policyStats.recordOperation();
      return;
    }

    // Miss
    policyStats.recordMiss();

    // Too big to admit
    if (size > maximumCacheSize) {
      return;
    }

    // Evict from the LRU end until it fits
    while (currentCacheSize + size > maximumCacheSize && !isEmpty()) {
      Prefix victim = removeLRU();
      data.remove(victim.key);
      currentCacheSize -= victim.size;
      policyStats.recordEviction();
    }

    // Admit
    Prefix newPrefix = new Prefix(key, size);
    newPrefix.lastAccessTime = currentTime;
    addToHead(newPrefix);
    data.put(key, newPrefix);
    currentCacheSize += size;
    policyStats.recordOperation();
    policyStats.recordAdmission();
  }

  // ==== Doubly-linked list helpers (head = MRU, tail = LRU) ====

  private boolean isEmpty() {
    return head.next == tail;
  }

  private void moveToHead(Prefix prefix) {
    detach(prefix);
    addToHead(prefix);
  }

  private void addToHead(Prefix prefix) {
    prefix.next = head.next;
    prefix.prev = head;
    head.next.prev = prefix;
    head.next = prefix;
  }

  private void detach(Prefix prefix) {
    prefix.prev.next = prefix.next;
    prefix.next.prev = prefix.prev;
    prefix.prev = null;
    prefix.next = null;
  }

  /**
   * Removes and returns LRU node (at tail.prev).
   */
  private Prefix removeLRU() {
    Prefix lru = tail.prev;
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

  static final class Prefix {
    final long key;
    final long size;
    long lastAccessTime;
    Prefix prev;
    Prefix next;

    Prefix(long key, long size) {
      this.key = key;
      this.size = size;
      this.lastAccessTime = 0L;
    }
  }
}
