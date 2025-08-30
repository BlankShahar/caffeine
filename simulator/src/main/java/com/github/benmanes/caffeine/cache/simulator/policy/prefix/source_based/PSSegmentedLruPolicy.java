package com.github.benmanes.caffeine.cache.simulator.policy.prefix.source_based;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.ChunkManager;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.SourceBasedChunkManager;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateUnderflowDelay;

@Policy.PolicySpec(name = "prefix.source-based.SegmentedLRU")
public final class PSSegmentedLruPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
  final PolicyStats policyStats;

  final Prefix headProtected;
  final Prefix headProbation;

  final long maxProtectedSize;
  final long maxProbationSize;
  final long maximumSize;

  final ChunkManager chunk_manager;

  /**
   * Sum of the sizes in the PROTECTED queue.
   */
  long sizeProtected;
  /**
   * Sum of the sizes in the PROBATION queue.
   */
  long sizeProbation;
  /**
   * Sum of the sizes of all cached entries (protected + probation).
   */
  long currentSize;

  public PSSegmentedLruPolicy(Config config) {
    this.policyStats = new PolicyStats(name());
    var settings = new BasicSettings(config);

    this.data = new Long2ObjectOpenHashMap<>();
    this.maximumSize = settings.maximumSize();

    // e.g., 80% of the cache is protected, 20% probation
    this.maxProtectedSize = (long) (maximumSize * 0.8);
    this.maxProbationSize = maximumSize - maxProtectedSize;

    // Initialize the protected queue's sentinel
    this.headProtected = new Prefix(-1);
    headProtected.prev = headProtected;
    headProtected.next = headProtected;

    // Initialize the probation queue's sentinel
    this.headProbation = new Prefix(-1);
    headProbation.prev = headProbation;
    headProbation.next = headProbation;

    this.chunk_manager = new SourceBasedChunkManager();
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
    onDelete(event);
    onRead(event);
  }

  private void onDelete(AccessEvent event) {
    var existingItem = data.get(event.key());
    if (existingItem != null) {
      // Item exists, remove it
      data.remove(existingItem.key);
      if (existingItem.type == QueueType.PROTECTED) sizeProtected -= existingItem.size;
      else sizeProbation -= existingItem.size;
      existingItem.remove();
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
    Prefix prefix = data.get(event.key());
    if (event.operation() == AccessEvent.Operation.READ)
      recordStats(event.itemSize(), prefix != null ? prefix.size : 0, event.retrievalDelay());
    long itemSize = Math.min(event.itemSize(), chunk_manager.getChunkSize(event.key(), event.itemSize()));

    if (prefix == null) onMiss(event.key(), event.retrievalDelay(), itemSize);
    else onHit(prefix);
  }

  /**
   * Handle a cache hit.
   * If the node is in probation, promote it to protected (if capacity allows).
   */
  private void onHit(Prefix prefix) {
    if (prefix.type == QueueType.PROTECTED) {
      // Already in protected => move to MRU
      prefix.moveToTail(headProtected);
      policyStats.recordOperation();
    } else {
      // It's in probation => attempt promotion to protected
      long neededSize = prefix.size;
      if (sizeProtected + neededSize <= maxProtectedSize) {
        demoteProtectedUntilFits(neededSize);

        // Remove from probation tracking
        sizeProbation -= prefix.size;
        prefix.remove();
        policyStats.recordOperation();

        // Switch type & add to protected
        prefix.type = QueueType.PROTECTED;
        prefix.appendToTail(headProtected);
        policyStats.recordOperation();
        sizeProtected += prefix.size;
      } else {
        // Not enough room => remain in probation, but move to MRU
        prefix.moveToTail(headProbation);
        policyStats.recordOperation();
      }
    }
    policyStats.recordHit();
  }

  /**
   * Handle a cache miss by inserting a new node in probation (if it fits).
   */
  private void onMiss(long key, double retrievalDelay, long itemSize) {
    // If item is bigger than the entire probation region, skip
    if (itemSize > maxProbationSize) {
      return;
    }

    Prefix prefix = new Prefix(key, itemSize);
    prefix.type = QueueType.PROBATION;
    data.put(key, prefix);

    // Add to probation
    prefix.appendToTail(headProbation);
    policyStats.recordOperation();
    sizeProbation += itemSize;

    currentSize += itemSize;
    policyStats.recordMiss();

    evictIfNeeded();
  }

  /**
   * Evict items while we're over capacity.
   * - If there's something in probation (sizeProbation > 0), evict from there.
   * - Otherwise, evict from protected.
   */
  private void evictIfNeeded() {
    while (currentSize > maximumSize) {
      Prefix victim;
      if (sizeProbation > 0) {
        // Evict from probation
        victim = headProbation.next;
        if (victim == headProbation) {
          // Shouldn't happen, but let's break if it does
          break;
        }
      } else {
        // Evict from protected
        victim = headProtected.next;
        if (victim == headProtected) {
          // No more items
          break;
        }
      }
      evictEntry(victim);
    }
  }

  /**
   * Demote protected items until there's enough free space in PROTECTED
   * for the newly promoted item.
   */
  private void demoteProtectedUntilFits(long neededSize) {
    while ((sizeProtected + neededSize) > maxProtectedSize
      && headProtected.next != headProtected) {
      Prefix demote = headProtected.next; // LRU in protected
      demote.remove();
      policyStats.recordOperation();
      demote.type = QueueType.PROBATION;

      // Move to probation's MRU
      demote.appendToTail(headProbation);
      policyStats.recordOperation();
      sizeProtected -= demote.size;
      sizeProbation += demote.size;
    }
  }

  /**
   * Evict a given node (remove from data structure + queues).
   */
  private void evictEntry(Prefix prefix) {
    data.remove(prefix.key);
    currentSize -= prefix.size;

    if (prefix.type == QueueType.PROTECTED) {
      sizeProtected -= prefix.size;
    } else {
      sizeProbation -= prefix.size;
    }

    prefix.remove();
    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  enum QueueType {
    PROTECTED,
    PROBATION
  }

  static final class Prefix {
    final long key;
    final long size;

    Prefix prev;
    Prefix next;
    QueueType type;

    Prefix(long size) {
      this(Long.MIN_VALUE, size);
    }

    Prefix(long key, long size) {
      this.key = key;
      this.size = size;
    }

    /**
     * Insert at the tail (MRU) of the given 'head' sentinel.
     */
    void appendToTail(Prefix head) {
      Prefix tail = head.prev;
      tail.next = this;
      this.prev = tail;
      this.next = head;
      head.prev = this;
    }

    /**
     * Move this node to the tail (MRU) of the given 'head' sentinel.
     */
    void moveToTail(Prefix head) {
      remove();
      appendToTail(head);
    }

    /**
     * Unlinks this node from its doubly-linked list.
     */
    void remove() {
      if (prev != null && next != null) {
        prev.next = next;
        next.prev = prev;
        prev = next = null;
      }
    }
  }
}
