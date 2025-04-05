package com.github.benmanes.caffeine.cache.simulator.policy.size_aware;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

@Policy.PolicySpec(name = "size-aware.SegmentedLRU")
public final class SASegmentedLruPolicy implements Policy {
  final Long2ObjectMap<Node> data;
  final PolicyStats policyStats;
  final Node headProtected;
  final Node headProbation;
  final long maxProtectedSize, maxProbationSize;
  final long maximumSize;

  long sizeProtected;
  long currentSize;

  public SASegmentedLruPolicy(Config config) {
    this.policyStats = new PolicyStats(name());
    var settings = new BasicSettings(config);

    this.data = new Long2ObjectOpenHashMap<>();
    this.maximumSize = settings.maximumSize();
    // For example, we assume 80% of the cache size is "protected".
    this.maxProtectedSize = (long) (maximumSize * 0.8);
    this.maxProbationSize = maximumSize - maxProtectedSize;

    // Initialize the protected queue's sentinel
    this.headProtected = new Node(-1);
    headProtected.prev = headProtected;
    headProtected.next = headProtected;

    // Initialize the probation queue's sentinel
    this.headProbation = new Node(-1);
    headProbation.prev = headProbation;
    headProbation.next = headProbation;
  }

  @Override
  public void record(AccessEvent event) {
    policyStats.recordOperation();
    Node node = data.get(event.key());

    if (node == null) {
      onMiss(event.key(), event.retrievalDelay(), event.itemSize());
    } else {
      onHit(node);
    }
  }

  private void onHit(Node node) {
    if (node.type == QueueType.PROTECTED) {
      // Already in protected queue, simply move it to MRU position
      node.moveToTail(headProtected);
    } else {
      // It's in probation, so promote it
      long neededSize = node.size;
      if (sizeProtected + neededSize > maxProtectedSize) return;
      demoteProtectedUntilFits(neededSize);
      sizeProtected += node.size;
      node.remove(); // remove from probation
      node.type = QueueType.PROTECTED;
      node.appendToTail(headProtected);
    }
    policyStats.recordHit();
    policyStats.addLatency(TimeCalculations.calculateTransmissionTime(node.size, Consts.BANDWIDTH));
  }

  private void onMiss(long key, double retrievalDelay, long itemSize) {
    policyStats.addLatency(TimeCalculations.calculateSourceLatency(retrievalDelay, itemSize, Consts.BANDWIDTH));
    policyStats.addDelay(retrievalDelay);

    // Optional: if an item is bigger than the entire cache, skip caching it
    if (itemSize > maxProbationSize) {
      policyStats.recordOperation(); // counted as an operation, no insert
      return;
    }

    Node node = new Node(key, itemSize);
    node.type = QueueType.PROBATION;
    data.put(key, node);
    currentSize += itemSize;

    node.appendToTail(headProbation);

    policyStats.recordMiss();
    evictIfNeeded();
  }

  /**
   * Ensures the overall cache size does not exceed {@link #maximumSize}.
   * We always evict from the probation queue first. If the probation
   * queue is empty, then we evict from the protected queue.
   */
  private void evictIfNeeded() {
    while (currentSize > maximumSize) {
      Node victim = (headProbation.next != headProbation)
        ? headProbation.next // eviction from probation first
        : (headProtected.next != headProtected)
        ? headProtected.next
        : null;
      if (victim == null) {
        // No more candidates to evict
        break;
      }
      evictEntry(victim);
    }
  }

  /**
   * Demotes protected items until there's enough free space in the
   * protected segment for a newly promoted item.
   */
  private void demoteProtectedUntilFits(long neededSize) {
    while (sizeProtected + neededSize > sizeProtected
      && headProtected.next != headProtected) {
      Node demote = headProtected.next; // LRU in protected
      demote.remove();
      demote.type = QueueType.PROBATION;
      demote.appendToTail(headProbation);
      sizeProtected -= demote.size;
    }
  }

  private void evictEntry(Node node) {
    data.remove(node.key);
    currentSize -= node.size;
    if (node.type == QueueType.PROTECTED) {
      sizeProtected -= node.size;
    }
    node.remove();
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

  static final class Node {
    final long key;
    final long size;
    Node prev;
    Node next;
    QueueType type;

    Node(long size) {
      this(Long.MIN_VALUE, size);
    }

    Node(long key, long size) {
      this.key = key;
      this.size = size;
    }

    /**
     * Insert at the tail (MRU) of the given 'head' sentinel.
     */
    void appendToTail(Node head) {
      Node tail = head.prev;
      tail.next = this;
      this.prev = tail;
      this.next = head;
      head.prev = this;
    }

    /**
     * Move this node to the tail (MRU) of the given 'head' sentinel.
     */
    void moveToTail(Node head) {
      remove();
      appendToTail(head);
    }

    /**
     * Unlinks this node from whatever list it's in.
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
