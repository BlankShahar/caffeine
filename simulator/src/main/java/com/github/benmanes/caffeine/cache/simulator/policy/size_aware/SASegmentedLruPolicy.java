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

@Policy.PolicySpec(name = "size-aware.SegmentedLRU")
public final class SASegmentedLruPolicy implements Policy {
  final Long2ObjectMap<Node> data;
  final PolicyStats policyStats;

  final Node headProtected;
  final Node headProbation;

  final long maxProtectedSize;
  final long maxProbationSize;
  final long maximumSize;

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

  public SASegmentedLruPolicy(Config config) {
    this.policyStats = new PolicyStats(name());
    var settings = new BasicSettings(config);

    this.data = new Long2ObjectOpenHashMap<>();
    this.maximumSize = settings.maximumSize();

    // e.g., 80% of the cache is protected, 20% probation
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

  private void onRead(AccessEvent event) {
    Node node = data.get(event.key());

    if (node == null) {
      long currentSize = event.itemSize();
      onMiss(event.key(), event.retrievalDelay(), currentSize);

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
      onHit(node);

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
  }

  /**
   * Handle a cache hit.
   * If the node is in probation, promote it to protected (if capacity allows).
   */
  private void onHit(Node node) {
    if (node.type == QueueType.PROTECTED) {
      // Already in protected => move to MRU
      node.moveToTail(headProtected);
      policyStats.recordOperation();
    } else {
      // It's in probation => attempt promotion to protected
      long neededSize = node.size;
      if (sizeProtected + neededSize <= maxProtectedSize) {
        demoteProtectedUntilFits(neededSize);

        // Remove from probation tracking
        sizeProbation -= node.size;
        node.remove();
        policyStats.recordOperation();

        // Switch type & add to protected
        node.type = QueueType.PROTECTED;
        node.appendToTail(headProtected);
        policyStats.recordOperation();
        sizeProtected += node.size;
      } else {
        // Not enough room => remain in probation, but move to MRU
        node.moveToTail(headProbation);
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

    Node node = new Node(key, itemSize);
    node.type = QueueType.PROBATION;
    data.put(key, node);

    // Add to probation
    node.appendToTail(headProbation);
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
      Node victim;
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
      Node demote = headProtected.next; // LRU in protected
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
  private void evictEntry(Node node) {
    data.remove(node.key);
    currentSize -= node.size;

    if (node.type == QueueType.PROTECTED) {
      sizeProtected -= node.size;
    } else {
      sizeProbation -= node.size;
    }

    node.remove();
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
