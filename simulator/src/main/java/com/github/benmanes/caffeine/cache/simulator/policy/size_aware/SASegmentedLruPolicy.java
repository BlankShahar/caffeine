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

@Policy.PolicySpec(name = "size-aware.SegmentedLru")
public final class SASegmentedLruPolicy implements Policy {
  static final Node UNLINKED = new Node(0);

  final Long2ObjectMap<Node> data;
  final PolicyStats policyStats;
  final Node headProtected;
  final Node headProbation;
  final long maxProtectedSize;
  final long maximumSize;

  long sizeProtected;
  long currentSize;

  public SASegmentedLruPolicy(Config config) {
    this.policyStats = new PolicyStats(name());
    var settings = new BasicSettings(config);

    this.headProtected = new Node(0);
    this.headProbation = new Node(0);
    this.headProtected.prev = this.headProtected.next = this.headProtected;
    this.headProbation.prev = this.headProbation.next = this.headProbation;

    this.data = new Long2ObjectOpenHashMap<>();
    this.maximumSize = settings.maximumSize();
    this.maxProtectedSize = (long) (maximumSize * 0.8);
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
      node.moveToTail(headProtected);
    } else {
      if (node.size + sizeProtected > maxProtectedSize) {
        demoteProtectedUntilFits(node.size);
      }
      sizeProtected += node.size;
      node.remove();
      node.type = QueueType.PROTECTED;
      node.appendToTail(headProtected);
    }
    policyStats.recordHit();
    policyStats.addLatency(TimeCalculations.calculateTransmissionTime(node.size, Consts.BANDWIDTH));
  }

  private void onMiss(long key, double retrievalDelay, long itemSize) {
    if (itemSize > maximumSize) return;

    var node = new Node(key, itemSize);
    data.put(key, node);
    currentSize += itemSize;
    node.appendToTail(headProbation);
    node.type = QueueType.PROBATION;

    policyStats.recordMiss();
    policyStats.addLatency(TimeCalculations.calculateSourceLatency(retrievalDelay, node.size, Consts.BANDWIDTH));
    policyStats.addDelay(retrievalDelay);

    evictIfNeeded();
  }

  private void evictIfNeeded() {
    while (currentSize > maximumSize) {
      Node victim = (maxProtectedSize == 0) ? headProtected.next : headProbation.next;
      if (victim == headProtected || victim == headProbation) return;
      evictEntry(victim);
    }
  }

  private void demoteProtectedUntilFits(long neededSize) {
    while (sizeProtected + neededSize > maxProtectedSize && headProtected.next != headProtected) {
      Node demote = headProtected.next;
      if (demote == null || demote == headProtected) break;

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
    PROBATION,
  }

  static final class Node {
    final long key;
    final long size;
    Node prev, next;
    QueueType type;

    Node(long size) {
      this(Long.MIN_VALUE, size);
    }

    Node(long key, long size) {
      this.key = key;
      this.size = size;
      this.prev = this.next = null;
    }

    void appendToTail(Node head) {
      if (head.prev == null || head.next == null) {
        head.prev = head.next = head;
      }
      Node tail = head.prev;
      tail.next = this;
      this.prev = tail;
      this.next = head;
      head.prev = this;
    }

    void moveToTail(Node head) {
      remove();
      appendToTail(head);
    }

    void remove() {
      if (prev == null || next == null || this == UNLINKED) return;
      prev.next = next;
      next.prev = prev;
      prev = next = null;
    }
  }
}
