/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.caffeine.cache.simulator.policy.adaptive;

import static com.google.common.base.Preconditions.checkState;

import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.KeyOnlyPolicy;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.PolicySpec;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Var;
import com.typesafe.config.Config;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.HashMap;
import java.util.Random;

/**
 * Clock with Adaptive Replacement policy. This algorithm differs from ARC by replacing the LRU
 * policy with the Clock (Second Chance) policy. This allows cache hits to be performed concurrently
 * at the cost of a global lock on a miss and has the worst case time of O(2n) on eviction due to
 * queues being scanned.
 * <p>
 * This implementation is based on the pseudo code provided by the authors in their paper <a href=
 * "https://www.usenix.org/legacy/publications/library/proceedings/fast04/tech/full_papers/bansal/bansal.pdf">
 * CAR: Clock with Adaptive Replacement</a> and is further described in their paper,
 * <p>
 * This algorithm is patented by IBM (6996676, 7096321, 7058766, 8612689).
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@PolicySpec(name = "adaptive.Car")
public final class CarPolicy implements KeyOnlyPolicy {
  private final Long2ObjectMap<Node> data;
  private final PolicyStats policyStats;
  private final int maximumSize;

  private final Node headT1;
  private final Node headT2;
  private final Node headB1;
  private final Node headB2;

  private int sizeT1;
  private int sizeT2;
  private int sizeB1;
  private int sizeB2;
  private int p;

  private final Random sourcePicker;
  private final HashMap<Long, Source> itemToSource;

  public CarPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.maximumSize = Math.toIntExact(settings.maximumSize());
    this.policyStats = new PolicyStats(name());
    this.data = new Long2ObjectOpenHashMap<>();

    this.itemToSource = new HashMap<>();
    sourcePicker = new Random(Consts.REAL_SEED);
    double itemSize = Consts.ITEM_CHUNKS_AMOUNT * Consts.CHUNK_SIZE;
    this.headT1 = new Node(itemSize);
    this.headT2 = new Node(itemSize);
    this.headB1 = new Node(itemSize);
    this.headB2 = new Node(itemSize);
  }

  @Override
  public void record(long key) {
    Node node = data.get(key);
    if (!itemToSource.containsKey(key)) {
      Source source = Consts.REAL_SOURCES.get(sourcePicker.nextInt(Consts.REAL_SOURCES.size()));
      itemToSource.put(key, source);
    }

    if (isHit(node)) {
      policyStats.recordHit();
      policyStats.addLatency(TimeCalculations.calculateTransmissionTime(node.size, Consts.BANDWIDTH));

      onHit(node);
    } else {
      policyStats.recordMiss();
      double itemSize = Consts.ITEM_CHUNKS_AMOUNT * Consts.CHUNK_SIZE;
      double realSourceProcessingTime = itemToSource.get(key).getNextProcessingTime();
      policyStats.addLatency(TimeCalculations.calculateSourceLatency(realSourceProcessingTime, itemSize, Consts.BANDWIDTH));
      policyStats.addDelay(realSourceProcessingTime);

      onMiss(key, node);
    }
  }

  private static boolean isHit(Node node) {
    // if (x is in T1 ∪ T2) then cache hit
    return (node != null) && ((node.type == QueueType.T1) || (node.type == QueueType.T2));
  }

  private void onHit(Node node) {
    // Set the page reference bit for x
    node.marked = true;
    policyStats.recordOperation();
  }

  private void onMiss(long key, @Var Node node) {
    // if (|T1| + |T2| = c) then
    //   /* cache full, replace a page from cache */
    //   replace()
    //   /* cache directory replacement */
    //   if ((x is not in B1 ∪ B2) and (|T1|+|B1| = c)) then
    //     Discard the LRU page in B1.
    //   else if ((x is not in B1 ∪ B2) and (|T1|+|T2|+|B1|+|B2| = 2c)) then
    //     Discard the LRU page in B2.
    //
    // /* cache directory miss */
    // if (x is not in B1 ∪ B2) then
    //   Insert x at the tail of T1.
    //   Reset the page reference bit of x // cache directory hit
    // else if (x is in B1) then
    //   Adapt: Increase the target size for the list T1 as: p = min{p + max{1, |B2|/|B1|}, c}
    //   Move x at the tail of T2.
    //   Reset the page reference bit of x. // cache directory hit
    // else /* x must be in B2 */
    //   Adapt: Decrease the target size for the list T1 as: p = max{p − max{1, |B1|/|B2|}, 0}
    //   Move x at the tail of T2.
    //   Set the page reference bit of x.
    policyStats.recordOperation();

    if ((sizeT1 + sizeT2) == maximumSize) {
      demote();
      if (!isGhost(node)) {
        if ((sizeT1 + sizeB1) == maximumSize) {
          // Discard the LRU page in B1
          Node victim = headB1.next;
          data.remove(victim.key);
          victim.remove();
          sizeB1--;
        } else if ((sizeT1 + sizeT2 + sizeB1 + sizeB2) == (2 * maximumSize)) {
          // Discard the LRU page in B2
          Node victim = headB2.next;
          data.remove(victim.key);
          victim.remove();
          sizeB2--;
        }
      }
    }
    if (!isGhost(node)) {
      // Insert x at the tail of T1
      // Reset the page reference bit of x
      checkState(node == null);
      node = new Node(key, Consts.ITEM_CHUNKS_AMOUNT * Consts.CHUNK_SIZE);
      node.appendToTail(headT1);
      node.type = QueueType.T1;
      data.put(key, node);
      sizeT1++;
    } else if (node.type == QueueType.B1) {
      // Adapt: Increase the target size for the list T1 as: p = min{p + max{1, |B2|/|B1|}, c}
      // Move x at the tail of T2
      // Reset the page reference bit of x.
      p = Math.min(p + Math.max(1, sizeB2 / sizeB1), maximumSize);
      node.remove();
      sizeB1--;
      node.appendToTail(headT2);
      node.type = QueueType.T2;
      sizeT2++;
      node.marked = false;
    } else if (node.type == QueueType.B2) {
      // Adapt: Decrease the target size for the list T1 as: p = max{p − max{1, |B1|/|B2|}, 0}
      // Move x at the tail of T2.
      // Reset the page reference bit of x.
      p = Math.max(p - Math.max(1, sizeB1 / sizeB2), 0);
      node.remove();
      sizeB2--;
      node.appendToTail(headT2);
      node.type = QueueType.T2;
      sizeT2++;
      node.marked = false;
    } else {
      throw new IllegalStateException();
    }
  }

  private static boolean isGhost(Node node) {
    return (node != null) && ((node.type == QueueType.B1) || (node.type == QueueType.B2));
  }

  @SuppressWarnings("PMD.ConfusingTernary")
  private void demote() {
    // found = 0
    // repeat
    //   if (|T1| >= max(1, p)) then
    //     if (the page reference bit of head page in T1 is 0) then
    //       found = 1;
    //       Demote the head page in T1 and make it the MRU page in B1.
    //     else
    //       Set the page reference bit of head page in T1 to 0, and make it the tail page in T2.
    //   else
    //     if (the page reference bit of head page in T2 is 0), then
    //       found = 1;
    //       Demote the head page in T2 and make it the MRU page in B2.
    //     else
    //       Set the page reference bit of head page in T2 to 0, and make it the tail page in T2.
    // until (found)

    policyStats.recordEviction();
    for (; ; ) {
      policyStats.recordOperation();
      if (sizeT1 >= Math.max(1, p)) {
        Node candidate = headT1.next;
        if (!candidate.marked) {
          candidate.remove();
          sizeT1--;
          candidate.appendToTail(headB1);
          candidate.type = QueueType.B1;
          sizeB1++;
          return;
        } else {
          candidate.marked = false;
          candidate.remove();
          sizeT1--;
          candidate.appendToTail(headT2);
          candidate.type = QueueType.T2;
          sizeT2++;
        }
      } else {
        Node candidate = headT2.next;
        if (!candidate.marked) {
          candidate.remove();
          sizeT2--;
          candidate.appendToTail(headB2);
          candidate.type = QueueType.B2;
          sizeB2++;
          return;
        } else {
          candidate.marked = false;
          candidate.remove();
          candidate.appendToTail(headT2);
          candidate.type = QueueType.T2;
        }
      }
    }
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  @Override
  public void finished() {
    checkState(sizeT1 == data.values().stream().filter(node -> node.type == QueueType.T1).count());
    checkState(sizeT2 == data.values().stream().filter(node -> node.type == QueueType.T2).count());
    checkState(sizeB1 == data.values().stream().filter(node -> node.type == QueueType.B1).count());
    checkState(sizeB2 == data.values().stream().filter(node -> node.type == QueueType.B2).count());
    checkState((sizeT1 + sizeT2) <= maximumSize);
    checkState((sizeB1 + sizeB2) <= maximumSize);
  }

  private enum QueueType {
    T1, B1,
    T2, B2,
  }

  static final class Node {
    final long key;
    final double size; // in MB

    @Nullable
    Node prev;
    @Nullable
    Node next;
    @Nullable
    QueueType type;

    boolean marked;

    Node(double size) {
      this.key = Long.MIN_VALUE;
      this.prev = this;
      this.next = this;
      this.size = size;
    }

    Node(long key, double size) {
      this.key = key;
      this.size = size;
    }

    /**
     * Appends the node to the tail of the list.
     */
    public void appendToTail(Node head) {
      Node tail = head.prev;
      head.prev = this;
      tail.next = this;
      next = head;
      prev = tail;
    }

    /**
     * Removes the node from the list.
     */
    public void remove() {
      prev.next = next;
      next.prev = prev;
      prev = next = null;
      type = null;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("type", type)
        .toString();
    }
  }
}
