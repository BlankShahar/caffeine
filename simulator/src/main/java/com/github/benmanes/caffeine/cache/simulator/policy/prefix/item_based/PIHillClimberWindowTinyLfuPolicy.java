/*
 * Copyright 2016 Ben Manes. All Rights Reserved.
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
package com.github.benmanes.caffeine.cache.simulator.policy.prefix.item_based;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.countmin4.PeriodicResetCountMin4;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.ChunkManager;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.ItemBasedChunkManager;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.HillClimber;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.HillClimber.Adaptation;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.HillClimber.QueueType;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.HillClimberType;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateUnderflowDelay;
import static com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.HillClimber.Adaptation.Type.DECREASE_WINDOW;
import static com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.HillClimber.Adaptation.Type.INCREASE_WINDOW;
import static com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.HillClimber.QueueType.*;
import static com.google.common.base.Preconditions.checkState;

/**
 * The Window TinyLfu algorithm where the size of the admission window is adjusted using the a hill
 * climbing algorithm.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings("PMD.TooManyFields")
@Policy.PolicySpec(name = "prefix.item-based.HillClimberWindowTinyLFU")
public class PIHillClimberWindowTinyLfuPolicy implements Policy {
  protected final double initialPercentMain;
  protected final HillClimberType strategy;
  private final Long2ObjectMap<Prefix> data;
  private final PolicyStats policyStats;
  private final HillClimber climber;
  protected final PeriodicResetCountMin4 sketch;
  protected final long maximumSize;

  private final Prefix headWindow;
  protected final Prefix headProbation;
  protected final Prefix headProtected;

  protected long maxWindow;
  private long maxProtected;
  private boolean isFull;

  protected long windowSize;
  private long protectedSize;
  protected long sizeData;

  static final boolean debug = false;
  static final boolean trace = false;

  final ItemBasedChunkManager chunk_manager;

  public PIHillClimberWindowTinyLfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    long maxMain = (long) (settings.maximumSize() * 0.99);
    this.maxProtected = (long) (maxMain * 0.8);
    this.maxWindow = settings.maximumSize() - maxMain;
    this.data = new Long2ObjectOpenHashMap<>();
    this.maximumSize = settings.maximumSize();
    this.headProtected = new Prefix();
    this.headProbation = new Prefix();
    this.headWindow = new Prefix();
    this.isFull = false;

    this.strategy = HillClimberType.SIMPLE;
    this.initialPercentMain = 0.99;
    this.policyStats = new PolicyStats(name());
    this.sketch = new PeriodicResetCountMin4(settings.config());
    this.climber = strategy.create(settings.config());
    this.chunk_manager = new ItemBasedChunkManager();

    printSegmentSizes();
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
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
      sizeData -= existingItem.weight;
      if (existingItem.queue == PROTECTED) protectedSize -= existingItem.weight;
      else if (existingItem.queue == WINDOW) windowSize -= existingItem.weight;
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
    final long key = event.key();
    Prefix prefix = data.get(key);
    recordStats(event.itemSize(), prefix != null ? prefix.weight : 0, event.retrievalDelay());

    final long weight = Math.min(event.itemSize(), chunk_manager.getChunkSize(event.key(), event.itemSize()));
    chunk_manager.addDelay(event.key(), event.retrievalDelay());

    if (sizeData >= (maximumSize >>> 1)) {
      sketch.ensureCapacity(2_000_000);
      if ((sizeData + weight) >= maximumSize) {
        isFull = true;
      }
    }
    sketch.increment(key);

    QueueType queue = null;
    if (prefix == null) {
      onMiss(key, weight);
      policyStats.recordWeightedMiss(weight);
    } else {
      queue = prefix.queue;
      policyStats.recordWeightedHit(weight);

      if (queue == WINDOW) {
        onWindowHit(prefix);
      } else if (queue == PROBATION) {
        onProbationHit(prefix);
      } else if (queue == PROTECTED) {
        onProtectedHit(prefix);
      } else {
        throw new IllegalStateException();
      }
    }
    climb(key, queue, isFull);
  }

  /**
   * Adds the entry to the admission window, evicting if necessary.
   */
  private void onMiss(long key, long weight) {
    if (weight > (maximumSize - maxWindow)) {
      policyStats.recordRejection();
      return;
    }

    Prefix prefix = new Prefix(key, weight, WINDOW);
    if (weight > maxWindow) {
      prefix.appendToHead(headWindow);
      policyStats.recordOperation();
    } else {
      prefix.appendToTail(headWindow);
      policyStats.recordOperation();
    }
    data.put(key, prefix);
    windowSize += weight;
    sizeData += weight;
    evict();
  }

  /**
   * Moves the entry to the MRU position in the admission window.
   */
  private void onWindowHit(Prefix prefix) {
    prefix.moveToTail(headWindow);
    policyStats.recordOperation();
  }

  /**
   * Promotes the entry to the protected region's MRU position, demoting an entry if necessary.
   */
  private void onProbationHit(Prefix prefix) {
    prefix.remove();
    policyStats.recordOperation();
    prefix.queue = PROTECTED;
    prefix.appendToTail(headProtected);
    policyStats.recordOperation();
    protectedSize += prefix.weight;
    demoteProtected();
  }

  private void demoteProtected() {
    while (protectedSize > maxProtected) {
      Prefix demote = headProtected.next;
      demote.remove();
      policyStats.recordOperation();
      demote.queue = PROBATION;
      demote.appendToTail(headProbation);
      policyStats.recordOperation();
      protectedSize -= demote.weight;
    }
  }

  /**
   * Moves the entry to the MRU position, if it falls outside of the fast-path threshold.
   */
  private void onProtectedHit(Prefix prefix) {
    prefix.moveToTail(headProtected);
    policyStats.recordOperation();
  }

  /**
   * Evicts from the admission window into the probation space. If the size exceeds the maximum,
   * then the admission candidate and probation's victim are evaluated and one is evicted.
   */
  private void evict() {
    final Prefix headCandidates = new Prefix();
    collectCandidates(headCandidates);
    while (headCandidates.prev != headCandidates) {
      Prefix candidate = headCandidates.prev;
      candidate.remove();
      policyStats.recordOperation();
      if ((sizeData + candidate.weight - windowSize) > (maximumSize - maxWindow)) {
        coreEviction(candidate);
      } else {
        admit(candidate);
      }
    }
  }

  protected void coreEviction(Prefix candidate) {
    Prefix victim = getVictim();
    if (compare(sketch.frequency(candidate.key), candidate.weight, sketch.frequency(victim.key), victim.weight)) {
      while ((sizeData + candidate.weight - windowSize) > (maximumSize - maxWindow)) {
        Prefix evict = getVictim();
        evictNode(evict);
      }
      admit(candidate);
    } else {
      reject(candidate);
    }
  }

  protected boolean compare(int candidateFreq, long candidateWeight, int victimFreq, long victimWeight) {
    return candidateFreq > victimFreq;
  }

  protected void admit(Prefix candidate) {
    candidate.appendToTail(headProbation);
    policyStats.recordOperation();
    sizeData += candidate.weight;
    policyStats.recordAdmission();
  }

  protected void reject(Prefix candidate) {
    data.remove(candidate.key);
    policyStats.recordOperation();
    policyStats.recordEviction();
    policyStats.recordRejection();
  }

  protected void evictNode(Prefix evict) {
    data.remove(evict.key);
    sizeData -= evict.weight;
    if (evict.queue == PROTECTED) {
      protectedSize -= evict.weight;
    }
    evict.remove();
    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  /**
   * Collect candidates  for eviction from the Window
   */
  private void collectCandidates(final Prefix headCandidates) {
    while (windowSize > maxWindow) {
      Prefix candidate = headWindow.next;
      windowSize -= candidate.weight;
      sizeData -= candidate.weight;
      candidate.remove();
      policyStats.recordOperation();
      if (candidate.weight > (maximumSize - maxWindow)) {
        reject(candidate);
      } else {
        candidate.queue = PROBATION;
        candidate.appendToTail(headCandidates);
        policyStats.recordOperation();
      }
    }
  }

  protected Prefix getVictim() {
    if (headProbation.next != headProbation) {
      return headProbation.next;
    }
    return headProtected.next;
  }

  int accesses;

  /**
   * Performs the hill climbing process.
   */
  private void climb(long key, @Nullable QueueType queue, boolean isFull) {
    if (queue == null) {
      climber.onMiss(key, isFull);
    } else {
      climber.onHit(key, queue, isFull);
    }

    double probationSize = maximumSize - windowSize - protectedSize;
    //if (isFull && sketch.isGoingToReset()) {
    if (++accesses == 1000000) {
      accesses = 0;
      Adaptation adaptation = climber.adapt(windowSize, probationSize, protectedSize, isFull);
      if (adaptation.type == INCREASE_WINDOW) {
        increaseWindow(adaptation.amount);
      } else if (adaptation.type == DECREASE_WINDOW) {
        decreaseWindow(adaptation.amount);
      }
    }
    //}
  }

  private void increaseWindow(double amount) {
    checkState(amount >= 0.0);
    if (maxProtected <= 0) {
      return;
    }

    long quota = Math.min((long) amount, maxProtected);

    maxWindow += quota;
    maxProtected -= quota;

    demoteProtected();
    while ((sizeData - windowSize) > (maximumSize - maxWindow)) {
      Prefix candidate = getVictim();
      if (candidate.queue == PROTECTED) {
        protectedSize -= candidate.weight;
      }
      candidate.queue = WINDOW;
      candidate.remove();
      policyStats.recordOperation();
      candidate.appendToHead(headWindow);
      policyStats.recordOperation();
      windowSize += candidate.weight;
    }
    evict();

    checkState(windowSize >= 0);
    checkState(maxWindow >= 0);
    checkState(maxProtected >= 0);
    checkState(sizeData <= maximumSize);
    checkState(windowSize <= maxWindow);

    if (trace) {
      System.out.printf("+%,d (%,d -> %,d)%n", quota, maxWindow - quota, maxWindow);
    }
  }

  private void decreaseWindow(double amount) {
    checkState(amount >= 0.0);
    if (maxWindow <= 0) {
      return;
    }

    long quota = Math.min((long) amount, maxWindow);
    checkState(quota >= 0);
    maxWindow -= quota;
    maxProtected += quota;

    Prefix candidate = headWindow.next;
    while ((windowSize > maxWindow) && (sizeData - windowSize + candidate.weight) <= (maximumSize - maxWindow)) {
      candidate.queue = PROBATION;
      candidate.remove();
      policyStats.recordOperation();
      candidate.appendToHead(headProbation);
      policyStats.recordOperation();
      windowSize -= candidate.weight;
      candidate = headWindow.next;
    }
    evict();

    checkState(windowSize >= 0);
    checkState(maxWindow >= 0);
    checkState(maxProtected >= 0);
    checkState(sizeData <= maximumSize);
    checkState(windowSize <= maxWindow);

    if (trace) {
      System.out.printf("-%,d (%,d -> %,d)%n", quota, maxWindow + quota, maxWindow);
    }
  }

  private void printSegmentSizes() {
    if (debug) {
      System.out.printf("maxWindow=%d, maxProtected=%d, percentWindow=%.1f",
        maxWindow, maxProtected, (100.0 * maxWindow) / maximumSize);
    }
  }

  @Override
  public void finished() {
    printSegmentSizes();

    long actualWindowSize = data.values().stream().filter(n -> n.queue == WINDOW).mapToLong(prefix -> prefix.weight).sum();
    long actualProbationSize = data.values().stream().filter(n -> n.queue == PROBATION).mapToLong(prefix -> prefix.weight).sum();
    long actualProtectedSize = data.values().stream().filter(n -> n.queue == PROTECTED).mapToLong(prefix -> prefix.weight).sum();
    long calculatedProbationSize = sizeData - actualWindowSize - actualProtectedSize;

    checkState(windowSize == actualWindowSize,
      "Window: %s != %s", windowSize, actualWindowSize);
    checkState(protectedSize == actualProtectedSize,
      "Protected: %s != %s", protectedSize, actualProtectedSize);
    checkState(actualProbationSize == calculatedProbationSize,
      "Probation: %s != %s", actualProbationSize, calculatedProbationSize);
    checkState(sizeData <= maximumSize, "Maximum: %s > %s", sizeData, maximumSize);
  }

  /**
   * A node on the double-linked list.
   */
  static final class Prefix {
    final long key;
    final long weight;

    QueueType queue;
    Prefix prev;
    Prefix next;

    /**
     * Creates a new sentinel node.
     */
    public Prefix() {
      this.key = Integer.MIN_VALUE;
      this.weight = 0;
      this.prev = this;
      this.next = this;
    }

    /**
     * Creates a new, unlinked node.
     */
    public Prefix(long key, long weight, QueueType queue) {
      this.queue = queue;
      this.key = key;
      this.weight = weight;
    }

    public void moveToTail(Prefix head) {
      remove();
      appendToTail(head);
    }

    /**
     * Appends the node to the tail of the list.
     */
    public void appendToHead(Prefix head) {
      Prefix first = head.next;
      head.next = this;
      first.prev = this;
      prev = head;
      next = first;
    }

    /**
     * Appends the node to the tail of the list.
     */
    public void appendToTail(Prefix head) {
      Prefix tail = head.prev;
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
      next = prev = null;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("weight", weight)
        .add("queue", queue)
        .toString();
    }
  }
}
