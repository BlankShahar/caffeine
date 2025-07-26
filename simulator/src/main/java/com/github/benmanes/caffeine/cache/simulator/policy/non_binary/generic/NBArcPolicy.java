package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.generic;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.SearchableMinHeap;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.LogNormalSource;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

import static com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent.Operation.READ;
import static com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations.calculateLatency;
import static com.google.common.base.Preconditions.checkState;


@Policy.PolicySpec(name = "non-binary.Arc")
public final class NBArcPolicy implements Policy {
  final Queue<Long> requests;
  static long currentTime;
  final long maximumCacheSize;
  long sizeT1, sizeT2, sizeB1, sizeB2, p;
  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Prefix> heapT1;
  final SearchableMinHeap<Long, Prefix> heapT2;
  final Long2ObjectOpenHashMap<Prefix> B1, B2;
  final Source source;

  enum Q {T1, T2, B1, B2, NONE}

  public NBArcPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.requests = new ArrayDeque<>();
    currentTime = 0;

    this.maximumCacheSize = settings.maximumSize();
    this.sizeT1 = this.sizeT2 = this.sizeB1 = this.sizeB2 = 0;
    this.p = 0;

    this.source = new LogNormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);
    this.heapT1 = new SearchableMinHeap<>((int) maximumCacheSize, this::comparePrefixes);
    this.heapT2 = new SearchableMinHeap<>((int) maximumCacheSize, this::comparePrefixes);
    this.B1 = new Long2ObjectOpenHashMap<>();
    this.B2 = new Long2ObjectOpenHashMap<>();
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
    var existingPrefix = heapT1.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      heapT1.remove(existingPrefix.itemKey);
      policyStats.recordOperation();
      sizeT1 -= existingPrefix.currentSize;
      if (existingPrefix.isEmpty()) existingPrefix.queue = Q.NONE;
      policyStats.recordEviction();
    }

    existingPrefix = heapT2.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      heapT2.remove(existingPrefix.itemKey);
      policyStats.recordOperation();
      sizeT2 -= existingPrefix.currentSize;
      if (existingPrefix.isEmpty()) existingPrefix.queue = Q.NONE;
      policyStats.recordEviction();
    }

    if (B1.containsKey(event.key())) {
      // prefix exists in B1, remove it
      Prefix prefix = B1.remove(event.key());
      sizeB1 -= prefix.fullItemSize;
      if (prefix.isEmpty()) prefix.queue = Q.NONE;
      policyStats.recordEviction();
    }

    if (B2.containsKey(event.key())) {
      // prefix exists in B1, remove it
      Prefix prefix = B2.remove(event.key());
      sizeB2 -= prefix.fullItemSize;
      if (prefix.isEmpty()) prefix.queue = Q.NONE;
      policyStats.recordEviction();
    }
  }

  private void onRead(AccessEvent event) {
    long itemKey = event.key();
    Prefix prefix = Optional.ofNullable(heapT1.get(itemKey)).orElse(heapT2.get(itemKey));
    if (prefix == null) {
      long currentSize = event.itemSize();
      prefix = new Prefix(itemKey, currentSize, source, currentTime);
    } else {
      prefix.lastRequestTime = currentTime;
    }

    handleRequestsFrequency(prefix);

//    if (heapT1.valuesMap.values().stream().anyMatch(p -> p.currentSize == 0))
//      throw new IllegalStateException("Empty prefix in T1: " + currentTime);
//    if (heapT2.valuesMap.values().stream().anyMatch(p -> p.currentSize == 0))
//      throw new IllegalStateException("Empty prefix in T2: " + currentTime);
//
//    if (sizeT1 > maximumCacheSize - p)
//      throw new IllegalStateException("Size overflow in T1- maxSizeT1: " + (maximumCacheSize - p) + ", sizeT1: " + sizeT1 + ", currentTime: " + currentTime);
//    if (sizeT2 > p)
//      throw new IllegalStateException("Size overflow in T2- maxSizeT2: " + p + ", sizeT2: " + sizeT2 + ", currentTime: " + currentTime);
//
//    if (heapT1.valuesMap.values().stream().anyMatch(p -> p.queue != Q.T1))
//      throw new IllegalStateException("Wrong queue in T1: " + currentTime);
//    if (heapT2.valuesMap.values().stream().anyMatch(p -> p.queue != Q.T2))
//      throw new IllegalStateException("Wrong queue in T2: " + currentTime);
//
//    if (heapT1.valuesMap.values().stream().mapToLong(p -> p.currentSize).sum() != sizeT1 ||
//      heapT2.valuesMap.values().stream().mapToLong(p -> p.currentSize).sum() != sizeT2)
//      System.out.println("Size mismatch: " + currentTime);

    double latency;
    // System.out.println(currentTime);
    switch (prefix.queue) {
      case T1:
        if (event.operation() == READ) recordRequestStatistics(prefix, event.retrievalDelay());
        onHitT1(prefix);
        break;
      case T2:
        if (event.operation() == READ) recordRequestStatistics(prefix, event.retrievalDelay());
        onHitT2(prefix);
        break;
      case B1:
        if (event.operation() == READ) {
          policyStats.addDelay(event.retrievalDelay());
          latency = calculateLatency(event.retrievalDelay(), prefix.fullItemSize(), 0, Consts.BANDWIDTH);
          policyStats.addLatency(latency);
        }
        onHitB1(prefix);
        break;
      case B2:
        if (event.operation() == READ) {
          policyStats.addDelay(event.retrievalDelay());
          latency = calculateLatency(event.retrievalDelay(), prefix.fullItemSize(), 0, Consts.BANDWIDTH);
          policyStats.addLatency(latency);
        }
        onHitB2(prefix);
        break;
      case NONE:
        if (event.operation() == READ) {
          policyStats.addDelay(event.retrievalDelay());
          latency = calculateLatency(event.retrievalDelay(), prefix.fullItemSize(), 0, Consts.BANDWIDTH);
          policyStats.addLatency(latency);
        }
        onMiss(prefix);
        break;
    }
  }

  private void onHitT1(Prefix prefix) {
    if (prefix.currentSize > p)
      // The prefix is too large for T2, but it's already cached in T1, so it'll stay there
      return;

    moveToT2(prefix);
    if (prefix.queue == Q.T2) // Move to T2 can result in moving to B2 in case the prefix is too large
      waterFill(prefix);
  }

  private void onHitT2(Prefix prefix) {
    waterFill(prefix);
  }

  private void onHitB1(Prefix prefix) {
    if (prefix.queue == Q.T1 && prefix.currentSize > p)
      // The prefix is too large for T2, but it's already cached in T1, so it'll stay there
      return;

    moveToT2(prefix);
    if (prefix.queue == Q.T2) // Move to T2 can result in moving to B2 in case the prefix is too large
      waterFill(prefix);
    p = Math.min(maximumCacheSize, p + Math.max(sizeB2 / Math.max(sizeB1, 1), 1));
    evict();
  }

  private void onHitB2(Prefix prefix) {
    if (prefix.queue == Q.T1 && prefix.currentSize > p)
      // The prefix is too large for T2, but it's already cached in T1, so it'll stay there
      return;

    moveToT2(prefix);
    if (prefix.queue == Q.T2) // Move to T2 can result in moving to B2 in case the prefix is too large
      waterFill(prefix);
    p = Math.max(0, p - Math.max(sizeB1 / Math.max(sizeB2, 1), 1));
    evict();
  }

  private void onMiss(Prefix prefix) {
    moveToT1(prefix);
    if (prefix.queue == Q.T1) // Move to T1 can result in moving to B1 in case the prefix is too large
      waterFill(prefix);
  }

  private void moveToT1(Prefix prefix) {
    long maxSizeT1 = maximumCacheSize - p;
    if (prefix.fullItemSize > maxSizeT1) {
      prefix.queue = Q.B1;
      sizeB1 += prefix.fullItemSize;
      B1.put(prefix.itemKey, prefix);
      prefix.currentSize = 0;
      return;
    }

    if (prefix.queue != Q.NONE)
      throw new IllegalArgumentException("A move from " + prefix.queue + " to T1 doesn't suppose to happen - this is a bug!");

    if (prefix.currentSize > 0)
      throw new IllegalStateException("A non-empty prefix doesn't suppose to move to T1 - this is a bug!");

    // no need to waterDraw or add chunks to sizeT1,
    //  since T1 is the first queue, so the prefix entering is always empty at that point
    prefix.queue = Q.T1;
  }

  private void moveToT2(Prefix prefix) {
    long maxSizeT2 = p;
    if (prefix.fullItemSize > maxSizeT2) {
      if (heapT1.contains(prefix.itemKey)) {
        heapT1.remove(prefix.itemKey);
        policyStats.recordOperation();
        sizeT1 -= prefix.currentSize;
      }
      if (prefix.queue == Q.T1)
        prefix.queue = Q.B2;
      B2.put(prefix.itemKey, prefix);
      sizeB2 += prefix.fullItemSize;
      prefix.currentSize = 0;
      return;
    }

    if (heapT1.contains(prefix.itemKey)) {
      heapT1.remove(prefix.itemKey);
      policyStats.recordOperation();
      sizeT1 -= prefix.currentSize;
    } else if (prefix.queue == Q.B1) {
      B1.remove(prefix.itemKey);
      sizeB1 -= prefix.fullItemSize;
    } else if (prefix.queue == Q.B2) {
      B2.remove(prefix.itemKey);
      sizeB2 -= prefix.fullItemSize;
    }

    if ((prefix.queue == Q.NONE || prefix.queue == Q.B1 || prefix.queue == Q.B2) && prefix.currentSize > 0)
      throw new IllegalStateException("A non-empty prefix doesn't suppose to move to T2 from B1 or B2 or NONE - this is a bug!");

    if (prefix.currentSize > 0) {
      waterDraw(Q.T2, prefix.currentSize);
      sizeT2 += prefix.currentSize;
      heapT2.upsert(prefix.itemKey, prefix);
      policyStats.recordOperation();
    }
    prefix.queue = Q.T2;
  }

  private void waterDraw(Q queue, long spaceNeeded) {
    if (queue != Q.T1 && queue != Q.T2)
      throw new IllegalArgumentException("Invalid queue type for waterDraw: " + queue);

    if (spaceNeeded == 0) return;
    long currentHeapSize = (queue == Q.T1) ? sizeT1 : sizeT2;
    long max = (queue == Q.T1) ? (maximumCacheSize - p) : p;
    if (spaceNeeded > max)
      throw new IllegalArgumentException("Not enough space in the queue");

    SearchableMinHeap<Long, Prefix> heap = (queue == Q.T1) ? heapT1 : heapT2;
    while (currentHeapSize + spaceNeeded > max) {
      if (heap.isEmpty()) break;
      Prefix victim = heap.min().value();
      shrinkPrefix(victim);
      currentHeapSize = (queue == Q.T1) ? sizeT1 : sizeT2;
    }
  }

  private void waterFill(Prefix prefix) {
    if (prefix.queue != Q.T1 && prefix.queue != Q.T2)
      throw new IllegalArgumentException("Invalid queue type for waterFill: " + prefix.queue);

    long currentHeapSize = prefix.queue == Q.T1 ? sizeT1 : sizeT2;
    long maximumHeapSize = prefix.queue == Q.T1 ? (maximumCacheSize - p) : p;
    long addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);

    while (!prefix.isFull() && currentHeapSize + addSize <= maximumHeapSize) {
      extendPrefix(prefix);
      currentHeapSize = prefix.queue == Q.T1 ? sizeT1 : sizeT2;
      addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    }

    if (prefix.isFull()) return;

    Prefix victim;
    do {
      extendPrefix(prefix);
      do {
        victim = findVictim(prefix.queue);
        shrinkPrefix(victim);

        currentHeapSize = prefix.queue == Q.T1 ? sizeT1 : sizeT2;
        maximumHeapSize = prefix.queue == Q.T1 ? (maximumCacheSize - p) : p;
      } while (currentHeapSize > maximumHeapSize);

    } while (!(prefix.isFull() || victim.itemKey == prefix.itemKey));

    assert currentHeapSize <= maximumCacheSize : "Current cache " + prefix.queue + " size exceeds the maximum cache size (current time: " + currentTime + ")";
    assert currentHeapSize >= 0 : "Current cache " + prefix.queue + " size cannot be negative (current time: " + currentTime + ")";
  }

  private void extendPrefix(Prefix prefix) {
    if (prefix.queue != Q.T1 && prefix.queue != Q.T2)
      throw new IllegalArgumentException("Invalid queue type for extendPrefix: " + prefix.queue);

    if (prefix.isFull()) return;

    long addedSize = prefix.insertChunk();
    if (prefix.queue == Q.T1) sizeT1 += addedSize;
    else sizeT2 += addedSize;

    SearchableMinHeap<Long, Prefix> heap = prefix.queue == Q.T1 ? heapT1 : heapT2;
    heap.upsert(prefix.itemKey, prefix);
    policyStats.recordOperation();
    policyStats.recordAdmission();

    assert prefix.currentSize <= prefix.fullItemSize : "Prefix size exceeds its full size (current time: " + currentTime + ")";
  }

  private void shrinkPrefix(Prefix prefix) {
    if (prefix.queue != Q.T1 && prefix.queue != Q.T2)
      throw new IllegalArgumentException("Invalid queue type for shrinkPrefix: " + prefix.queue);

    if (prefix.isEmpty()) return;

    long removedSize = prefix.removeChunk();
    if (prefix.queue == Q.T1) sizeT1 -= removedSize;
    else sizeT2 -= removedSize;

    SearchableMinHeap<Long, Prefix> heap = prefix.queue == Q.T1 ? heapT1 : heapT2;
    if (!prefix.isEmpty()) {
      heap.upsert(prefix.itemKey, prefix);
      policyStats.recordOperation();
    }
    else if (heap.contains(prefix.itemKey)) {
      heap.remove(prefix.itemKey);
      policyStats.recordOperation();
    }
    policyStats.recordEviction();

    if (prefix.isEmpty())
      if (prefix.queue == Q.T1) prefix.queue = Q.B1;
      else prefix.queue = Q.B2;

    assert prefix.currentSize >= 0 : "Prefix size cannot be negative";
    assert sizeT1 >= 0 : "T1 size cannot be negative";
    assert sizeT2 >= 0 : "T2 size cannot be negative";
    assert sizeB1 >= 0 : "B1 size cannot be negative";
    assert sizeB2 >= 0 : "B2 size cannot be negative";
  }

  private Prefix findVictim(Q queue) {
    if (queue == Q.T1 && !heapT1.isEmpty()) return heapT1.min().value();
    if (queue == Q.T2 && !heapT2.isEmpty()) return heapT2.min().value();
    return null;
  }

  private void evict() {
    // Evict from T1 and T2 if they overflow
    long maxSizeT1 = maximumCacheSize - p;
    long t1Overflow = Math.max(0, sizeT1 - maxSizeT1);
    for (int i = 0; i < t1Overflow; i++) {
      Prefix victim = heapT1.min().value();
      shrinkPrefix(victim);
    }

    long maxSizeT2 = p;
    long t2Overflow = Math.max(0, sizeT2 - maxSizeT2);
    for (int i = 0; i < t2Overflow; i++) {
      Prefix victim = heapT2.min().value();
      shrinkPrefix(victim);
    }
  }

  private void handleRequestsFrequency(Prefix prefix) {
    prefix.requestsCountInPeriod++;
    requests.add(prefix.itemKey);
    if (requests.size() == Consts.REQUESTS_FREQUENCY_PERIOD + 1) {
      long last = requests.remove();
      Prefix lastPrefix = Optional.ofNullable(heapT1.get(last)).orElse(heapT2.get(last));
      if (lastPrefix == null) lastPrefix = Optional.ofNullable(B1.get(last)).orElse(B2.get(last));

      if (lastPrefix != null) {
        lastPrefix.requestsCountInPeriod--;
      }
    }
  }

  private void recordRequestStatistics(Prefix prefix, double delay) {
    double underflow = TimeCalculations.calculateUnderflowDelay(delay, prefix.fullItemSize(), prefix.currentSize(), Consts.BANDWIDTH);
    policyStats.addDelay(underflow);
    double latency = calculateLatency(delay, prefix.fullItemSize(), prefix.currentSize(), Consts.BANDWIDTH);
    policyStats.addLatency(latency);
  }

  public int comparePrefixes(long k1, long k2) {
    Prefix p1 = Optional.ofNullable(heapT1.get(k1)).orElse(heapT2.get(k1));
    Prefix p2 = Optional.ofNullable(heapT1.get(k2)).orElse(heapT2.get(k2));
    assert p1 != null;
    assert p2 != null;
    return p1.lruCompareTo(p2);
  }

  @Override
  public void finished() {
    Policy.super.finished();
    checkState(sizeT1 + sizeT2 <= maximumCacheSize, "resident size overflow");

    long countedT1 = heapT1.valuesMap.values().stream().filter(p -> p.queue == Q.T1).mapToLong(p -> p.currentSize).sum();
    long countedT2 = heapT2.valuesMap.values().stream().filter(p -> p.queue == Q.T2).mapToLong(p -> p.currentSize).sum();
    checkState(countedT1 == sizeT1, "T1 mismatch");
    checkState(countedT2 == sizeT2, "T2 mismatch");
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  static public class Prefix {
    final long itemKey, fullItemSize;
    final Source source;
    long currentSize;
    long requestsCountInPeriod;
    long lastRequestTime;
    Q queue = Q.NONE;

    public Prefix(long itemKey, long fullItemSize, Source source, long currentTime) {
      this.itemKey = itemKey;
      this.fullItemSize = fullItemSize;
      this.source = source;
      this.lastRequestTime = currentTime;
      this.currentSize = 0;
    }

    public long insertChunk() {
      long addSize = Math.min(fullItemSize - currentSize, Consts.CHUNK_SIZE);
      currentSize += addSize;
      return addSize;
    }

    public long removeChunk() {
      long remainder = currentSize % Consts.CHUNK_SIZE;
      long removeSize = (remainder > 0) ? remainder : Consts.CHUNK_SIZE;
      currentSize -= removeSize;
      return removeSize;
    }

    public boolean isFull() {
      return currentSize == fullItemSize;
    }

    public boolean isEmpty() {
      return currentSize == 0;
    }

    public double currentSize() {
      return currentSize;
    }

    public double fullItemSize() {
      return fullItemSize;
    }

    public double recency() {
      return 1.0 / (currentTime - lastRequestTime + 1);
    }

    public double lruScore() {
      double t = TimeCalculations.calculateTransmissionTime(currentSize, Consts.BANDWIDTH);
      return recency() * (1 - source.calculateCDF(t));
    }

    public int lruCompareTo(Prefix other) {
      return Double.compare(this.lruScore(), other.lruScore());
    }
  }
}
