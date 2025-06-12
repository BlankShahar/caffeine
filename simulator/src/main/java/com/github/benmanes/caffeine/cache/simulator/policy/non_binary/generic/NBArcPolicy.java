package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.generic;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.SearchableMinHeap;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.NormalSource;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayDeque;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkState;


@Policy.PolicySpec(name = "non-binary.Arc")
public final class NBArcPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
  final Queue<Long> requests;
  static long currentTime;
  final long maximumCacheSize;
  long sizeT1, sizeT2, sizeB1, sizeB2, p;
  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Prefix> heapT1;
  final SearchableMinHeap<Long, Prefix> heapT2;
  final Source source;

  enum Q {T1, T2, B1, B2, NONE}

  public NBArcPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.data = new Long2ObjectOpenHashMap<>();
    this.requests = new ArrayDeque<>();
    currentTime = 0;

    this.maximumCacheSize = settings.maximumSize();
    this.sizeT1 = this.sizeT2 = this.sizeB1 = this.sizeB2 = 0;
    this.p = 0;

    this.source = new NormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);
    this.heapT1 = new SearchableMinHeap<>((int) maximumCacheSize, this::comparePrefixes);
    this.heapT2 = new SearchableMinHeap<>((int) maximumCacheSize, this::comparePrefixes);
  }

  @Override
  public void record(AccessEvent event) {
    policyStats.recordOperation();
    currentTime++;
    long itemKey = event.key();
    Prefix prefix = data.get(itemKey);
    if (prefix == null) {
      long chunksAmount = event.itemSize();
      prefix = new Prefix(itemKey, chunksAmount, source, currentTime);
      data.put(itemKey, prefix);
    } else {
      prefix.lastRequestTime = currentTime;
    }

    handleRequestsFrequency(prefix);

//    if (heapT1.valuesMap.values().stream().anyMatch(p -> p.chunksAmount == 0))
//      throw new IllegalStateException("Empty prefix in T1: " + currentTime);
//    if (heapT2.valuesMap.values().stream().anyMatch(p -> p.chunksAmount == 0))
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
//    if (heapT1.valuesMap.values().stream().mapToLong(p -> p.chunksAmount).sum() != sizeT1 ||
//      heapT2.valuesMap.values().stream().mapToLong(p -> p.chunksAmount).sum() != sizeT2)
//      System.out.println("Size mismatch: " + currentTime);

    switch (prefix.queue) {
      case T1:
        recordRequestStatistics(prefix, event.retrievalDelay());
        onHitT1(prefix);
        break;
      case T2:
        recordRequestStatistics(prefix, event.retrievalDelay());
        onHitT2(prefix);
        break;
      case B1:
        policyStats.addDelay(event.retrievalDelay());
        onHitB1(prefix);
        break;
      case B2:
        policyStats.addDelay(event.retrievalDelay());
        onHitB2(prefix);
        break;
      case NONE:
        policyStats.addDelay(event.retrievalDelay());
        onMiss(prefix);
        break;
    }
  }

  private void onHitT1(Prefix prefix) {
    if (prefix.chunksAmount > p)
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
    if (prefix.queue == Q.T1 && prefix.chunksAmount > p)
      // The prefix is too large for T2, but it's already cached in T1, so it'll stay there
      return;

    moveToT2(prefix);
    if (prefix.queue == Q.T2) // Move to T2 can result in moving to B2 in case the prefix is too large
      waterFill(prefix);
    p = Math.min(maximumCacheSize, p + Math.max(sizeB2 / sizeB1, 1));
    evict();
  }

  private void onHitB2(Prefix prefix) {
    if (prefix.queue == Q.T1 && prefix.chunksAmount > p)
      // The prefix is too large for T2, but it's already cached in T1, so it'll stay there
      return;

    moveToT2(prefix);
    if (prefix.queue == Q.T2) // Move to T2 can result in moving to B2 in case the prefix is too large
      waterFill(prefix);
    p = Math.max(0, p - Math.max(sizeB1 / sizeB2, 1));
    evict();
  }

  private void onMiss(Prefix prefix) {
    moveToT1(prefix);
    if (prefix.queue == Q.T1) // Move to T1 can result in moving to B1 in case the prefix is too large
      waterFill(prefix);
  }

  private void moveToT1(Prefix prefix) {
    long maxSizeT1 = maximumCacheSize - p;
    if (prefix.fullItemChunksAmount > maxSizeT1) {
      prefix.queue = Q.B1;
      prefix.chunksAmount = 0;
      sizeB1 += prefix.fullItemChunksAmount;
      return;
    }

    if (prefix.queue != Q.NONE)
      throw new IllegalArgumentException("A move from " + prefix.queue + " to T1 doesn't suppose to happen - this is a bug!");

    if (prefix.chunksAmount > 0)
      throw new IllegalStateException("A non-empty prefix doesn't suppose to move to T1 - this is a bug!");

    // no need to waterDraw or add chunks to sizeT1,
    //  since T1 is the first queue, so the prefix entering is always empty at that point
    prefix.queue = Q.T1;
  }

  private void moveToT2(Prefix prefix) {
    long maxSizeT2 = p;
    if (prefix.fullItemChunksAmount > maxSizeT2) {
      if (heapT1.contains(prefix.itemKey)) {
        heapT1.remove(prefix.itemKey);
        sizeT1 -= prefix.chunksAmount;
      }
      if (prefix.queue == Q.T1)
        prefix.queue = Q.B2;
      prefix.chunksAmount = 0;
      sizeB2 += prefix.fullItemChunksAmount;
      return;
    }

    if (heapT1.contains(prefix.itemKey)) {
      heapT1.remove(prefix.itemKey);
      sizeT1 -= prefix.chunksAmount;
    } else if (prefix.queue == Q.B1) sizeB1 -= prefix.fullItemChunksAmount;
    else if (prefix.queue == Q.B2) sizeB2 -= prefix.fullItemChunksAmount;

    if ((prefix.queue == Q.NONE || prefix.queue == Q.B1 || prefix.queue == Q.B2) && prefix.chunksAmount > 0)
      throw new IllegalStateException("A non-empty prefix doesn't suppose to move to T2 from B1 or B2 or NONE - this is a bug!");

    if (prefix.chunksAmount > 0) {
      waterDraw(Q.T2, prefix.chunksAmount);
      sizeT2 += prefix.chunksAmount;
      heapT2.upsert(prefix.itemKey, prefix);
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

    while (!prefix.isFull() && currentHeapSize < maximumHeapSize) {
      extendPrefix(prefix);
      currentHeapSize = prefix.queue == Q.T1 ? sizeT1 : sizeT2;
    }

    if (prefix.isFull()) return;


    Prefix victim;
    do {
      victim = findVictim(prefix.queue);
      if (victim == null || victim.itemKey == prefix.itemKey) break;
      shrinkPrefix(victim);
      extendPrefix(prefix);
    } while (!prefix.isFull());
  }

  private void extendPrefix(Prefix prefix) {
    if (prefix.queue != Q.T1 && prefix.queue != Q.T2)
      throw new IllegalArgumentException("Invalid queue type for extendPrefix: " + prefix.queue);

    if (prefix.isFull()) return;

    prefix.insertChunk();
    if (prefix.queue == Q.T1) sizeT1++;
    else sizeT2++;

    SearchableMinHeap<Long, Prefix> heap = prefix.queue == Q.T1 ? heapT1 : heapT2;
    heap.upsert(prefix.itemKey, prefix);
    policyStats.recordAdmission();
  }

  private void shrinkPrefix(Prefix prefix) {
    if (prefix.queue != Q.T1 && prefix.queue != Q.T2)
      throw new IllegalArgumentException("Invalid queue type for shrinkPrefix: " + prefix.queue);

    if (prefix.isEmpty()) return;

    prefix.removeChunk();
    if (prefix.queue == Q.T1) sizeT1--;
    else sizeT2--;

    SearchableMinHeap<Long, Prefix> heap = prefix.queue == Q.T1 ? heapT1 : heapT2;
    if (!prefix.isEmpty()) heap.upsert(prefix.itemKey, prefix);
    else if (heap.contains(prefix.itemKey)) heap.remove(prefix.itemKey);
    policyStats.recordEviction();

    if (prefix.isEmpty())
      if (prefix.queue == Q.T1) prefix.queue = Q.B1;
      else prefix.queue = Q.B2;
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
      Prefix lastPrefix = data.get(last);
      if (lastPrefix != null) lastPrefix.requestsCountInPeriod--;
    }
  }

  private void recordRequestStatistics(Prefix prefix, double delay) {
    double idealSize = Math.min(prefix.fullItemSizeInMB(), delay * Consts.BANDWIDTH);
    long idealChunks = (long) Math.ceil(idealSize / Consts.CHUNK_SIZE);

    policyStats.addHits(prefix.chunksAmount);
    policyStats.addMisses(Math.max(0, idealChunks - prefix.chunksAmount));

    double underflow = TimeCalculations.calculateUnderflowDelay(delay, prefix.fullItemSizeInMB(), prefix.sizeInMB(), Consts.BANDWIDTH);
    policyStats.addDelay(underflow);
  }

  public int comparePrefixes(long k1, long k2) {
    return data.get(k1).lruCompareTo(data.get(k2));
  }

  @Override
  public void finished() {
    Policy.super.finished();
    checkState(sizeT1 + sizeT2 <= maximumCacheSize, "resident size overflow");

    long countedT1 = heapT1.valuesMap.values().stream().filter(p -> p.queue == Q.T1).mapToLong(p -> p.chunksAmount).sum();
    long countedT2 = heapT2.valuesMap.values().stream().filter(p -> p.queue == Q.T2).mapToLong(p -> p.chunksAmount).sum();
    checkState(countedT1 == sizeT1, "T1 mismatch");
    checkState(countedT2 == sizeT2, "T2 mismatch");
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  static public class Prefix {
    final long itemKey, fullItemChunksAmount;
    final Source source;
    long chunksAmount;
    long requestsCountInPeriod;
    long lastRequestTime;
    Q queue = Q.NONE;

    public Prefix(long itemKey, long fullItemChunksAmount, Source source, long currentTime) {
      this.itemKey = itemKey;
      this.fullItemChunksAmount = fullItemChunksAmount;
      this.source = source;
      this.lastRequestTime = currentTime;
      this.chunksAmount = 0;
    }

    public void insertChunk() {
      if (!isFull()) chunksAmount++;
    }

    public void removeChunk() {
      if (chunksAmount > 0) chunksAmount--;
    }

    public boolean isFull() {
      return chunksAmount == fullItemChunksAmount;
    }

    public boolean isEmpty() {
      return chunksAmount == 0;
    }

    public double sizeInMB() {
      return chunksAmount * Consts.CHUNK_SIZE;
    }

    public double fullItemSizeInMB() {
      return fullItemChunksAmount * Consts.CHUNK_SIZE;
    }

    public double recency() {
      return 1.0 / (currentTime - lastRequestTime + 1);
    }

    public double lruScore() {
      double t = TimeCalculations.calculateTransmissionTime(sizeInMB(), Consts.BANDWIDTH);
      return recency() * (1 - source.calculateCDF(t));
    }

    public int lruCompareTo(Prefix other) {
      return Double.compare(this.lruScore(), other.lruScore());
    }
  }
}
