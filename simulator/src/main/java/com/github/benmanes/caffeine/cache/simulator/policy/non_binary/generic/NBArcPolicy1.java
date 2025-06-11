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
public final class NBArcPolicy1 implements Policy {
  final Long2ObjectMap<Prefix> data;
  final Queue<Long> requests;
  static long currentTime;
  final long maximumCacheSize;
  long sizeT1, sizeT2, sizeB1, sizeB2, sizeResident, p;
  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Prefix> heapT1;
  final SearchableMinHeap<Long, Prefix> heapT2;
  final Source source;
  int hits;

  enum Q {T1, T2, B1, B2, NONE}

  public NBArcPolicy1(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.data = new Long2ObjectOpenHashMap<>();
    this.requests = new ArrayDeque<>();
    currentTime = 0;

    this.maximumCacheSize = settings.maximumSize();
    this.sizeT1 = this.sizeT2 = this.sizeB1 = this.sizeB2 = this.sizeResident = 0;
    this.p = 0;

    this.source = new NormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);
    this.heapT1 = new SearchableMinHeap<>((int) maximumCacheSize, this::comparePrefixes);
    this.heapT2 = new SearchableMinHeap<>((int) maximumCacheSize, this::comparePrefixes);
    hits = 0;
  }

  @Override
  public void record(AccessEvent event) {
    policyStats.recordOperation();
    currentTime++;
    System.out.println(currentTime);
    long itemKey = event.key();
    Prefix prefix = data.get(itemKey);
    if (prefix == null) {
      long chunksAmount = event.itemSize(); // (long) Math.ceil(event.itemSize() / (Consts.CHUNK_SIZE * 1024 * 1024));
      prefix = new Prefix(itemKey, chunksAmount, source, currentTime);
      data.put(itemKey, prefix);
    } else {
      prefix.lastRequestTime = currentTime;
    }

    handleRequestsFrequency(prefix);

    switch (prefix.queue) {
      case T1:
        recordRequestStatistics(prefix, event.retrievalDelay());
        onHitT1(prefix);
        hits++;
        break;
      case T2:
        recordRequestStatistics(prefix, event.retrievalDelay());
        onHitT2(prefix);
        hits++;
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
    moveToT2(prefix);
    waterFill(prefix);
  }

  private void onHitT2(Prefix prefix) {
    waterFill(prefix);
  }

  private void onHitB1(Prefix prefix) {
    p = Math.min(maximumCacheSize, p + Math.max(1, sizeB2 / Math.max(1, sizeB1)));

    if (sizeResident >= maximumCacheSize) {
      if ((sizeT1 >= 1) && (sizeT1 > p)) {
        evictResident(Q.T1);
      } else {
        evictResident(Q.T2);
      }
    }

    sizeB1 -= prefix.chunksAmount;
    moveToT2(prefix);
    waterFill(prefix);
  }

  private void onHitB2(Prefix prefix) {
    p = Math.max(0, p - Math.max(1, sizeB1 / Math.max(1, sizeB2)));

    if ((sizeT1 >= 1) && ((sizeT1 > p) || (sizeT1 == p))) {
      evictResident(Q.T1);
    } else {
      evictResident(Q.T2);
    }

    sizeB2 -= prefix.chunksAmount;
    moveToT2(prefix);
    waterFill(prefix);
  }

  private void onMiss(Prefix prefix) {
    long L1 = sizeT1 + sizeB1;
    long L2 = sizeT2 + sizeB2;

    if (L1 == maximumCacheSize) {
      if (sizeT1 < maximumCacheSize) {
        evictGhost(Q.B1);
      } else {
        evictResident(Q.T1);
      }
    } else if ((L1 < maximumCacheSize) && ((L1 + L2) >= maximumCacheSize)) {
      if ((L1 + L2) >= 2 * maximumCacheSize) {
        evictGhost(Q.B2);
      }
      if (sizeResident >= maximumCacheSize) {
        if ((sizeT1 >= 1) && (sizeT1 > p)) {
          evictResident(Q.T1);
        } else {
          evictResident(Q.T2);
        }
      }
    }

    moveToT1(prefix);
    waterFill(prefix);
  }

  private void moveToT1(Prefix prefix) {
    long available = maximumCacheSize - sizeT2;
    if (prefix.fullItemChunksAmount > available) {
      prefix.queue = Q.B1;
      prefix.chunksAmount = 0;
      sizeB1 += prefix.fullItemChunksAmount;
      return;
    }


    if (prefix.queue == Q.T2) {
      waterDraw(Q.T1, prefix.chunksAmount);
      heapT2.remove(prefix.itemKey);
      sizeT2 -= prefix.chunksAmount;
    } else if (prefix.queue == Q.B1) sizeB1 -= prefix.chunksAmount;
    else if (prefix.queue == Q.B2) sizeB2 -= prefix.chunksAmount;

    if (prefix.queue == Q.NONE || prefix.queue == Q.B1 || prefix.queue == Q.B2)
      prefix.chunksAmount = 0;
    prefix.queue = Q.T1;
    heapT1.upsert(prefix.itemKey, prefix);
  }

  private void moveToT2(Prefix prefix) {
    long available = maximumCacheSize - sizeT1;
    if (prefix.fullItemChunksAmount > available) {
      prefix.queue = Q.B2;
      prefix.chunksAmount = 0;
      sizeB2 += prefix.fullItemChunksAmount;
      return;
    }


    if (prefix.queue == Q.T1) {
      waterDraw(Q.T2, prefix.chunksAmount);
      heapT1.remove(prefix.itemKey);
      sizeT1 -= prefix.chunksAmount;
    } else if (prefix.queue == Q.B1) sizeB1 -= prefix.chunksAmount;
    else if (prefix.queue == Q.B2) sizeB2 -= prefix.chunksAmount;

    if (prefix.queue == Q.NONE || prefix.queue == Q.B1 || prefix.queue == Q.B2)
      prefix.chunksAmount = 0;
    prefix.queue = Q.T2;
    heapT2.upsert(prefix.itemKey, prefix);
  }

  private void waterDraw(Q queue, long spaceNeeded) {
    if (spaceNeeded == 0) System.out.println("Water draw called with 0 space needed");
    long currentHeapSize = (queue == Q.T1) ? sizeT1 : sizeT2;
    long max = (queue == Q.T1) ? (maximumCacheSize - sizeT2) : (maximumCacheSize - sizeT1);
    if (spaceNeeded > max)
      throw new IllegalArgumentException("Not enough space in the queue");

    SearchableMinHeap<Long, Prefix> heap = (queue == Q.T1) ? heapT1 : heapT2;
    while (currentHeapSize + spaceNeeded > max) {
      if (heap.isEmpty()) return;
      Prefix victim = heap.min().value();
      shrinkPrefix(victim);
      currentHeapSize = (queue == Q.T1) ? sizeT1 : sizeT2;
    }
  }

  private void waterFill(Prefix prefix) {
    long currentHeapSize = prefix.queue == Q.T1 || prefix.queue == Q.B1 ? sizeT1 : sizeT2;
    long maximumHeapSize = prefix.queue == Q.T1 || prefix.queue == Q.B1
      ? (maximumCacheSize - sizeT2)
      : (maximumCacheSize - sizeT1);
    while (!prefix.isFull() && currentHeapSize < maximumHeapSize) {
      extendPrefix(prefix);
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
    if (prefix.isFull()) return;

    SearchableMinHeap<Long, Prefix> heap = prefix.queue == Q.T1 ? heapT1 : heapT2;
//    if (heap.contains(prefix.itemKey)) heap.remove(prefix.itemKey);

    prefix.insertChunk();
    sizeResident++;
    if (prefix.queue == Q.T1) sizeT1++;
    else if (prefix.queue == Q.T2) sizeT2++;

    if (!prefix.isEmpty()) heap.upsert(prefix.itemKey, prefix);
    else if (heap.contains(prefix.itemKey)) heap.remove(prefix.itemKey);
    policyStats.recordAdmission();
  }

  private void shrinkPrefix(Prefix prefix) {
    if (prefix.isEmpty()) return;

    SearchableMinHeap<Long, Prefix> heap = prefix.queue == Q.T1 ? heapT1 : heapT2;
//    if (heap.contains(prefix.itemKey)) heap.remove(prefix.itemKey);

    prefix.removeChunk();
    sizeResident--;
    if (prefix.queue == Q.T1) sizeT1--;
    else if (prefix.queue == Q.T2) sizeT2--;

    if (!prefix.isEmpty()) heap.upsert(prefix.itemKey, prefix);
    else heap.remove(prefix.itemKey);
    policyStats.recordEviction();

    if (prefix.isEmpty()) {
      if (prefix.queue == Q.T1) prefix.queue = Q.B1;
      else prefix.queue = Q.B2;
    }
  }

  private Prefix findVictim(Q queue) {
    if (queue == Q.T1 && !heapT1.isEmpty()) return heapT1.min().value();
    if (queue == Q.T2 && !heapT2.isEmpty()) return heapT2.min().value();
    return null;
  }

  private void evictResident(Q queue) {
    Prefix victim = findVictim(queue);
    if (victim == null) return;

    SearchableMinHeap<Long, Prefix> heap = (queue == Q.T1) ? heapT1 : heapT2;
    if (heap.contains(victim.itemKey)) heap.remove(victim.itemKey);

    if (queue == Q.T1) sizeT1 -= victim.chunksAmount;
    else sizeT2 -= victim.chunksAmount;
    sizeResident -= victim.chunksAmount;

    victim.queue = (queue == Q.T1) ? Q.B1 : Q.B2;
    if (victim.queue == Q.B1) sizeB1 += victim.chunksAmount;
    else sizeB2 += victim.chunksAmount;

    policyStats.recordEviction();
  }


  private void evictGhost(Q queue) {
    for (var prefix : data.values()) {
      if (prefix.queue == queue) {
        data.remove(prefix.itemKey);
        if (queue == Q.B1) sizeB1 -= prefix.chunksAmount;
        else sizeB2 -= prefix.chunksAmount;
        prefix.chunksAmount = 0;
        prefix.queue = Q.NONE;
        break;
      }
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
    checkState(sizeB1 + sizeB2 <= maximumCacheSize, "ghost size overflow");

    long countedT1 = data.values().stream().filter(p -> p.queue == Q.T1).mapToLong(p -> p.chunksAmount).sum();
    long countedT2 = data.values().stream().filter(p -> p.queue == Q.T2).mapToLong(p -> p.chunksAmount).sum();
    checkState(countedT1 == sizeT1, "T1 mismatch");
    checkState(countedT2 == sizeT2, "T2 mismatch");

    long countedB1 = data.values().stream().filter(p -> p.queue == Q.B1).mapToLong(p -> p.fullItemChunksAmount).sum();
    long countedB2 = data.values().stream().filter(p -> p.queue == Q.B2).mapToLong(p -> p.fullItemChunksAmount).sum();
    checkState(countedB1 == sizeB1, "B1 mismatch");
    checkState(countedB2 == sizeB2, "B2 mismatch");

    System.out.println("Non-Binary ARC Hits: " + hits);
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
