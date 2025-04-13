package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.score_based;

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

@Policy.PolicySpec(name = "non-binary.score-based.Arc")
public final class NBArcPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
  final Queue<Long> requests;
  static long currentTime;
  final long maximumCacheSize;
  long sizeT1, sizeT2, sizeB1, sizeB2, sizeResident, p;
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
    this.sizeT1 = this.sizeT2 = this.sizeB1 = this.sizeB2 = this.sizeResident = 0;
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
      prefix = new Prefix(itemKey, event.itemSize(), source, currentTime);
      data.put(itemKey, prefix);
    } else {
      prefix.lastRequestTime = currentTime;
    }

    recordRequestStatistics(prefix, event.retrievalDelay());
    handleRequestsFrequency(prefix);

    switch (prefix.queue) {
      case T1:
        onHitT1(prefix);
        break;
      case T2:
        onHitT2(prefix);
        break;
      case B1:
        onHitB1(prefix);
        break;
      case B2:
        onHitB2(prefix);
        break;
      case NONE:
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
    p = Math.min(maximumCacheSize, p + prefix.chunksAmount);
    waterDraw(Q.T2, prefix.chunksAmount);
    moveToT2(prefix);
    waterFill(prefix);
  }

  private void onHitB2(Prefix prefix) {
    p = Math.max(0, p - prefix.chunksAmount);
    waterDraw(Q.T2, prefix.chunksAmount);
    moveToT2(prefix);
    waterFill(prefix);
  }

  private void onMiss(Prefix prefix) {
    long L1 = sizeT1 + sizeB1;
    long L2 = sizeT2 + sizeB2;

    if (L1 == maximumCacheSize) {
      if (sizeT1 < maximumCacheSize) evictGhost(Q.B1);
      else evictResident(Q.T1);
    } else if (L1 < maximumCacheSize && L1 + L2 >= maximumCacheSize) {
      if (L1 + L2 >= 2 * maximumCacheSize) evictGhost(Q.B2);
    }

    moveToT1(prefix);
    waterFill(prefix);
  }

  private void moveToT1(Prefix prefix) {
    // Ensure space in T2 before admitting
    long maximumT1Size = p;
    if (prefix.chunksAmount > maximumT1Size || maximumT1Size == 0) {
      if (heapT1.contains(prefix.itemKey)) heapT1.remove(prefix.itemKey);
      prefix.queue = Q.B1;
      sizeB1 += prefix.chunksAmount;
      prefix.chunksAmount = 0;
      return;
    }

    waterDraw(Q.T1, prefix.chunksAmount);
    if (prefix.queue == Q.T2) {
      heapT2.remove(prefix.itemKey);
      sizeT2 -= prefix.chunksAmount;
    } else if (prefix.queue == Q.B1) sizeB1 -= prefix.chunksAmount;
    else if (prefix.queue == Q.B2) sizeB2 -= prefix.chunksAmount;

    prefix.queue = Q.T1;
    sizeT1 += prefix.chunksAmount;
    if (!prefix.isEmpty()) {
      if (heapT1.contains(prefix.itemKey)) heapT1.remove(prefix.itemKey);
      heapT1.insert(prefix.itemKey, prefix);
    }
  }

  private void moveToT2(Prefix prefix) {
    // Ensure space in T2 before admitting
    long maximumT2Size = maximumCacheSize - p;
    if (prefix.chunksAmount > maximumT2Size || maximumT2Size == 0) {
      if (heapT2.contains(prefix.itemKey)) heapT2.remove(prefix.itemKey);
      prefix.queue = Q.B2;
      sizeB2 += prefix.chunksAmount;
      prefix.chunksAmount = 0;
      return;
    }
    waterDraw(Q.T2, prefix.chunksAmount);
    if (prefix.queue == Q.T1) {
      heapT1.remove(prefix.itemKey);
      sizeT1 -= prefix.chunksAmount;
    } else if (prefix.queue == Q.B1) sizeB1 -= prefix.chunksAmount;
    else if (prefix.queue == Q.B2) sizeB2 -= prefix.chunksAmount;

    prefix.queue = Q.T2;
    sizeT2 += prefix.chunksAmount;
    if (!prefix.isEmpty()) {
      if (heapT2.contains(prefix.itemKey)) heapT2.remove(prefix.itemKey);
      heapT2.insert(prefix.itemKey, prefix);
    }
  }


  private void waterDraw(Q queue, long spaceNeeded) {
    long currentHeapSize = (queue == Q.T1) ? sizeT1 : sizeT2;
    long max = (queue == Q.T1) ? p : maximumCacheSize - p;
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
    long maximumHeapSize = prefix.queue == Q.T1 || prefix.queue == Q.B1 ? p : maximumCacheSize - p;
    while (!prefix.isFull() && currentHeapSize < maximumHeapSize) {
      extendPrefix(prefix);
    }
    while (true) {
      Prefix victim = findVictim(prefix.queue);
      if (victim == null) break;
      double sPlus = prefix.lruScoreAfterInsertion();
      double sMinus = victim.lruScoreAfterEviction();
      if (prefix.isFull() || victim.itemKey == prefix.itemKey || sPlus < sMinus) break;
      shrinkPrefix(victim);
      extendPrefix(prefix);
    }
  }

  private void extendPrefix(Prefix prefix) {
    if (prefix.isFull()) return;

    SearchableMinHeap<Long, Prefix> heap = prefix.queue == Q.T1 ? heapT1 : heapT2;
    if (heap.contains(prefix.itemKey)) heap.remove(prefix.itemKey);

    prefix.insertChunk();
    sizeResident++;
    if (prefix.queue == Q.T1) sizeT1++;
    else if (prefix.queue == Q.T2) sizeT2++;

    heap.insert(prefix.itemKey, prefix);
    policyStats.recordAdmission();
  }

  private void shrinkPrefix(Prefix prefix) {
    if (prefix.isEmpty()) return;

    SearchableMinHeap<Long, Prefix> heap = prefix.queue == Q.T1 ? heapT1 : heapT2;
    if (heap.contains(prefix.itemKey)) heap.remove(prefix.itemKey);

    prefix.removeChunk();
    sizeResident--;
    if (prefix.queue == Q.T1) sizeT1--;
    else if (prefix.queue == Q.T2) sizeT2--;

    if (!prefix.isEmpty()) heap.insert(prefix.itemKey, prefix);
    policyStats.recordEviction();

    if (prefix.isEmpty()) {
      if (prefix.queue == Q.T1) {
        prefix.queue = Q.B1;
        sizeB1 += prefix.chunksAmount;
      } else {
        prefix.queue = Q.B2;
        sizeB2 += prefix.chunksAmount;
      }
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

    victim.chunksAmount = 0;
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

    public double lruScoreAfterInsertion() {
      if (isFull()) return 0;
      double t = TimeCalculations.calculateTransmissionTime(sizeInMB() + Consts.CHUNK_SIZE, Consts.BANDWIDTH);
      return recency() * (1 - source.calculateCDF(t));
    }

    public double lruScoreAfterEviction() {
      if (isEmpty()) return recency();
      double t = TimeCalculations.calculateTransmissionTime(sizeInMB() - Consts.CHUNK_SIZE, Consts.BANDWIDTH);
      return recency() * (1 - source.calculateCDF(t));
    }

    public int lruCompareTo(Prefix other) {
      return Double.compare(this.lruScore(), other.lruScore());
    }
  }
}
