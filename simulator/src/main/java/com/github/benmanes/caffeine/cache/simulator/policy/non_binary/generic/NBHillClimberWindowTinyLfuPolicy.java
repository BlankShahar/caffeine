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

import static com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations.calculateLatency;

/**
 * Non‑binary Hill‑Climber Window‑TinyLFU.
 * cacheLRU  – admission window (recency based)
 * cacheLFU  – main cache (frequency based)
 */
@Policy.PolicySpec(name = "non-binary.HillClimberWindowTinyLFU")
public final class NBHillClimberWindowTinyLfuPolicy implements Policy {

  /* ------------------------------  configuration  ------------------------------ */
  private final int REFINEMENT_INTERVAL; // = 1_000_000;   // operations per hill‑climb step
  private final double STEP_SIZE;        // Δq

  /* ------------------------------  global state  -------------------------------- */
  private final PolicyStats stats;
  private final Long2ObjectMap<Prefix> data;
  private final SearchableMinHeap<Long, Prefix> heapLRU;  // recency order
  private final SearchableMinHeap<Long, Prefix> heapLFU;  // score order
  private final Source source = new NormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);

  /* cache capacities (chunks) */
  private final long maximumCacheSize;
  private long maxCacheLRU;
  private long maxCacheLFU;

  /* current usage (chunks) */
  private long sizeLRU;
  private long sizeLFU;

  /* hill‑climber parameters */
  private double q = 1;            // virtual slope parameter
  private double ratio = 0.5;      // = 1 / 2^q  – fraction of window (LRU)
  private double previousTotalDelay = 0;
  private double currentTotalDelay = 0;
  private long opCounter = 0;

  /* time */
  private static long now = 0;

  public NBHillClimberWindowTinyLfuPolicy(Config cfg) {
    var settings = new BasicSettings(cfg);
    this.maximumCacheSize = settings.maximumSize();
    this.maxCacheLRU = maximumCacheSize / 2;
    this.maxCacheLFU = maximumCacheSize - maxCacheLRU;

    this.stats = new PolicyStats(name());
    this.data = new Long2ObjectOpenHashMap<>();
    this.heapLRU = new SearchableMinHeap<>((int) maximumCacheSize, this::compareLRU);
    this.heapLFU = new SearchableMinHeap<>((int) maximumCacheSize, this::compareLFU);

    REFINEMENT_INTERVAL = 1_000_000;
    STEP_SIZE = 0.05;
  }

  /* ------------------------------  main entry  ---------------------------------- */
  @Override
  public void record(AccessEvent event) {
    now++;
    opCounter++;
    stats.recordOperation();
    long key = event.key();
    Prefix p = data.get(key);
    if (p == null) {
      long chunksAmount = event.itemSize(); // (long) Math.ceil(event.itemSize() / (Consts.CHUNK_SIZE * 1024 * 1024));
      p = new Prefix(key, chunksAmount, source, now);
      data.put(key, p);
    }

    /* stats bookkeeping */
    p.lastAccessTime = now;
    p.requestCount++;
    recordDelayStats(p, event.retrievalDelay());
    updateParameters(event.retrievalDelay());

    /* routing logic */
    if (maxCacheLFU == 0 || heapLRU.contains(key)) {
      waterFillLru(p);
    } else if (maxCacheLRU == 0 || heapLFU.contains(key)) {
      waterFillLfu(p);
    } else {
      waterFillLru(p);                       // first‑time admission
    }
  }

  /* ------------------------------  hill‑climber  -------------------------------- */
  private void updateParameters(double retrievalDelay) {
    currentTotalDelay += retrievalDelay;
    if (opCounter % REFINEMENT_INTERVAL != 0) return;

    if (currentTotalDelay < previousTotalDelay) {
      q += STEP_SIZE;
    } else {
      q = Math.max(0, q - STEP_SIZE);
    }

    ratio = 1 / Math.pow(2, q);
    long newMaxLRU = (long) Math.floor(ratio * maximumCacheSize);
    long newMaxLFU = maximumCacheSize - newMaxLRU;

    /* rebalance by moving prefixes */
    if (newMaxLRU < maxCacheLRU) {            // shrink LRU, grow LFU
      long toMove = maxCacheLRU - newMaxLRU;
      maxCacheLRU = newMaxLRU;
      maxCacheLFU = newMaxLFU;

      while (toMove > 0 && !heapLRU.isEmpty()) {
        Prefix victim = heapLRU.min().value();
        movePrefixToLfu(victim);
        toMove -= victim.chunksAmount;
      }
    } else {     // grow LRU, shrink LFU
      long toMove = newMaxLRU - maxCacheLRU;
      maxCacheLRU = newMaxLRU;
      maxCacheLFU = newMaxLFU;

      while (toMove > 0 && !heapLFU.isEmpty()) {
        Prefix victim = heapLFU.min().value();
        movePrefixToLru(victim);
        toMove -= victim.chunksAmount;
      }
    }

    maxCacheLRU = newMaxLRU;
    maxCacheLFU = newMaxLFU;

    previousTotalDelay = currentTotalDelay;
    currentTotalDelay = 0;
  }

  private void waterDrawLru(long spaceNeeded) {
    if (spaceNeeded > maxCacheLRU) return;
    while (sizeLRU + spaceNeeded > maxCacheLRU) {
      Prefix victim = heapLRU.min().value();
      shrinkPrefixLRU(victim);
//      long evictionSize = Math.min(victim.chunksAmount, spaceNeeded);
//      shrinkPrefixLruBySize(victim, evictionSize);
    }
  }

  private void waterDrawLfu(long spaceNeeded) {
    if (spaceNeeded > maxCacheLFU) return;
    while (sizeLFU + spaceNeeded > maxCacheLFU) {
      Prefix victim = heapLFU.min().value();
      shrinkPrefixLFU(victim);
//      long evictionSize = Math.min(victim.chunksAmount, spaceNeeded);
//      shrinkPrefixLfuBySize(victim, evictionSize);
    }
  }

  /* ------------------------------  LRU cache (window)  -------------------------- */
  private void waterFillLru(Prefix prefix) {
    long available_space = Math.min(maxCacheLRU - sizeLRU, prefix.fullItemChunksAmount - prefix.chunksAmount);
    prefix.chunksAmount += available_space;
    sizeLRU += available_space;
    if (available_space > 0) updateHeap(heapLRU, prefix);

    if (prefix.isFull()) return;

    Prefix victim;
    do {
      victim = heapLRU.min().value();
      movePrefixToLfu(victim);
      if (prefix.itemKey == victim.itemKey) {
        waterFillLfu(prefix);
        break;
      }

      available_space = Math.min(maxCacheLRU - sizeLRU, prefix.fullItemChunksAmount - prefix.chunksAmount);
      prefix.chunksAmount += available_space;
      sizeLRU += available_space;
      updateHeap(heapLRU, prefix);
    } while (!(prefix.isFull() || victim.itemKey == prefix.itemKey));
  }

  private void movePrefixToLru(Prefix v) {
    sizeLFU -= v.chunksAmount;
    heapLFU.remove(v.itemKey);

    if (v.chunksAmount > maxCacheLRU) {
      // There isn't enough space for the whole prefix
      // so move part of the prefix, until the LRU cache is full
      long available_space = maxCacheLRU - sizeLRU;
      v.chunksAmount = available_space;
      sizeLRU += available_space;
    } else {
      waterDrawLru(v.chunksAmount);
      sizeLRU += v.chunksAmount;
    }
    if (!v.isEmpty()) heapLRU.upsert(v.itemKey, v);
  }

  /* ------------------------------  LFU cache (main)  --------------------------- */
  private void waterFillLfu(Prefix prefix) {
    while (!prefix.isFull() && sizeLFU < maxCacheLFU) {
      extendPrefixLFU(prefix);
    }

//    long fillUpSize = Math.min(
//      prefix.fullItemChunksAmount - prefix.chunksAmount,
//      maxCacheLFU - sizeLFU
//    );
//    extendPrefixLfuBySize(prefix, fillUpSize);

    if (prefix.isFull()) return;

    Prefix victim;
    do {
      victim = heapLFU.min().value();
      shrinkPrefixLFU(victim);
      extendPrefixLFU(prefix);
    } while (!(prefix.isFull() || victim.itemKey == prefix.itemKey));
  }

  private void shrinkPrefixLFU(Prefix prefix) {
    if (prefix.isEmpty()) {
      return;
    }
    prefix.removeChunk();
    sizeLFU--;
    updateHeap(heapLFU, prefix);

    stats.recordOperation();
    stats.recordEviction();
  }

  private void shrinkPrefixLfuBySize(Prefix prefix, long size) {
    if (prefix.chunksAmount - size < 0)
      throw new IllegalArgumentException("Cannot shrink prefix #" + prefix.itemKey + " below zero chunks");
    if (sizeLFU - size < 0)
      throw new IllegalArgumentException("Cannot shrink prefix #" + prefix.itemKey + " below zero size in LFU cache");
    prefix.chunksAmount -= size;
    sizeLFU -= size;
    updateHeap(heapLFU, prefix);

    stats.recordOperation();
    stats.recordEviction();
  }

  private void extendPrefixLFU(Prefix prefix) {
    if (prefix.isFull()) {
      return;
    }
    prefix.insertChunk();
    sizeLFU++;
    updateHeap(heapLFU, prefix);

    stats.recordOperation();
    stats.recordAdmission();
  }

  private void extendPrefixLfuBySize(Prefix prefix, long size) {
    if (prefix.chunksAmount + size > prefix.fullItemChunksAmount)
      throw new IllegalArgumentException("Cannot extend prefix #" + prefix.itemKey + " beyond its full size");
    if (prefix.chunksAmount + size > maxCacheLFU)
      throw new IllegalArgumentException("Cannot extend prefix #" + prefix.itemKey + " beyond LFU cache size");

    prefix.chunksAmount += size;
    sizeLFU += size;

    updateHeap(heapLFU, prefix);

    stats.recordOperation();
    stats.recordAdmission();
  }

  private void shrinkPrefixLRU(Prefix prefix) {
    if (prefix.isEmpty()) {
      return;
    }
    prefix.removeChunk();
    sizeLRU--;
    updateHeap(heapLRU, prefix);

    stats.recordOperation();
    stats.recordEviction();
  }

  private void shrinkPrefixLruBySize(Prefix prefix, long size) {
    if (prefix.chunksAmount - size < 0)
      throw new IllegalArgumentException("Cannot shrink prefix #" + prefix.itemKey + " below zero chunks");
    if (sizeLRU - size < 0)
      throw new IllegalArgumentException("Cannot shrink prefix #" + prefix.itemKey + " below zero size in LRU cache");
    prefix.chunksAmount -= size;
    sizeLRU -= size;
    updateHeap(heapLRU, prefix);

    stats.recordOperation();
    stats.recordEviction();
  }

  private void movePrefixToLfu(Prefix v) {
    sizeLRU -= v.chunksAmount;
    heapLRU.remove(v.itemKey);

    if (v.chunksAmount > maxCacheLFU) {
      // There isn't enough space for the whole prefix
      // so move part of the prefix, until the LFU cache is full
      long available_space = maxCacheLFU - sizeLFU;
      v.chunksAmount = available_space;
      sizeLFU += available_space;
    } else {
      waterDrawLfu(v.chunksAmount);
      sizeLFU += v.chunksAmount;
    }
    if (!v.isEmpty()) heapLFU.upsert(v.itemKey, v);
  }

  /* ------------------------------  helpers  ------------------------------------ */
  private void recordDelayStats(Prefix p, double srcDelay) {
    double ideal = Math.min(p.fullSizeInMB(), srcDelay * Consts.BANDWIDTH);
    long idealChunks = (long) Math.ceil(ideal / Consts.CHUNK_SIZE);
    stats.addHits(p.chunksAmount);
    stats.addMisses(Math.max(0, idealChunks - p.chunksAmount));
    stats.addDelay(TimeCalculations.calculateUnderflowDelay(srcDelay, p.fullSizeInMB(), p.sizeInMB(), Consts.BANDWIDTH));
    double latency = calculateLatency(srcDelay, p.fullSizeInMB(), p.sizeInMB(), Consts.BANDWIDTH);
    stats.addLatency(latency);
  }

  private void updateHeap(SearchableMinHeap<Long, Prefix> heap, Prefix prefix) {
    if (prefix.isEmpty() && heap.contains(prefix.itemKey)) heap.remove(prefix.itemKey);
    else heap.upsert(prefix.itemKey, prefix);
  }

  private int compareLRU(long a, long b) {
    return Double.compare(data.get(a).lruScore(), data.get(b).lruScore());
  }

  private int compareLFU(long a, long b) {
    return Double.compare(data.get(a).lfuScore(), data.get(b).lfuScore());
  }

  /* ------------------------------  plumbing  ----------------------------------- */
  @Override
  public PolicyStats stats() {
    return stats;
  }

  @Override
  public void finished() {
  }

  @Override
  public String name() {
    return Policy.super.name();
  }

  /* ------------------------------  inner prefix class  ------------------------- */
  static final class Prefix {
    final long itemKey, fullItemChunksAmount;
    final Source source;
    long chunksAmount, lastAccessTime, requestCount;

    Prefix(long k, long fullChunks, Source src, long time) {
      itemKey = k;
      fullItemChunksAmount = fullChunks;
      source = src;
      chunksAmount = 0;
      lastAccessTime = time;
      requestCount = 0;
    }

    void insertChunk() {
      if (chunksAmount < fullItemChunksAmount) chunksAmount++;
    }

    void removeChunk() {
      if (chunksAmount > 0) chunksAmount--;
    }

    boolean isFull() {
      return chunksAmount == fullItemChunksAmount;
    }

    boolean isEmpty() {
      return chunksAmount == 0;
    }

    double sizeInMB() {
      return chunksAmount * Consts.CHUNK_SIZE;
    }

    double fullSizeInMB() {
      return fullItemChunksAmount * Consts.CHUNK_SIZE;
    }

    double frequency() {
      return (double) requestCount / Consts.REQUESTS_FREQUENCY_PERIOD;
    }

    double lfuScore() {
      double t = sizeInMB();
      return frequency() * (1 - source.calculateCDF(TimeCalculations.calculateTransmissionTime(t, Consts.BANDWIDTH)));
    }

    public double lruScore() {
      // Idea - recency times the probability of not experiencing delay
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(sizeInMB(), Consts.BANDWIDTH);
      return recency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double recency() {
      return (double) 1 / (now - lastAccessTime + 1);
    }
  }
}
