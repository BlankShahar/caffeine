package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.black_box;

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

/**
 * Non‑binary Hill‑Climber Window‑TinyLFU.
 * cacheLRU  – admission window (recency based)
 * cacheLFU  – main cache (frequency based)
 */
@Policy.PolicySpec(name = "non-binary.black-box.HillClimberWindowTinyLFU")
public final class BBNBHillClimberWindowTinyLfuPolicy implements Policy {

  /* ------------------------------  configuration  ------------------------------ */
  private static final int REFINEMENT_INTERVAL = 1_000_000;   // operations per hill‑climb step
  private static final double STEP_SIZE = 0.05;        // Δq

  /* ------------------------------  global state  -------------------------------- */
  private final PolicyStats stats;
  private final Long2ObjectMap<Prefix> data;
  private final SearchableMinHeap<Long, Prefix> heapLRU;  // recency order
  private final SearchableMinHeap<Long, Prefix> heapLFU;  // score order
  private final Source source = new NormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);

  /* cache capacities (chunks) */
  private final long fullCacheSize;
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

  public BBNBHillClimberWindowTinyLfuPolicy(Config cfg) {
    var settings = new BasicSettings(cfg);
    this.fullCacheSize = settings.maximumSize();
    this.maxCacheLRU = fullCacheSize / 2;
    this.maxCacheLFU = fullCacheSize - maxCacheLRU;

    this.stats = new PolicyStats(name());
    this.data = new Long2ObjectOpenHashMap<>();
    this.heapLRU = new SearchableMinHeap<>((int) fullCacheSize, this::compareLRU);
    this.heapLFU = new SearchableMinHeap<>((int) fullCacheSize, this::compareLFU);
  }

  /* ------------------------------  main entry  ---------------------------------- */
  @Override
  public void record(AccessEvent e) {
    now++;
    opCounter++;
    stats.recordOperation();
    long key = e.key();
    Prefix p = data.get(key);
    if (p == null) {
      p = new Prefix(key, e.itemSize(), source, now);
      data.put(key, p);
    }

    /* stats bookkeeping */
    p.lastAccessTime = now;
    p.requestCount++;
    recordDelayStats(p, e.retrievalDelay());
    updateParameters(e.retrievalDelay());

    /* routing logic */
    if (heapLRU.contains(key)) {
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
    long newMaxLRU = (long) Math.floor(ratio * fullCacheSize);
    long newMaxLFU = fullCacheSize - newMaxLRU;

    /* rebalance by moving prefixes */
    if (newMaxLRU < maxCacheLRU) {            // shrink LRU, grow LFU
      long toMove = maxCacheLRU - newMaxLRU;
      maxCacheLRU = newMaxLRU;
      maxCacheLFU = newMaxLFU;

      while (toMove > 0 && !heapLRU.isEmpty()) {
        Prefix victim = heapLRU.min().value();
        heapLRU.remove(victim.itemKey);
        sizeLRU -= victim.chunksAmount;
        toMove -= victim.chunksAmount;
        movePrefixToLfu(victim);
      }
    } else {     // grow LRU, shrink LFU
      long toMove = newMaxLRU - maxCacheLRU;
      maxCacheLRU = newMaxLRU;
      maxCacheLFU = newMaxLFU;

      while (toMove > 0 && !heapLFU.isEmpty()) {
        Prefix victim = heapLFU.min().value();
        heapLFU.remove(victim.itemKey);
        sizeLFU -= victim.chunksAmount;
        toMove -= victim.chunksAmount;
        movePrefixToLru(victim);
      }
    }

    maxCacheLRU = newMaxLRU;
    maxCacheLFU = newMaxLFU;

    previousTotalDelay = currentTotalDelay;
    currentTotalDelay = 0;
  }

  private void waterDrawLru(long spaceNeeded) {
    if (spaceNeeded > maxCacheLRU)
      return;
    while (sizeLRU + spaceNeeded > maxCacheLRU) {
      Prefix victim = heapLRU.min().value();
      shrinkPrefixLRU(victim);
    }
  }

  private void waterDrawLfu(long spaceNeeded) {
    if (spaceNeeded > maxCacheLFU)
      return;
    while (sizeLFU + spaceNeeded > maxCacheLFU) {
      Prefix victim = heapLFU.min().value();
      shrinkPrefixLFU(victim);
    }
  }

  /* ------------------------------  LRU cache (window)  -------------------------- */
  private void waterFillLru(Prefix p) {
    long available_space = Math.min(maxCacheLRU - sizeLRU, p.fullItemChunksAmount - p.chunksAmount);
    p.chunksAmount += available_space;
    sizeLRU += available_space;
    updateHeap(heapLRU, p);
    if (p.isFull()) return;

    while (true) {
      Prefix victim = heapLRU.min().value();
      if (p.isFull() || p.itemKey == victim.itemKey) break;

      heapLRU.remove(victim.itemKey);
      sizeLRU -= victim.chunksAmount;

      movePrefixToLfu(victim);

      available_space = Math.min(maxCacheLRU - sizeLRU, p.fullItemChunksAmount - p.chunksAmount);
      p.chunksAmount += available_space;
      sizeLRU += available_space;
      updateHeap(heapLRU, p);
      if (p.isFull()) break;
    }
  }

  private void movePrefixToLru(Prefix v) {
    if (v.chunksAmount > maxCacheLRU) {
      v.chunksAmount = 0;
      if (heapLFU.contains(v.itemKey)) heapLFU.remove(v.itemKey);
      return;
    }
    waterDrawLru(v.chunksAmount);
    sizeLRU += v.chunksAmount;
    heapLRU.insert(v.itemKey, v);
  }

  /* ------------------------------  LFU cache (main)  --------------------------- */
  private void waterFillLfu(Prefix prefix) {
    while (!prefix.isFull() && sizeLFU < maxCacheLFU) {
      extendPrefixLFU(prefix);
    }

    while (true) {
      Prefix victim = heapLFU.min().value();
      if (prefix.isFull() || victim.itemKey == prefix.itemKey)
        break;
      shrinkPrefixLFU(victim);
      extendPrefixLFU(prefix);
    }
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

  private void movePrefixToLfu(Prefix v) {
    if (v.chunksAmount > maxCacheLFU) {
      v.chunksAmount = 0;
      if (heapLRU.contains(v.itemKey)) heapLRU.remove(v.itemKey);
      return;
    }
    waterDrawLfu(v.chunksAmount);
    sizeLFU += v.chunksAmount;
    heapLFU.insert(v.itemKey, v);
  }

  /* ------------------------------  helpers  ------------------------------------ */
  private void recordDelayStats(Prefix p, double srcDelay) {
    double ideal = Math.min(p.fullSizeInMB(), srcDelay * Consts.BANDWIDTH);
    long idealChunks = (long) Math.ceil(ideal / Consts.CHUNK_SIZE);
    stats.addHits(p.chunksAmount);
    stats.addMisses(Math.max(0, idealChunks - p.chunksAmount));
    stats.addDelay(TimeCalculations.calculateUnderflowDelay(srcDelay, p.fullSizeInMB(), p.sizeInMB(), Consts.BANDWIDTH));
  }

  private void updateHeap(SearchableMinHeap<Long, Prefix> heap, Prefix p) {
    if (heap.contains(p.itemKey)) heap.remove(p.itemKey);
    if (!p.isEmpty()) heap.insert(p.itemKey, p);
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
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
        sizeInMB(),
        Consts.BANDWIDTH
      );
      return recency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double recency() {
      return (double) 1 / (now - lastAccessTime + 1);
    }
  }
}
