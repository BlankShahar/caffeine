package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.generic;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.countmin4.PeriodicResetCountMin4;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.SearchableMinHeap;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.NormalSource;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;
import com.typesafe.config.Config;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

import static com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent.Operation.READ;
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
  private final PolicyStats policyStats;
  final Queue<Long> requests;
  private final SearchableMinHeap<Long, Prefix> heapLRU;  // recency order
  private final SearchableMinHeap<Long, Prefix> heapLFU;  // score order
  private final Source source = new NormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);

  private final long maximumCacheSize;
  private long maxCacheSizeLRU;
  private long maxCacheSizeLFU;

  private long currentCacheSizeLRU;
  private long currentCacheSizeLFU;

  /* hill‑climber parameters */
  private double q = 1;            // virtual slope parameter
  private double ratio = 0.5;      // = 1 / 2^q  – fraction of window (LRU)
  private double previousTotalDelay = 0;
  private double currentTotalDelay = 0;
  private long opCounter = 0;

  private final PeriodicResetCountMin4 sketch;

  /* time */
  private static long currentTime = 0;

  public NBHillClimberWindowTinyLfuPolicy(Config cfg) {
    var settings = new BasicSettings(cfg);
    this.maximumCacheSize = settings.maximumSize();
    this.maxCacheSizeLRU = maximumCacheSize / 2;
    this.maxCacheSizeLFU = maximumCacheSize - maxCacheSizeLRU;

    this.policyStats = new PolicyStats(name());
    this.sketch = new PeriodicResetCountMin4(settings.config());
    this.requests = new ArrayDeque<>();
    this.heapLRU = new SearchableMinHeap<>((int) maximumCacheSize, this::compareLRU);
    this.heapLFU = new SearchableMinHeap<>((int) maximumCacheSize, this::compareLFU);

    REFINEMENT_INTERVAL = 1_000_000;
    STEP_SIZE = 0.05;
  }

  private void handleRequestFrequency(Prefix prefix) {
    sketch.increment(prefix.itemKey);
    prefix.frequency = sketch.frequency(prefix.itemKey);

    if (heapLRU.contains(prefix.itemKey)) {
      heapLRU.upsert(prefix.itemKey, prefix);
      policyStats.recordOperation();
    } else if (heapLFU.contains(prefix.itemKey)) {
      heapLFU.upsert(prefix.itemKey, prefix);
      policyStats.recordOperation();
    }
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
    var existingPrefix = heapLRU.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      heapLRU.remove(existingPrefix.itemKey);
      policyStats.recordOperation();
      currentCacheSizeLRU -= existingPrefix.currentSize;
      policyStats.recordEviction();
    }

    existingPrefix = heapLFU.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      heapLFU.remove(existingPrefix.itemKey);
      policyStats.recordOperation();
      currentCacheSizeLFU -= existingPrefix.currentSize;
      policyStats.recordEviction();
    }
  }

  private void onRead(AccessEvent event) {
    opCounter++;
    long key = event.key();
    Prefix p = Optional.ofNullable(heapLFU.get(key)).orElse(heapLRU.get(key));
    if (p == null) {
      long currentSize = event.itemSize();
      p = new Prefix(key, currentSize, source, currentTime);
    }

    /* stats bookkeeping */
    p.lastAccessTime = currentTime;
    p.frequency++;
    if (event.operation() == READ) recordDelayStats(p, event.retrievalDelay());
    updateParameters(event.retrievalDelay());
    handleRequestFrequency(p);

    if (maximumCacheSize == 0) return;

    if (currentCacheSizeLRU + currentCacheSizeLFU >= maximumCacheSize)
      sketch.ensureCapacity(2_000_000);

    /* routing logic */
    if (maxCacheSizeLFU == 0 || heapLRU.contains(key)) {
      waterFillLru(p);
    } else if (maxCacheSizeLRU == 0 || heapLFU.contains(key)) {
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
    if (newMaxLRU < maxCacheSizeLRU) {            // shrink LRU, grow LFU
      long toMove = maxCacheSizeLRU - newMaxLRU;
      maxCacheSizeLRU = newMaxLRU;
      maxCacheSizeLFU = newMaxLFU;

      while (toMove > 0 && !heapLRU.isEmpty()) {
        Prefix victim = heapLRU.min().value();
        movePrefixToLfu(victim);
        toMove -= victim.currentSize;
      }
    } else {     // grow LRU, shrink LFU
      long toMove = newMaxLRU - maxCacheSizeLRU;
      maxCacheSizeLRU = newMaxLRU;
      maxCacheSizeLFU = newMaxLFU;

      while (toMove > 0 && !heapLFU.isEmpty()) {
        Prefix victim = heapLFU.min().value();
        movePrefixToLru(victim);
        toMove -= victim.currentSize;
      }
    }

    maxCacheSizeLRU = newMaxLRU;
    maxCacheSizeLFU = newMaxLFU;

    previousTotalDelay = currentTotalDelay;
    currentTotalDelay = 0;
  }

  private void waterDrawLru(long spaceNeeded) {
    if (spaceNeeded > maxCacheSizeLRU) return;

    while (currentCacheSizeLRU + spaceNeeded > maxCacheSizeLRU) {
      Prefix victim = heapLRU.min().value();
      shrinkPrefixLRU(victim);
    }
  }

  private void waterDrawLfu(long spaceNeeded) {
    if (spaceNeeded > maxCacheSizeLFU) return;
    while (currentCacheSizeLFU + spaceNeeded > maxCacheSizeLFU) {
      Prefix victim = heapLFU.min().value();
      shrinkPrefixLFU(victim);
    }
  }

  /* ------------------------------  LRU cache (window)  -------------------------- */
  private void waterFillLru(Prefix prefix) {
    long addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    while (!prefix.isFull() && // stop if the prefix is full
      currentCacheSizeLRU + addSize <= maxCacheSizeLRU // stop if adding another chunk would exceed the maximum cache size
    ) {
      extendPrefixLRU(prefix);
      addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    }

    if (prefix.isFull()) return;

    Prefix victim;
    do {
      extendPrefixLRU(prefix);

      do {
        victim = heapLRU.min().value();
        movePrefixToLfu(victim);
        if (prefix.itemKey == victim.itemKey) {
          waterFillLfu(prefix);
          break;
        }
      } while (currentCacheSizeLRU > maxCacheSizeLRU);

    } while (!(prefix.isFull() || victim.itemKey == prefix.itemKey));

    assert currentCacheSizeLRU <= maxCacheSizeLRU : "Current LRU cache size exceeds the maximum cache size (current time: " + currentTime + ")";
    assert currentCacheSizeLRU >= 0 : "Current LRU cache size cannot be negative (current time: " + currentTime + ")";
  }

  private void movePrefixToLru(Prefix v) {
    currentCacheSizeLFU -= v.currentSize;
    heapLFU.remove(v.itemKey);
    policyStats.recordOperation();

    if (v.currentSize > maxCacheSizeLRU) {
      // There isn't enough space for the whole prefix
      // so move part of the prefix, until the LRU cache is full
      long available_space = maxCacheSizeLRU - currentCacheSizeLRU;
      if (v.fullItemSize >= Consts.CHUNK_SIZE) {
        long available_space_in_chunks = available_space / Consts.CHUNK_SIZE * Consts.CHUNK_SIZE;
        v.currentSize = available_space_in_chunks;
        currentCacheSizeLRU += available_space_in_chunks;
      } else currentCacheSizeLRU += v.currentSize;

    } else {
      waterDrawLru(v.currentSize);
      currentCacheSizeLRU += v.currentSize;
    }
    if (!v.isEmpty()) {
      heapLRU.upsert(v.itemKey, v);
      policyStats.recordOperation();
    }
  }

  /* ------------------------------  LFU cache (main)  --------------------------- */
  private void waterFillLfu(Prefix prefix) {
    long addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    while (!prefix.isFull() && // stop if the prefix is full
      currentCacheSizeLFU + addSize <= maxCacheSizeLFU // stop if adding another chunk would exceed the maximum cache size
    ) {
      extendPrefixLFU(prefix);
      addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    }

    if (prefix.isFull()) return;

    Prefix victim;
    do {
      extendPrefixLFU(prefix);

      do {
        victim = heapLFU.min().value();
        shrinkPrefixLFU(victim);
      } while (currentCacheSizeLFU > maxCacheSizeLFU);

    } while (!(prefix.isFull() || victim.itemKey == prefix.itemKey));

    assert currentCacheSizeLFU <= maxCacheSizeLFU : "Current LFU cache size exceeds the maximum cache size (current time: " + currentTime + ")";
    assert currentCacheSizeLFU >= 0 : "Current LFU cache size cannot be negative (current time: " + currentTime + ")";
  }

  private void shrinkPrefixLFU(Prefix prefix) {
    if (prefix.isEmpty())
      return;

    long removedSize = prefix.removeChunk();
    currentCacheSizeLFU -= removedSize;
    updateHeap(heapLFU, prefix);

    policyStats.recordEviction();

    assert prefix.currentSize >= 0 : "Prefix size cannot be negative";
    assert currentCacheSizeLFU >= 0 : "Current LFU cache size cannot be negative";
  }

  private void extendPrefixLRU(Prefix prefix) {
    if (prefix.isFull())
      return;

    long addedSize = prefix.insertChunk();
    currentCacheSizeLRU += addedSize;
    updateHeap(heapLRU, prefix);

    policyStats.recordAdmission();

    assert prefix.currentSize <= prefix.fullItemSize : "Prefix size exceeds its full size (current time: " + currentTime + ")";
  }

  private void extendPrefixLFU(Prefix prefix) {
    if (prefix.isFull())
      return;

    long addedSize = prefix.insertChunk();
    currentCacheSizeLFU += addedSize;
    updateHeap(heapLFU, prefix);

    policyStats.recordAdmission();

    assert prefix.currentSize <= prefix.fullItemSize : "Prefix size exceeds its full size (current time: " + currentTime + ")";
  }

  private void shrinkPrefixLRU(Prefix prefix) {
    if (prefix.isEmpty())
      return;

    long removedSize = prefix.removeChunk();
    currentCacheSizeLRU -= removedSize;
    updateHeap(heapLRU, prefix);

    policyStats.recordEviction();

    assert prefix.currentSize >= 0 : "Prefix size cannot be negative";
    assert currentCacheSizeLRU >= 0 : "Current LRU cache size cannot be negative";
  }

  private void movePrefixToLfu(Prefix v) {
    currentCacheSizeLRU -= v.currentSize;
    heapLRU.remove(v.itemKey);
    policyStats.recordOperation();

    if (v.currentSize > maxCacheSizeLFU) {
      // There isn't enough space for the whole prefix
      // so move part of the prefix, until the LFU cache is full
      long available_space = maxCacheSizeLFU - currentCacheSizeLFU;
      if (v.fullItemSize >= Consts.CHUNK_SIZE) {
        long available_space_in_chunks = available_space / Consts.CHUNK_SIZE * Consts.CHUNK_SIZE;
        v.currentSize = available_space_in_chunks;
        currentCacheSizeLFU += available_space_in_chunks;
      } else currentCacheSizeLFU += v.currentSize;
    } else {
      waterDrawLfu(v.currentSize);
      currentCacheSizeLFU += v.currentSize;
    }
    if (!v.isEmpty()) {
      heapLFU.upsert(v.itemKey, v);
      policyStats.recordOperation();
    }
  }

  /* ------------------------------  helpers  ------------------------------------ */
  private void recordDelayStats(Prefix p, double srcDelay) {
    policyStats.addDelay(TimeCalculations.calculateUnderflowDelay(srcDelay, p.fullItemSize(), p.currentSize(), Consts.BANDWIDTH));
    double latency = calculateLatency(srcDelay, p.fullItemSize(), p.currentSize(), Consts.BANDWIDTH);
    policyStats.addLatency(latency);
  }

  private void updateHeap(SearchableMinHeap<Long, Prefix> heap, Prefix prefix) {
    if (prefix.isEmpty() && heap.contains(prefix.itemKey)) {
      heap.remove(prefix.itemKey);
      policyStats.recordOperation();
    }
    else {
      heap.upsert(prefix.itemKey, prefix);
      policyStats.recordOperation();
    }
  }

  private int compareLRU(long a, long b) {
    Prefix p1 = heapLRU.get(a);
    Prefix p2 = heapLRU.get(b);
    assert p1 != null;
    assert p2 != null;
    return Double.compare(p1.lruScore(), p2.lruScore());
  }

  private int compareLFU(long a, long b) {
    Prefix p1 = heapLFU.get(a);
    Prefix p2 = heapLFU.get(b);
    assert p1 != null;
    assert p2 != null;
    return Double.compare(p1.lfuScore(), p2.lfuScore());
  }

  /* ------------------------------  plumbing  ----------------------------------- */
  @Override
  public PolicyStats stats() {
    return policyStats;
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
    final long itemKey, fullItemSize;
    final Source source;
    long currentSize, lastAccessTime, frequency;

    Prefix(long k, long fullChunks, Source src, long time) {
      itemKey = k;
      fullItemSize = fullChunks;
      source = src;
      currentSize = 0;
      lastAccessTime = time;
      frequency = 0;
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

    boolean isFull() {
      return currentSize == fullItemSize;
    }

    boolean isEmpty() {
      return currentSize == 0;
    }

    double currentSize() {
      return currentSize;
    }

    double fullItemSize() {
      return fullItemSize;
    }

    double frequency() {
      return (double) frequency;
    }

    double lfuScore() {
      double t = currentSize();
      return frequency() * (1 - source.calculateCDF(TimeCalculations.calculateTransmissionTime(t, Consts.BANDWIDTH)));
    }

    public double lruScore() {
      // Idea - recency times the probability of not experiencing delay
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(currentSize, Consts.BANDWIDTH);
      return recency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double recency() {
      return (double) 1 / (currentTime - lastAccessTime + 1);
    }
  }
}
