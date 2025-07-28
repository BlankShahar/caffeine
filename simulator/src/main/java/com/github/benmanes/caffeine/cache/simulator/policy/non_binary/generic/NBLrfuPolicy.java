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

import static com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent.Operation.READ;
import static com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations.calculateLatency;

@Policy.PolicySpec(name = "non-binary.LRFU")
public final class NBLrfuPolicy implements Policy {
  long currentTime;
  final long maximumCacheSize;
  long currentCacheSize;
  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Prefix> scoreMinHeap;
  Source source;

  public NBLrfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    currentTime = 0;

    this.scoreMinHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::comparePrefixes);
    this.source = new NormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);

    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0;
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
    var existingPrefix = scoreMinHeap.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      scoreMinHeap.remove(existingPrefix.itemKey);
      currentCacheSize -= existingPrefix.currentSize;
      policyStats.recordEviction();
      policyStats.recordOperation();
    }
  }

  private void onRead(AccessEvent event) {
    long itemKey = event.key();
    var existingPrefix = scoreMinHeap.get(itemKey);

    if (existingPrefix != null) {
      existingPrefix.lastRequestTime = currentTime;
      onRequest(existingPrefix, event.retrievalDelay(), event.operation());
    } else {
      long currentSize = event.itemSize();
      var newPrefix = new Prefix(itemKey, currentSize, source, currentTime);
      onRequest(newPrefix, event.retrievalDelay(), event.operation());
    }
  }

  private void onRequest(Prefix prefix, double sourceDelay, AccessEvent.Operation operation) {
    if (operation == READ) recordRequestStatistics(prefix, sourceDelay);
    prefix.updateScore(currentTime);
    waterFill(prefix);
  }

  private void recordRequestStatistics(Prefix old, double sourceDelay) {
    double underflowDelay = calculateUnderflowDelay(sourceDelay, old);
    policyStats.addDelay(underflowDelay);
    double latency = calculateLatency(sourceDelay, old.fullItemSize(), old.currentSize(), Consts.BANDWIDTH);
    policyStats.addLatency(latency);
  }

  private void waterFill(Prefix prefix) {
    long addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    while (!prefix.isFull() && // stop if the prefix is full
      currentCacheSize + addSize <= maximumCacheSize // stop if adding another chunk would exceed the maximum cache size
    ) {
      extendPrefix(prefix);
      addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    }

    if (prefix.isFull() || maximumCacheSize == 0) return;

    Prefix victim;
    do {
      extendPrefix(prefix);
      do {
        victim = findVictim();
        shrinkPrefix(victim);
      } while (currentCacheSize > maximumCacheSize);

    } while (!(prefix.isFull() || victim.itemKey == prefix.itemKey));

    assert currentCacheSize <= maximumCacheSize : "Current cache size exceeds the maximum cache size (current time: " + currentTime + ")";
    assert currentCacheSize >= 0 : "Current cache size cannot be negative (current time: " + currentTime + ")";
  }

  private void shrinkPrefix(Prefix prefix) {
    if (prefix.isEmpty())
      return;

    long removedSize = prefix.removeChunk();
    currentCacheSize -= removedSize;
    if (prefix.isEmpty())
      scoreMinHeap.remove(prefix.itemKey);
    else
      scoreMinHeap.upsert(prefix.itemKey, prefix);

    policyStats.recordOperation();
    policyStats.recordEviction();

    assert prefix.currentSize >= 0 : "Prefix size cannot be negative";
    assert currentCacheSize >= 0 : "Current cache size cannot be negative";
  }

  private void extendPrefix(Prefix prefix) {
    if (prefix.isFull())
      return;

    long addedSize = prefix.insertChunk();
    currentCacheSize += addedSize;
    scoreMinHeap.upsert(prefix.itemKey, prefix);

    policyStats.recordOperation();
    policyStats.recordAdmission();

    assert prefix.currentSize <= prefix.fullItemSize : "Prefix size exceeds its full size (current time: " + currentTime + ")";
  }

  private Prefix findVictim() {
    return scoreMinHeap.min().value();
  }

  private static double calculateUnderflowDelay(double sourceDelay, Prefix prefix) {
    return TimeCalculations.calculateUnderflowDelay(
      sourceDelay,
      prefix.fullItemSize(),
      prefix.currentSize(),
      Consts.BANDWIDTH
    );
  }

  public int comparePrefixes(long prefixKey1, long prefixKey2) {
    Prefix p1 = scoreMinHeap.get(prefixKey1);
    Prefix p2 = scoreMinHeap.get(prefixKey2);
    if (p1 == null || p2 == null) {
      throw new IllegalArgumentException("Prefixes not found in the heap");
    }
    return p1.lrfuCompareTo(p2);
  }

  @Override
  public void finished() {
    Policy.super.finished();
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  @Override
  public String name() {
    return Policy.super.name();
  }

  static public class Prefix {
    final long itemKey, fullItemSize;
    final Source source;
    long currentSize;
    long lastRequestTime;

    double score;
    static final double LAMBDA = 2.0;

    public Prefix(long itemKey, long fullItemSize, Source source, long currentTime) {
      this.itemKey = itemKey;
      this.fullItemSize = fullItemSize;
      this.source = source;
      this.lastRequestTime = currentTime;
      this.currentSize = 0;
      this.score = 0;
    }

    public void updateScore(long currentTime) {
      double decay = Math.pow(0.5, (currentTime - lastRequestTime) / LAMBDA);
      score = score * decay + 1;
    }

    public double lrfuScore() {
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
        currentSize(),
        Consts.BANDWIDTH
      );
      return score * (1 - source.calculateCDF(prefixTransmissionTime));
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

    public double currentSize() {
      return currentSize;
    }

    public double fullItemSize() {
      return fullItemSize;
    }

    public boolean isFull() {
      return currentSize == fullItemSize;
    }

    public boolean isEmpty() {
      return currentSize == 0;
    }

    public int lrfuCompareTo(Prefix other) {
      return Double.compare(this.lrfuScore(), other.lrfuScore());
    }
  }
}
