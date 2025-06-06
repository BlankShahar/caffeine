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

@Policy.PolicySpec(name = "non-binary.generic.LRFU")
public final class GNBLrfuPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
  long currentTime;
  final long maximumCacheSize; // in chunks
  long currentCacheSize; // in chunks
  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Prefix> scoreMinHeap;
  Source source;

  public GNBLrfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.data = new Long2ObjectOpenHashMap<>();
    currentTime = 0;

    this.scoreMinHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::comparePrefixes);
    this.source = new NormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);

    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0;
  }

  @Override
  public void record(AccessEvent event) {
    long itemKey = event.key();
    var existingPrefix = data.getOrDefault(itemKey, null);
    policyStats.recordOperation();
    currentTime++;

    if (existingPrefix != null) {
      existingPrefix.lastRequestTime = currentTime;
      onRequest(existingPrefix, event.retrievalDelay());
    } else {
      long chunksAmount = event.itemSize(); // (long) Math.ceil(event.itemSize() / (Consts.CHUNK_SIZE * 1024 * 1024));
      var newPrefix = new Prefix(itemKey, chunksAmount, source, currentTime);
      onRequest(newPrefix, event.retrievalDelay());
    }
  }

  private void onRequest(Prefix prefix, double sourceDelay) {
    recordRequestStatistics(prefix, sourceDelay);
    prefix.updateScore(currentTime);

    if (!data.containsKey(prefix.itemKey)) {
      data.put(prefix.itemKey, prefix);
    }
    waterFill(prefix);
  }

  private void recordRequestStatistics(Prefix old, double sourceDelay) {
    double idealSize = Math.min(old.fullItemSizeInMB(), sourceDelay * Consts.BANDWIDTH);
    long idealChunksAmount = (long) Math.ceil(idealSize / Consts.CHUNK_SIZE);

    policyStats.addHits(old.chunksAmount);
    policyStats.addMisses(Math.max(0, idealChunksAmount - old.chunksAmount));

    double underflowDelay = calculateUnderflowDelay(sourceDelay, old);
    policyStats.addDelay(underflowDelay);
  }

  private void waterFill(Prefix prefix) {
    while (!prefix.isFull() && currentCacheSize < maximumCacheSize) {
      extendPrefix(prefix);
    }

    Prefix victim;
    do {
      victim = findVictim();
      shrinkPrefix(victim);
      extendPrefix(prefix);
    } while (!(prefix.isFull() || victim.itemKey == prefix.itemKey));
  }

  private void shrinkPrefix(Prefix prefix) {
    if (prefix.isEmpty()) {
      return;
    }
    prefix.removeChunk();
    currentCacheSize--;

    if (prefix.isEmpty())
      scoreMinHeap.remove(prefix.itemKey);
    else
      scoreMinHeap.upsert(prefix.itemKey, prefix);

    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  private void extendPrefix(Prefix prefix) {
    if (prefix.isFull()) {
      return;
    }
    prefix.insertChunk();
    currentCacheSize++;

//    if (scoreMinHeap.contains(prefix.itemKey)) {
//      scoreMinHeap.remove(prefix.itemKey);
//    }
    scoreMinHeap.upsert(prefix.itemKey, prefix);

    policyStats.recordOperation();
    policyStats.recordAdmission();
  }

  private Prefix findVictim() {
    return scoreMinHeap.min().value();
  }

  private static double calculateUnderflowDelay(double sourceDelay, Prefix prefix) {
    return TimeCalculations.calculateUnderflowDelay(
      sourceDelay,
      prefix.fullItemSizeInMB(),
      prefix.sizeInMB(),
      Consts.BANDWIDTH
    );
  }

  public int comparePrefixes(long prefixKey1, long prefixKey2) {
    Prefix p1 = data.get(prefixKey1);
    Prefix p2 = data.get(prefixKey2);
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
    final long itemKey, fullItemChunksAmount;
    final Source source;
    long chunksAmount;
    long lastRequestTime;

    double score;
    static final double LAMBDA = 2.0;

    public Prefix(long itemKey, long fullItemChunksAmount, Source source, long currentTime) {
      this.itemKey = itemKey;
      this.fullItemChunksAmount = fullItemChunksAmount;
      this.source = source;
      this.lastRequestTime = currentTime;
      this.chunksAmount = 0;
      this.score = 0;
    }

    public void updateScore(long currentTime) {
      double decay = Math.pow(0.5, (currentTime - lastRequestTime) / LAMBDA);
      score = score * decay + 1;
    }

    public double lrfuScore() {
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
        sizeInMB(),
        Consts.BANDWIDTH
      );
      return score * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public void insertChunk() {
      if (!isFull()) {
        chunksAmount++;
      }
    }

    public void removeChunk() {
      if (chunksAmount > 0) {
        chunksAmount--;
      }
    }

    public double sizeInMB() {
      return chunksAmount * Consts.CHUNK_SIZE;
    }

    public double fullItemSizeInMB() {
      return fullItemChunksAmount * Consts.CHUNK_SIZE;
    }

    public boolean isFull() {
      return chunksAmount == fullItemChunksAmount;
    }

    public boolean isEmpty() {
      return chunksAmount == 0;
    }

    public int lrfuCompareTo(Prefix other) {
      return Double.compare(this.lrfuScore(), other.lrfuScore());
    }
  }
}
