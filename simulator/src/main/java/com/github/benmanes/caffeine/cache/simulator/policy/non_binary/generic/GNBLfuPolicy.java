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


@Policy.PolicySpec(name = "non-binary.generic.LFU")
public final class GNBLfuPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
  final Queue<Long> requests;
  final long maximumCacheSize; // in chunks
  long currentCacheSize; // in chunks
  final PolicyStats policyStats;
  final Source source;
  final SearchableMinHeap<Long, Prefix> scoreMinHeap;

  public GNBLfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.data = new Long2ObjectOpenHashMap<>();
    this.requests = new ArrayDeque<>();

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

    if (existingPrefix != null) {
      // prefix exist (partial hit)
      onRequest(existingPrefix, event.retrievalDelay());
    } else {
      // prefix missing (full miss)
      var newPrefix = new Prefix(itemKey, event.itemSize(), source);
      onRequest(newPrefix, event.retrievalDelay());
    }
  }

  private void onRequest(Prefix prefix, double sourceDelay) {
    recordRequestStatistics(prefix, sourceDelay);
    handleRequestsFrequency(prefix);

    if (!data.containsKey(prefix.itemKey)) {
      data.put(prefix.itemKey, prefix);
    }
    waterFill(prefix);
  }

  private void handleRequestsFrequency(Prefix prefix) {
    prefix.requestsCountInPeriod++;

    requests.add(prefix.itemKey);
    if (requests.size() == Consts.REQUESTS_FREQUENCY_PERIOD + 1) {
      long lastRequestItemKey = requests.remove();
      var lastRequestedPrefix = data.getOrDefault(lastRequestItemKey, null);
      policyStats.recordOperation();

      if (lastRequestedPrefix != null) {
        lastRequestedPrefix.requestsCountInPeriod--;
      }
    }
  }

  private void recordRequestStatistics(Prefix old, double sourceDelay) {
    // The ideal prefix size - the size that gives "no delay"/"all the item is cached" illusion
    double idealSize = Math.min(old.fullItemSizeInMB(), sourceDelay * Consts.BANDWIDTH);
    long idealChunksAmount = (long) Math.ceil(idealSize / Consts.CHUNK_SIZE);

    // Chunk Hit Rate
    policyStats.addHits(old.chunksAmount);
    policyStats.addMisses(Math.max(0, idealChunksAmount - old.chunksAmount));

    // Total delay
    double underflowDelay = calculateDelay(sourceDelay, old);
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

    scoreMinHeap.remove(prefix.itemKey);
    if (prefix.chunksAmount > 0) {
      scoreMinHeap.insert(prefix.itemKey, prefix);
    }

    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  private void extendPrefix(Prefix prefix) {
    if (prefix.isFull()) {
      return;
    }
    prefix.insertChunk();
    currentCacheSize++;

    if (scoreMinHeap.contains(prefix.itemKey)) {
      scoreMinHeap.remove(prefix.itemKey);
    }
    scoreMinHeap.insert(prefix.itemKey, prefix);

    policyStats.recordOperation();
    policyStats.recordAdmission();
  }

  /**
   * @return the victim chunk to be evicted, or null if no suitable one is found
   */
  private Prefix findVictim() {
    return scoreMinHeap.min().value();
  }

  /**
   * Calculate the delay of fetching a partial cached object
   *
   * @param sourceDelay in seconds
   * @param prefix      the prefix of the item
   * @return the delay in seconds
   */
  private static double calculateDelay(double sourceDelay, Prefix prefix) {
    return TimeCalculations.calculateUnderflowDelay(sourceDelay, prefix.fullItemSizeInMB(), prefix.sizeInMB(), Consts.BANDWIDTH);
  }

  public int comparePrefixes(long prefixKey1, long prefixKey2) {
    Prefix p1 = data.get(prefixKey1);
    Prefix p2 = data.get(prefixKey2);
    return p1.lfuCompareTo(p2);
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
    long requestsCountInPeriod;
    long firstCacheChunksAmount, secondCacheChunksAmount;

    public Prefix(long itemKey, long fullItemChunksAmount, Source source) {
      this.itemKey = itemKey;
      this.fullItemChunksAmount = fullItemChunksAmount;
      this.source = source;
      this.requestsCountInPeriod = 0;
      this.chunksAmount = 0;
      this.firstCacheChunksAmount = 0;
      this.secondCacheChunksAmount = 0;
    }

    public double lfuScore() {
      // Idea - frequency times the probability of not experiencing delay
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(sizeInMB(), Consts.BANDWIDTH);
      return frequency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double frequency() {
      return (double) requestsCountInPeriod / Consts.REQUESTS_FREQUENCY_PERIOD;
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


    public int lfuCompareTo(Prefix other) {
      return Double.compare(this.lfuScore(), other.lfuScore());
    }
  }
}
