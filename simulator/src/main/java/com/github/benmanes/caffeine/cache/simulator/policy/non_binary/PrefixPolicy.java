package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Random;

@Policy.PolicySpec(name = "non-binary.Prefix")
public final class PrefixPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
  final long maximumCacheSize; // in chunks
  long currentCacheSize; // in chunks

  final PolicyStats policyStats;

  public static final long ITEM_CHUNKS_AMOUNT = 1024;
  public static final double CHUNK_SIZE = 0.001; // in MB (1 KB)
  public static final long BANDWIDTH = 1250; // in MBps

  public static final double MEAN_PROCESSING_TIME = 0.2; // average delay in seconds (e.g., 200 ms)
  public static final double STANDARD_DEVIATION_PROCESSING_TIME = 0.05; // standard deviation in seconds (e.g., 50 ms)

  final Random realSampler, approximatedSampler;
  public static final int REAL_SEED = 1337, APPROXIMATED_SEED = 1234;


  public PrefixPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats("non-binary.Prefix");

    this.data = new Long2ObjectOpenHashMap<>();

    // Our cache size unit is in chunks, but the settings are in items/entries amount in cache.
    // So to reflect the settings in chunks, we multiply the settings size by the average chunks amount in item -
    //  which we assume is ~1024 chunks per item.
    // If we assume that a chunk size is 1KB, then an average item size is 1MB.
    this.maximumCacheSize = settings.maximumSize() * ITEM_CHUNKS_AMOUNT;
    this.currentCacheSize = 0;

    this.realSampler = new Random(REAL_SEED);
    this.approximatedSampler = new Random(APPROXIMATED_SEED);
  }

  @Override
  public void record(AccessEvent event) {
    long itemKey = event.key();
    Optional<Prefix> existing = Optional.ofNullable(data.getOrDefault(itemKey, null));
    policyStats.recordOperation();

    Prefix currentPrefix = existing.orElseGet(() -> new Prefix(itemKey, ITEM_CHUNKS_AMOUNT));
    currentPrefix.frequency++; // TODO: add time interval/period/window logic
    recordRequestStatistics(currentPrefix);

    double approximatedSourceDelay = sampleApproximatedSourceProcessingTime();
    double approximatedIdealSize = Math.min(currentPrefix.fullItemSizeInMB(), (calculateDelay(approximatedSourceDelay, currentPrefix, BANDWIDTH) * BANDWIDTH));
    long approximatedIdealChunksAmount = Math.round(approximatedIdealSize / CHUNK_SIZE);
    insertChunks(currentPrefix, approximatedIdealChunksAmount);
    if (currentPrefix.chunksAmount > 0) {
      data.put(itemKey, currentPrefix);
      policyStats.recordOperation();
    }
  }

  private void recordRequestStatistics(Prefix old) {
    double realSourceDelay = getRealNextSourceProcessingTime();
    // The ideal prefix size - the size that gives "no delay"/"all the item is cached" illusion
    double realIdealSize = Math.min(old.fullItemSizeInMB(), (calculateDelay(realSourceDelay, old, BANDWIDTH) * BANDWIDTH));
    long realIdealChunksAmount = Math.round(realIdealSize / CHUNK_SIZE);

    // Chunk Hit Rate
    policyStats.addHits(old.chunksAmount);
    policyStats.addMisses(Math.max(0, realIdealChunksAmount - old.chunksAmount));

    // Total real delay and latency
    double delay = calculateDelay(realSourceDelay, old, BANDWIDTH);
    policyStats.addDelay(Math.max(0, delay));
    double latency = calculateLatency(realSourceDelay, old, BANDWIDTH);
    policyStats.addLatency(latency);
  }

  private double sampleApproximatedSourceProcessingTime() {
    return MEAN_PROCESSING_TIME + STANDARD_DEVIATION_PROCESSING_TIME * approximatedSampler.nextGaussian();
  }

  private double getRealNextSourceProcessingTime() {
    return MEAN_PROCESSING_TIME + STANDARD_DEVIATION_PROCESSING_TIME * realSampler.nextGaussian();
  }

  private void insertChunks(Prefix prefix, long idealChunksAmount) {
    // try to insert more chunks until we reach full size,
    //  or we stop due to not benefiting from it

    while (true) {
      if (prefix.chunksAmount == ITEM_CHUNKS_AMOUNT) {
        // if the item is fully cached, stop inserting more chunks of it
        break;
      }

      // Chunk newChunk = new Chunk(prefix);
      if (currentCacheSize < maximumCacheSize) {
        // insert if there's enough space in the cache
        prefix.insertChunk();
      } else { // cache's full
        // if exists, evict a victim chunk victim from the cache
        Prefix victim = findVictim(prefix);
        if (victim == null) {
          // no suitable victim found and the cache is full - stop inserting
          if (prefix.chunksAmount < idealChunksAmount) {
            policyStats.addRejections(idealChunksAmount - prefix.chunksAmount);
          }
          break;
        }
        removeChunkFromPrefix(victim);
        insertChunkToPrefix(prefix);
      }
    }
  }

  private void removeChunkFromPrefix(Prefix victimPrefix) {
    victimPrefix.removeChunk();
    currentCacheSize--;
    if (victimPrefix.chunksAmount == 0) {
      data.remove(victimPrefix.itemKey);
    }
    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  private void insertChunkToPrefix(Prefix prefix) {
    prefix.insertChunk();
    currentCacheSize++;
    policyStats.recordOperation();
    policyStats.recordAdmission();
  }

  /**
   * @param wantedPrefix the prefix of the new chunk to be inserted
   * @return the victim chunk to be evicted, or null if no suitable one is found
   */
  @Nullable
  private Prefix findVictim(Prefix wantedPrefix) {
    double approximateSourceDelay = sampleApproximatedSourceProcessingTime();
    ArrayList<Prefix> suitableVictims = findSuitableVictims(wantedPrefix, approximateSourceDelay);
    return getLowestEvictionCostChunk(suitableVictims, approximateSourceDelay);
  }

  @Nullable
  private Prefix getLowestEvictionCostChunk(ArrayList<Prefix> possibleVictims, double sourceDelay) {
    if (possibleVictims.isEmpty()) {
      return null;
    }

    policyStats.recordOperation();
    Prefix victim = possibleVictims.get(0);
    for (Prefix candidate : possibleVictims) {
      if (evictionCost(candidate, sourceDelay) < evictionCost(victim, sourceDelay)) {
        victim = candidate;
      }
    }
    return victim;
  }

  /**
   * @param wantedPrefix           the prefix of the new chunk to be inserted
   * @param approximateSourceDelay the approximated source processing time
   * @return list of possible victims that can be evicted -
   * those that have a lower eviction benefit than the new chunk insertion benefit
   */
  private ArrayList<Prefix> findSuitableVictims(
    Prefix wantedPrefix,
    double approximateSourceDelay
  ) {
    policyStats.recordOperation();
    ArrayList<Prefix> possibleVictims = new ArrayList<>();
    for (Prefix candidate : data.values()) {
      if (insertionBenefit(wantedPrefix, approximateSourceDelay) >= evictionCost(candidate, approximateSourceDelay)) {
        possibleVictims.add(candidate);
      }
    }
    return possibleVictims;
  }

  /**
   * Calculate the full latency of fetching a partial cached object
   *
   * @param sourceDelay in seconds
   * @param prefix      the prefix of the item
   * @param bandwidth   in MBps
   * @return the delay in seconds
   */
  private static double calculateDelay(double sourceDelay, Prefix prefix, long bandwidth) {
    if (prefix.isFull()) {
      // If the whole item is cached, there's no delay whatsoever.
      // Even if the source delay is very large, but the whole item is cached -
      //  there will not be a request to source, therefore no delay.
      return 0;
    }
    return sourceDelay - prefix.sizeInMB() / bandwidth;
  }

  /**
   * Calculate the full latency of fetching a partial cached object
   *
   * @param sourceDelay  in seconds
   * @param fullItemSize in MB
   * @param prefixSize   in MB
   * @param bandwidth    in MBps
   * @return the delay in seconds
   */
  private static double calculateDelay(double sourceDelay, double fullItemSize, double prefixSize, long bandwidth) {
    if (fullItemSize == prefixSize) {
      // If the whole item is cached, there's no delay whatsoever.
      // Even if the source delay is very big, if the whole item is cached, there will not be a request to source, therefore no delay.
      return 0;
    }
    return sourceDelay - prefixSize / bandwidth;
  }

  /**
   * Calculate the full latency of fetching a partial cached object
   *
   * @param sourceDelay in s
   * @param prefix      the prefix of the item
   * @param bandwidth   in MBps
   * @return the latency in seconds
   */
  private double calculateLatency(double sourceDelay, Prefix prefix, long bandwidth) {
    double prefixLatency = prefix.sizeInMB() / bandwidth; // cache->client
    double delay = calculateDelay(sourceDelay, prefix, bandwidth);
    double restLatency = (double) 2 * (prefix.fullItemSizeInMB() - prefix.sizeInMB()) / bandwidth; // source->cache->client
    if (delay < 0) { // overflow case
      return prefixLatency + restLatency;
    }
    return prefixLatency + delay + restLatency;
  }

  private double insertionBenefit(Prefix prefix, double approximatedSourceDelay) {
    // calculate the benefit of inserting a new chunk to its prefix
    // D_i[r] = T[s] - (|P_i[r]| + 1) / B
    // Benefit = F_i * (D_i[r+1] - D_i[r])
    // double newDelay = calculateDelay(approximatedSourceDelay, chunk.fatherPrefix.size() + 1, BANDWIDTH);
    // return 1 / Math.pow(newDelay, 2) * chunk.fatherPrefix.frequency;

    double currentDelay = calculateDelay(approximatedSourceDelay, prefix, BANDWIDTH);
    double approximatedNextSampleSourceDelay = sampleApproximatedSourceProcessingTime();
    double newDelay = calculateDelay(approximatedNextSampleSourceDelay, prefix.fullItemSizeInMB(), prefix.sizeInMB() + CHUNK_SIZE, BANDWIDTH);
    double deltaDelay = newDelay - currentDelay;
    return prefix.frequency * deltaDelay;
  }

  private double evictionCost(Prefix prefix, double approximatedSourceDelay) {
    // calculate the cost of inserting a new chunk to its prefix
    // D_i[r] = T[s] - (|P_i[r]| + 1) / B
    // Cost = F_i * (D_i[r] - D_i[r+1])
    // double newDelay = calculateDelay(approximatedSourceDelay, chunk.fatherPrefix.size() - 1, BANDWIDTH);
    // return 1 / Math.pow(newDelay, 2) * chunk.fatherPrefix.frequency;

    double currentDelay = calculateDelay(approximatedSourceDelay, prefix, BANDWIDTH);
    double approximatedNextSampleSourceDelay = sampleApproximatedSourceProcessingTime();
    double newDelay = calculateDelay(approximatedNextSampleSourceDelay, prefix.fullItemSizeInMB(), prefix.sizeInMB() - CHUNK_SIZE, BANDWIDTH);
    double deltaDelay = currentDelay - newDelay;
    return prefix.frequency * deltaDelay;
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

  static class Chunk {
    final Prefix fatherPrefix;

    public Chunk(Prefix fatherPrefix) {
      this.fatherPrefix = fatherPrefix;
    }
  }

  static class Prefix {
    final long itemKey, fullItemChunksAmount;
    long chunksAmount;
    long frequency;

    public Prefix(long itemKey, long fullItemChunksAmount) {
      this.itemKey = itemKey;
      this.fullItemChunksAmount = fullItemChunksAmount;
      this.frequency = 0;
      this.chunksAmount = 0;
    }

    public void insertChunk() {
      chunksAmount++;
    }

    public void removeChunk() {
      if (chunksAmount > 0) {
        chunksAmount--;
      }
    }

    public double sizeInMB() {
      return chunksAmount * CHUNK_SIZE;
    }

    public double fullItemSizeInMB() {
      return fullItemChunksAmount * CHUNK_SIZE;
    }

    public boolean isFull() {
      return chunksAmount == fullItemChunksAmount;
    }
  }
}
