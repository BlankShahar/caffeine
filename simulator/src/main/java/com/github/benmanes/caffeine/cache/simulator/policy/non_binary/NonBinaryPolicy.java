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
import java.util.Stack;

@Policy.PolicySpec(name = "non-binary.NonBinary")
public final class NonBinaryPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
  final long maximumCacheSize; // in chunks
  long currentCacheSize;

  final PolicyStats policyStats;

  static final long ITEM_CHUNKS_AMOUNT = 1024;
  static final double CHUNK_SIZE = 0.001; // in MB (1 KB)
  static final long BANDWIDTH = 1250; // in MBps

  static final double MEAN = 0.2; // average delay in seconds (e.g., 200 ms)
  static final double STANDARD_DEVIATION = 0.05; // standard deviation in seconds (e.g., 50 ms)

  final Random random;

  public NonBinaryPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats("Non-Binary");

    this.data = new Long2ObjectOpenHashMap<>();

    // Our cache size unit is in chunks, but the settings are in items/entries amount in cache.
    // So to reflect the settings in chunks, we multiply the settings size by the average chunks amount in item -
    //  which we assume is ~1024 chunks per item.
    // If we assume that a chunk size is 1KB, then an average item size is 1MB.
    this.maximumCacheSize = settings.maximumSize() * ITEM_CHUNKS_AMOUNT;
    this.currentCacheSize = 0;

    this.random = new Random(1337);
  }

  @Override
  public void record(AccessEvent event) {
    long itemKey = event.key();
    Optional<Prefix> existing = Optional.ofNullable(data.getOrDefault(itemKey, null));
    policyStats.recordOperation();

    Prefix currentPrefix = existing.orElseGet(() -> new Prefix(itemKey, ITEM_CHUNKS_AMOUNT));
    currentPrefix.frequency++; // TODO: add time interval/period/window logic
    recordRequestStatistics(currentPrefix);

    double approximatedSourceDelay = sampleSourceProcessingTime();
    double approximatedIdealSize = (long) (calculateDelay(approximatedSourceDelay, currentPrefix, BANDWIDTH) * BANDWIDTH);
    long approximatedIdealChunksAmount = Math.min(ITEM_CHUNKS_AMOUNT, (long) (approximatedIdealSize / CHUNK_SIZE));
    insertChunks(currentPrefix, approximatedIdealChunksAmount);
    if (currentPrefix.chunksAmount() > 0) {
      data.put(itemKey, currentPrefix);
      policyStats.recordOperation();
    }
  }

  private void recordRequestStatistics(Prefix old) {
    double realSourceDelay = sampleSourceProcessingTime();
    // The ideal prefix size - the number of chunks that give the "no delay" illusion
    long idealSize = (long) (calculateDelay(realSourceDelay, old, BANDWIDTH) * BANDWIDTH);

    // Chunk Hit Rate
    policyStats.addHits(old.chunksAmount());
    if (old.chunksAmount() < idealSize) { // underflow case
      policyStats.addMisses(idealSize - old.chunksAmount());
    }

    // Total real delay and latency
    double delay = calculateDelay(realSourceDelay, old, BANDWIDTH);
    if (delay > 0) {// underflow case
      policyStats.addDelay(delay);
    }
    policyStats.addLatency(
      calculateLatency(realSourceDelay, old, BANDWIDTH)
    );
  }

  private double sampleSourceProcessingTime() {
    return MEAN + STANDARD_DEVIATION * random.nextGaussian();
  }

  private void insertChunks(Prefix prefix, long idealChunksAmount) {
    // try to insert more chunks until we reach ideal fatherPrefix size,
    //  or we stop due to not benefiting from it

    while (true) {
      if (prefix.chunksAmount() == ITEM_CHUNKS_AMOUNT) {
        // if the prefix is already fully cached, stop inserting more chunks
        break;
      }

      Chunk newChunk = new Chunk(prefix);
      if (currentCacheSize < maximumCacheSize) {
        // insert if there's enough space in the cache
        insertChunkToPrefix(prefix, newChunk);
      } else {
        // if exists, evict a chunk (victim) if there's no space
        Chunk victim = findVictim(newChunk);
        if (victim == null) {
          // no suitable victim found and the cache is full - stop inserting
          for (int i = 0; prefix.chunksAmount() < idealChunksAmount && i < idealChunksAmount - prefix.chunksAmount(); i++) {
            // record rejection for each chunk that could not be inserted
            policyStats.recordRejection();
          }
          break;
        }
        removeChunkFromPrefix(victim.fatherPrefix);
        insertChunkToPrefix(prefix, newChunk);
      }
    }
  }

  private void removeChunkFromPrefix(Prefix victimPrefix) {
    victimPrefix.removeChunk();
    currentCacheSize--;
    if (victimPrefix.chunksAmount() == 0) {
      data.remove(victimPrefix.itemKey);
    }
    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  private void insertChunkToPrefix(Prefix prefix, Chunk newChunk) {
    prefix.insertChunk(newChunk);
    currentCacheSize++;
    policyStats.recordOperation();
    policyStats.recordAdmission();
  }

  /**
   * @param newChunk the new chunk to be inserted
   * @return the victim chunk to be evicted, or null if no suitable one is found
   */
  @Nullable
  private Chunk findVictim(Chunk newChunk) {
    ArrayList<Chunk> victimCandidates = getAllEndChunks();
    double sourceDelay = sampleSourceProcessingTime();
    ArrayList<Chunk> suitableVictims = findSuitableVictims(newChunk, victimCandidates, sourceDelay);
    return getLowestEvictionCostChunk(suitableVictims, sourceDelay);
  }

  @Nullable
  private Chunk getLowestEvictionCostChunk(ArrayList<Chunk> possibleVictims, double sourceDelay) {
    if (possibleVictims.isEmpty()) {
      return null;
    }

    policyStats.recordOperation();
    Chunk victim = possibleVictims.get(0);
    for (Chunk candidate : possibleVictims) {
      if (evictionCost(candidate, sourceDelay) < evictionCost(victim, sourceDelay)) {
        victim = candidate;
      }
    }
    return victim;
  }

  /**
   * @param newChunk         the new chunk to be inserted
   * @param victimCandidates the list of possible victims - all end chunks
   * @param sourceDelay      the source processing time
   * @return list of possible victims that can be evicted -
   * those that have a lower eviction benefit than the new chunk insertion benefit
   */
  private ArrayList<Chunk> findSuitableVictims(
    Chunk newChunk,
    ArrayList<Chunk> victimCandidates,
    double sourceDelay
  ) {
    policyStats.recordOperation();
    ArrayList<Chunk> possibleVictims = new ArrayList<>();
    for (Chunk candidate : victimCandidates) {
      if (insertionBenefit(newChunk, sourceDelay) >= evictionCost(candidate, sourceDelay)) {
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

  private double insertionBenefit(Chunk chunk, double approximatedSourceDelay) {
    // calculate the benefit of inserting a new chunk to its prefix
    // D_i[r] = T[s] - (|P_i[r]| + 1) / B
    // Benefit = F_i * (D_i[r+1] - D_i[r])
    // double newDelay = calculateDelay(approximatedSourceDelay, chunk.fatherPrefix.size() + 1, BANDWIDTH);
    // return 1 / Math.pow(newDelay, 2) * chunk.fatherPrefix.frequency;

    double currentDelay = calculateDelay(approximatedSourceDelay, chunk.fatherPrefix, BANDWIDTH);
    double approximatedNextSampleSourceDelay = sampleSourceProcessingTime();
    double newDelay = calculateDelay(approximatedNextSampleSourceDelay, chunk.fatherPrefix.fullItemSizeInMB(), chunk.fatherPrefix.sizeInMB() + CHUNK_SIZE, BANDWIDTH);
    double deltaDelay = newDelay - currentDelay;
    return chunk.fatherPrefix.frequency * deltaDelay;
  }

  private double evictionCost(Chunk chunk, double approximatedSourceDelay) {
    // calculate the cost of inserting a new chunk to its prefix
    // D_i[r] = T[s] - (|P_i[r]| + 1) / B
    // Cost = F_i * (D_i[r] - D_i[r+1])
    // double newDelay = calculateDelay(approximatedSourceDelay, chunk.fatherPrefix.size() - 1, BANDWIDTH);
    // return 1 / Math.pow(newDelay, 2) * chunk.fatherPrefix.frequency;

    double currentDelay = calculateDelay(approximatedSourceDelay, chunk.fatherPrefix, BANDWIDTH);
    double approximatedNextSampleSourceDelay = sampleSourceProcessingTime();
    double newDelay = calculateDelay(approximatedNextSampleSourceDelay, chunk.fatherPrefix.fullItemSizeInMB(), chunk.fatherPrefix.sizeInMB() - CHUNK_SIZE, BANDWIDTH);
    double deltaDelay = currentDelay - newDelay;
    return chunk.fatherPrefix.frequency * deltaDelay;
  }

  private ArrayList<Chunk> getAllEndChunks() {
    policyStats.recordOperation();
    ArrayList<Chunk> endChunks = new ArrayList<>();
    for (Prefix prefix : data.values()) {
      endChunks.add(prefix.chunks.peek());
    }
    return endChunks;
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
    long frequency;
    Stack<Chunk> chunks;

    public Prefix(long itemKey, long fullItemChunksAmount) {
      this.itemKey = itemKey;
      this.fullItemChunksAmount = fullItemChunksAmount;
      this.frequency = 0;
      this.chunks = new Stack<>();
    }

    public void insertChunk(Chunk chunk) {
      chunks.push(chunk);
    }

    public void removeChunk() {
      chunks.pop();
    }

    public long chunksAmount() {
      return chunks.size();
    }

    public double sizeInMB() {
      return chunks.size() * CHUNK_SIZE;
    }

    public double fullItemSizeInMB() {
      return fullItemChunksAmount * CHUNK_SIZE;
    }

    public boolean isFull() {
      return chunksAmount() == fullItemChunksAmount;
    }
  }
}
