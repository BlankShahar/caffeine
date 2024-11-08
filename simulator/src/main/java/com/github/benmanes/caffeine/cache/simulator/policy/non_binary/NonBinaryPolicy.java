package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Random;
import java.util.Stack;

@Policy.PolicySpec(name = "non-binary.NonBinary")
public final class NonBinaryPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
  final long maximumSize; // in chunks
  long currentSize;

  final PolicyStats policyStats;

  static final long AVG_ITEM_SIZE = 1024; // in chunks
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
    this.maximumSize = settings.maximumSize() * AVG_ITEM_SIZE;
    this.currentSize = 0;

    this.random = new Random(1337);
  }

  @Override
  public void record(AccessEvent event) {
    long itemKey = event.key();
    Optional<Prefix> existing = Optional.ofNullable(data.getOrDefault(itemKey, null));
    policyStats.recordOperation();

    Prefix currentPrefix = existing.orElseGet(() -> new Prefix(itemKey));
    currentPrefix.frequency++; // TODO: add time interval/period/window logic

    double sourceDelay = sampleSourceProcessingTime();
    // The ideal prefix size - the number of chunks that give the "no delay" illusion
    long idealSize = (long) (calculateDelay(sourceDelay, currentPrefix.size(), BANDWIDTH) * BANDWIDTH);
    recordPartialHitStats(currentPrefix, idealSize);

    insertChunks(currentPrefix, idealSize);
    if (currentPrefix.size() > 0) {
      data.put(itemKey, currentPrefix);
      policyStats.recordOperation();
    }
  }

  private void recordPartialHitStats(Prefix old, long idealSize) {
    policyStats.addHits(old.size());
    if (old.size() < idealSize) {
      // underflow case
      policyStats.addMisses(idealSize - old.size());
    }
  }

  private double sampleSourceProcessingTime() {
    return MEAN + STANDARD_DEVIATION * random.nextGaussian();
  }

  private void insertChunks(Prefix prefix, long idealSize) {
    // try to insert more chunks until we reach ideal fatherPrefix size,
    //  or we stop due to not benefiting from it

    while (true) {
      Chunk newChunk = new Chunk(prefix);
      if (currentSize < maximumSize) {
        // insert if there's enough space in the cache
        insertChunkToPrefix(prefix, newChunk);
      } else {
        // if exists, evict a chunk (victim) if there's no space
        Chunk victim = findVictim(newChunk);
        if (victim == null) {
          // no suitable victim found and the cache is full - stop inserting
          for (int i = 0; prefix.size() < idealSize && i < idealSize - prefix.size(); i++) {
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
    currentSize--;
    if (victimPrefix.size() == 0) {
      data.remove(victimPrefix.itemKey);
    }
    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  private void insertChunkToPrefix(Prefix prefix, Chunk newChunk) {
    prefix.insertChunk(newChunk);
    currentSize++;
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
    return getLowestEvictionBenefitChunk(suitableVictims, sourceDelay);
  }

  @Nullable
  private Chunk getLowestEvictionBenefitChunk(ArrayList<Chunk> possibleVictims, double sourceDelay) {
    if (possibleVictims.isEmpty()) {
      return null;
    }

    policyStats.recordOperation();
    Chunk victim = possibleVictims.get(0);
    for (Chunk candidate : possibleVictims) {
      if (evictionBenefit(candidate, sourceDelay) < evictionBenefit(victim, sourceDelay)) {
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
      if (insertionBenefit(newChunk, sourceDelay) >= evictionBenefit(candidate, sourceDelay)) {
        possibleVictims.add(candidate);
      }
    }
    return possibleVictims;
  }

  private static double calculateDelay(double sourceDelay, long prefixSize, long bandwidth) {
    if (prefixSize < 0) {
      throw new InvalidParameterException("Prefix size must be non-negative!");
    }
    return sourceDelay - (double) prefixSize / bandwidth;
  }

  private double insertionBenefit(Chunk chunk, double sourceDelay) {
    // calculate the benefit of inserting a new chunk to its prefix
    // D_i[r] = T[s] - (|P_i[r]| + 1) / B
    double newDelay = calculateDelay(sourceDelay, chunk.fatherPrefix.size() + 1, BANDWIDTH);
    return 1 / Math.pow(newDelay, 2) * chunk.fatherPrefix.frequency;
  }

  private double evictionBenefit(Chunk chunk, double sourceDelay) {
    // calculate the benefit of evicting the last chunk from its prefix
    // D_i[r] = T[s] - (|P_i[r]| - 1) / B
    double newDelay = calculateDelay(sourceDelay, chunk.fatherPrefix.size() - 1, BANDWIDTH);
    return 1 / Math.pow(newDelay, 2) * chunk.fatherPrefix.frequency;
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
    final long itemKey;
    long frequency;
    Stack<Chunk> chunks;

    public Prefix(long itemKey) {
      this.itemKey = itemKey;
      this.frequency = 0;
      this.chunks = new Stack<>();
    }

    public void insertChunk(Chunk chunk) {
      chunks.push(chunk);
    }

    public void removeChunk() {
      chunks.pop();
    }

    public long size() {
      return chunks.size();
    }
  }
}
