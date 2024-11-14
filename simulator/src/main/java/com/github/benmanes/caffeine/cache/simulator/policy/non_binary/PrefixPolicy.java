package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.HashMap;


@Policy.PolicySpec(name = "non-binary.Prefix")
public final class PrefixPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
  final Queue<Long> requests;
  final long maximumCacheSize; // in chunks
  long currentCacheSize; // in chunks
  final PolicyStats policyStats;
  final Random sourcePicker;

  public PrefixPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.data = new Long2ObjectOpenHashMap<>();
    this.requests = new ArrayDeque<>();

    // Our cache size unit is in chunks, but the settings are in items/entries amount in cache.
    // So to reflect the settings in chunks, we multiply the settings size by the average chunks amount in item -
    //  which we assume is ~1024 chunks per item.
    // If we assume that a chunk size is 1KB, then an average item size is 1MB.
    this.maximumCacheSize = settings.maximumSize() * Consts.ITEM_CHUNKS_AMOUNT;
    this.currentCacheSize = 0;

    this.sourcePicker = new Random(Consts.SOURCE_PICKER_SEED);
  }

  @Override
  public void record(AccessEvent event) {
    long itemKey = event.key();
    var existing = data.getOrDefault(itemKey, null);
    policyStats.recordOperation();

    if (existing != null) {
      // prefix exist (partial hit)
      onRequest(existing);
    } else {
      // prefix missing (full miss)
      Source realSource = Consts.REAL_SOURCES.get(sourcePicker.nextInt(Consts.REAL_SOURCES.size()));
      Source approximatedSource = Consts.APPROXIMATED_SOURCES.get(sourcePicker.nextInt(Consts.APPROXIMATED_SOURCES.size()));
      var newPrefix = new Prefix(itemKey, Consts.ITEM_CHUNKS_AMOUNT, realSource, approximatedSource);
      onRequest(newPrefix);
    }
  }

  private void onRequest(Prefix prefix) {
    recordRequestStatistics(prefix);
    handleRequestsFrequency(prefix);
    insertChunks(prefix);

    if (!data.containsKey(prefix.itemKey)) {
      data.put(prefix.itemKey, prefix);
      policyStats.recordOperation();
    }
  }

  private void handleRequestsFrequency(Prefix prefix) {
    prefix.requestsCountInPeriod++;

    requests.add(prefix.itemKey);
    if (requests.size() == Consts.REQUESTS_FREQUENCY_PERIOD + 1) {
      long lastRequestItemKey = requests.remove();
      var lastRequestedPrefix = data.getOrDefault(lastRequestItemKey, null);
      if (lastRequestedPrefix != null) {
        lastRequestedPrefix.requestsCountInPeriod--;
      }
    }
  }

  private void recordRequestStatistics(Prefix old) {
    double realSourceDelay = old.realSource.getNextProcessingTime();
    // The ideal prefix size - the size that gives "no delay"/"all the item is cached" illusion
    double realIdealSize = Math.min(old.fullItemSizeInMB(), (calculateDelay(realSourceDelay, old, Consts.BANDWIDTH) * Consts.BANDWIDTH));
    long realIdealChunksAmount = Math.round(realIdealSize / Consts.CHUNK_SIZE);

    // Chunk Hit Rate
    policyStats.addHits(old.chunksAmount);
    policyStats.addMisses(Math.max(0, realIdealChunksAmount - old.chunksAmount));

    // Total (real) delay and latency
    double delay = calculateDelay(realSourceDelay, old, Consts.BANDWIDTH);
    policyStats.addDelay(Math.max(0, delay));
    double latency = calculateLatency(realSourceDelay, old, Consts.BANDWIDTH);
    policyStats.addLatency(latency);
  }

  private void insertChunks(Prefix prefix) {
    // try to insert more chunks until we reach full size,
    //  or we stop due to not benefiting from it

    while (true) {
      if (prefix.isFull()) {
        // if the item is fully cached, stop inserting more chunks of it
        break;
      }

      if (currentCacheSize < maximumCacheSize) {
        // insert if there's enough space in the cache
        insertChunkToPrefix(prefix);
      } else { // cache's full
        // if exists, evict a victim (last) chunk from a victim prefix from the cache
        Prefix victim = findVictim(prefix);
        if (victim == null) {
          // no suitable victim found and the cache is full - stop inserting (reject all the rest)
          policyStats.addRejections(Math.max(0, prefix.fullItemChunksAmount - prefix.chunksAmount));
          break;
        }
        removeChunkFromPrefix(victim);
        insertChunkToPrefix(prefix);
      }
    }
  }

  private void removeChunkFromPrefix(Prefix prefix) {
    prefix.removeChunk();
    currentCacheSize--;
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
   * @param competitor the prefix of the new chunk to be inserted
   * @return the victim chunk to be evicted, or null if no suitable one is found
   */
  @Nullable
  private Prefix findVictim(Prefix competitor) {
    HashMap<Source, Double> currentApproximatedProcessingTimes = TimeCalculations.getNextProcessingTimes(Consts.APPROXIMATED_SOURCES);
    HashMap<Source, Double> nextApproximatedProcessingTimes = TimeCalculations.getNextProcessingTimes(Consts.APPROXIMATED_SOURCES);
    List<Prefix> suitableVictims = findSuitableVictims(competitor, currentApproximatedProcessingTimes, nextApproximatedProcessingTimes);
    return getLowestEvictionCostChunk(suitableVictims, currentApproximatedProcessingTimes, nextApproximatedProcessingTimes);
  }

  @Nullable
  private Prefix getLowestEvictionCostChunk(
    List<Prefix> possibleVictims,
    HashMap<Source, Double> currentApproximatedProcessingTimes,
    HashMap<Source, Double> nextApproximatedProcessingTimes
  ) {
    policyStats.recordOperation();
    // TODO: implement as min heap instead to improve runtime
    Prefix victim = null;
    double minCost = Double.MAX_VALUE;

    for (Prefix candidate : possibleVictims) {
      double candidateCost = evictionCost(
        candidate,
        currentApproximatedProcessingTimes.get(candidate.approximatedSource),
        nextApproximatedProcessingTimes.get(candidate.approximatedSource)
      );
      if (candidateCost < minCost) {
        minCost = candidateCost;
        victim = candidate;
      }
    }
    return victim;
  }


  /**
   * @param competitor the prefix of the new chunk to be inserted
   * @return list of possible victims that can be evicted -
   * those that have a lower eviction benefit than the new chunk insertion benefit
   */
  private List<Prefix> findSuitableVictims(
    Prefix competitor,
    HashMap<Source, Double> currentApproximatedProcessingTimes,
    HashMap<Source, Double> nextApproximatedProcessingTimes
  ) {
    policyStats.recordOperation();

    double competitorInsertionBenefit = insertionBenefit(
      competitor,
      currentApproximatedProcessingTimes.get(competitor.approximatedSource),
      nextApproximatedProcessingTimes.get(competitor.approximatedSource)
    );

    ArrayList<Prefix> possibleVictims = new ArrayList<>();
    for (Prefix candidate : data.values()) {
      double candidateEvictionCost = evictionCost(
        candidate,
        currentApproximatedProcessingTimes.get(candidate.approximatedSource),
        nextApproximatedProcessingTimes.get(candidate.approximatedSource)
      );
      if (
        candidate.chunksAmount > 0 &&
          candidate.itemKey != competitor.itemKey &&
          competitorInsertionBenefit >= candidateEvictionCost
      ) {
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
    return TimeCalculations.calculateDelay(sourceDelay, prefix.fullItemSizeInMB(), prefix.sizeInMB(), bandwidth);
  }

  /**
   * Calculate the full latency of fetching a partial cached object
   *
   * @param sourceDelay in s
   * @param prefix      the prefix of the item
   * @param bandwidth   in MBps
   * @return the latency in seconds
   */
  private static double calculateLatency(double sourceDelay, Prefix prefix, long bandwidth) {
    return TimeCalculations.calculateNonBinaryLatency(sourceDelay, prefix.fullItemSizeInMB(), prefix.sizeInMB(), bandwidth);
  }

  private static double insertionBenefit(
    Prefix prefix,
    double currentApproximatedSourceDelay,
    double nextApproximatedSourceDelay
  ) {
    // calculate the benefit of inserting a new chunk to its prefix
    // D_i[r] = T[s] - (|P_i[r]| + 1) / B
    // Benefit = F_i * (D_i[r+1] - D_i[r])

    double currentDelay = calculateDelay(
      currentApproximatedSourceDelay,
      prefix,
      Consts.BANDWIDTH
    );
    double newDelay = TimeCalculations.calculateDelay(
      nextApproximatedSourceDelay,
      prefix.fullItemSizeInMB(),
      prefix.sizeInMB() + Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    double deltaDelay = newDelay - currentDelay;
    return prefix.frequency() * deltaDelay;
  }

  private static double evictionCost(
    Prefix prefix,
    double currentApproximatedSourceDelay,
    double nextApproximatedSourceDelay
  ) {
    // calculate the cost of inserting a new chunk to its prefix
    // D_i[r] = T[s] - (|P_i[r]| + 1) / B
    // Cost = F_i * (D_i[r] - D_i[r+1])

    double currentDelay = calculateDelay(
      currentApproximatedSourceDelay,
      prefix,
      Consts.BANDWIDTH
    );
    double newDelay = TimeCalculations.calculateDelay(
      nextApproximatedSourceDelay, //prefix.approximatedSource.getNextProcessingTime(),
      prefix.fullItemSizeInMB(),
      prefix.sizeInMB() - Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    double deltaDelay = currentDelay - newDelay;
    return prefix.frequency() * deltaDelay;
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
}
