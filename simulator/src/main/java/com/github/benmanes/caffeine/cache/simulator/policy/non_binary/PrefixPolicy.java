package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;


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
    var existingPrefix = data.getOrDefault(itemKey, null);
    policyStats.recordOperation();

    if (existingPrefix != null) {
      // prefix exist (partial hit)
      onRequest(existingPrefix);
    } else {
      // prefix missing (full miss)
      int sourceKey = sourcePicker.nextInt(Consts.SOURCES.size());
      Source source = Consts.SOURCES.get(sourceKey);
      var newPrefix = new Prefix(itemKey, Consts.ITEM_CHUNKS_AMOUNT, source);
      onRequest(newPrefix);
    }
  }

  private void onRequest(Prefix prefix) {
    recordRequestStatistics(prefix);
    handleRequestsFrequency(prefix);
    insertChunks(prefix);

    if (!data.containsKey(prefix.itemKey)) {
      data.put(prefix.itemKey, prefix);
    }
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

  private void recordRequestStatistics(Prefix old) {
    double sourceDelay = old.source.sampleProcessingTime();
    // The ideal prefix size - the size that gives "no delay"/"all the item is cached" illusion
    double realIdealSize = Math.min(old.fullItemSizeInMB(), sourceDelay * Consts.BANDWIDTH);
    long realIdealChunksAmount = (long) Math.ceil(realIdealSize / Consts.CHUNK_SIZE);

    // Chunk Hit Rate
    policyStats.addHits(old.chunksAmount);
    policyStats.addMisses(Math.max(0, realIdealChunksAmount - old.chunksAmount));

    // Total delay and latency
    double delay = calculateDelay(sourceDelay, old);
    policyStats.addDelay(delay);
    double latency = calculateLatency(sourceDelay, old);
    policyStats.addLatency(latency);
  }

  private void insertChunks(Prefix prefix) {
    // try to insert more chunks until we reach full size,
    //  or we stop due to not benefiting from it

    // if the item is fully cached, stop inserting more chunks of it
    while (!prefix.isFull()) {
      insertChunkToPrefix(prefix);

      if (currentCacheSize == maximumCacheSize + 1) { // cache's full+1 and someone needs to be evicted
        // if exists, evict a victim (last) chunk from a victim prefix from the cache
        Prefix victim = findVictim();
        removeChunkFromPrefix(victim);

        if (victim.itemKey == prefix.itemKey) {
          // The victim became the prefix itself, so we stop benefiting from inserting more chunks to it
          // So we stop here
          break;
        }
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
   * @return the victim chunk to be evicted, or null if no suitable one is found
   */
  private Prefix findVictim() {
    return getLowestScorePrefix();
  }

  private Prefix getLowestScorePrefix() {
    policyStats.recordOperation();
    Prefix victim = null;
    double minCost = Double.MAX_VALUE;

    for (Prefix candidate : data.values()) {
      if (candidate.chunksAmount == 0) {
        continue;
      }

      double score = candidate.insertionScore();
      if (score < minCost) {
        minCost = score;
        victim = candidate;
      }
    }
    assert victim != null;
    return victim;
  }

  /**
   * Calculate the full latency of fetching a partial cached object
   *
   * @param sourceDelay in seconds
   * @param prefix      the prefix of the item
   * @return the delay in seconds
   */
  private static double calculateDelay(double sourceDelay, Prefix prefix) {
    return TimeCalculations.calculateDelay(
      sourceDelay,
      prefix.fullItemSizeInMB(),
      prefix.sizeInMB(),
      Consts.BANDWIDTH
    );
  }

  /**
   * Calculate the full latency of fetching a partial cached object
   *
   * @param sourceDelay in s
   * @param prefix      the prefix of the item
   * @return the latency in seconds
   */
  private static double calculateLatency(double sourceDelay, Prefix prefix) {
    return TimeCalculations.calculateNonBinaryLatency(
      sourceDelay,
      prefix.fullItemSizeInMB(),
      prefix.sizeInMB(),
      Consts.BANDWIDTH
    );
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
