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
  final SearchableMinHeap<Long, Prefix> scoreMinHeap;

  public PrefixPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.data = new Long2ObjectOpenHashMap<>();
    this.requests = new ArrayDeque<>();

    this.scoreMinHeap = new SearchableMinHeap<>((int) Consts.REQUESTS_FREQUENCY_PERIOD, this::compare);

    // Our cache size unit is in chunks, but the settings are in items/entries amount in cache.
    // So to reflect the settings in chunks, we multiply the settings size by the average chunks amount in item -
    //  which we assume is ~1024 chunks per item.
    // If we assume that a chunk size is 4KB, then an average item size is 4MB.
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

    if (!data.containsKey(prefix.itemKey)) {
      data.put(prefix.itemKey, prefix);
    }
    insertChunks(prefix);
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
    double idealSize = Math.min(old.fullItemSizeInMB(), sourceDelay * Consts.BANDWIDTH);
    long idealChunksAmount = (long) Math.ceil(idealSize / Consts.CHUNK_SIZE);

    // Chunk Hit Rate
    policyStats.addHits(old.chunksAmount);
    policyStats.addMisses(Math.max(0, idealChunksAmount - old.chunksAmount));

    // Total delay and latency
    double delay = calculateDelay(sourceDelay, old);
    policyStats.addDelay(delay);
    double latency = calculateLatency(sourceDelay, old);
    policyStats.addLatency(latency);
  }

  private void insertChunks(Prefix prefix) {
    while (!prefix.isFull() && currentCacheSize < maximumCacheSize) {
      insertChunkToPrefix(prefix);
    }

    while (true) {
      Prefix victim = findVictim();
      double sPlus = prefix.lfu_score_after_insertion();
      double sMinus = victim.lfu_score_after_eviction();

        if (victim.itemKey == prefix.itemKey) {
          // The victim became the prefix itself, so we stop benefiting from inserting more chunks to it
          // So we stop here
          break;
        }
      }
      if (prefix.isFull() || victim.itemKey == prefix.itemKey || sPlus < sMinus) {
        break;
      }

      removeChunkFromPrefix(victim);
      insertChunkToPrefix(prefix);
    }
  }

  private void removeChunkFromPrefix(Prefix prefix) {
    prefix.removeChunk();
    currentCacheSize--;

    scoreMinHeap.remove(prefix.itemKey);
    if (prefix.chunksAmount > 0) {
      scoreMinHeap.insert(prefix.itemKey, prefix);
    }

    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  private void insertChunkToPrefix(Prefix prefix) {
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

  public int compare(long prefixKey1, long prefixKey2) {
    Prefix p1 = data.get(prefixKey1);
    Prefix p2 = data.get(prefixKey2);
    return p1.compareTo(p2);
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
