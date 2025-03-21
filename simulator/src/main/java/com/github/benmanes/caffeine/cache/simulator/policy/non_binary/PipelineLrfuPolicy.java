package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.NormalSource;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayDeque;
import java.util.Queue;


@Policy.PolicySpec(name = "non-binary.PipelineLrfu")
public final class PipelineLrfuPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
  final Queue<Long> requests;
  static long currentTime;
  final long firstCacheSize, secondCacheSize; // in chunks
  long currentFirstCacheSize, currentSecondCacheSize; // in chunks
  final PolicyStats policyStats;
  final Source source;
  final SearchableMinHeap<Long, Prefix> firstCacheScoreMinHeap, secondCacheScoreMinHeap;

  public PipelineLrfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.data = new Long2ObjectOpenHashMap<>();
    this.requests = new ArrayDeque<>();
    currentTime = 0;

    this.firstCacheScoreMinHeap = new SearchableMinHeap<>((int) Consts.REQUESTS_FREQUENCY_PERIOD, this::comparePrefixesFirstCache);
    this.secondCacheScoreMinHeap = new SearchableMinHeap<>((int) Consts.REQUESTS_FREQUENCY_PERIOD, this::comparePrefixesSecondCache);

    this.source = new NormalSource(1, 0.003, 0.00075);

    // Our cache size unit is in chunks, but the settings are in items/entries amount in cache.
    // So to reflect the settings in chunks, we multiply the settings size by the average chunks amount in item -
    //  which we assume is ~1024 chunks per item.
    // If we assume that a chunk size is 4KB, then an average item size is 4MB.
    this.firstCacheSize = settings.maximumSize() * Consts.ITEM_CHUNKS_AMOUNT / 2;
    this.secondCacheSize = settings.maximumSize() * Consts.ITEM_CHUNKS_AMOUNT - this.firstCacheSize;
    this.currentFirstCacheSize = 0;
    this.currentSecondCacheSize = 0;
  }

  @Override
  public void record(AccessEvent event) {
    long itemKey = event.key();
    var existingPrefix = data.getOrDefault(itemKey, null);
    policyStats.recordOperation();
    currentTime++;

    if (existingPrefix != null) {
      // prefix exist (partial hit)
      existingPrefix.lastRequestTime = currentTime;
      onRequest(existingPrefix, event.retrievalDelay());
    } else {
      // prefix missing (full miss)
      var newPrefix = new Prefix(itemKey, Consts.ITEM_CHUNKS_AMOUNT, source, currentTime);
      onRequest(newPrefix, event.retrievalDelay());
    }
  }

  private void onRequest(Prefix prefix, double sourceDelay) {
    recordRequestStatistics(prefix, sourceDelay);
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

  private void recordRequestStatistics(Prefix old, double sourceDelay) {
    // The ideal prefix size - the size that gives "no delay"/"all the item is cached" illusion
    double idealSize = Math.min(old.fullItemSizeInMB(), sourceDelay * Consts.BANDWIDTH);
    long idealChunksAmount = (long) Math.ceil(idealSize / Consts.CHUNK_SIZE);

    // Chunk Hit Rate
    policyStats.addHits(old.chunksAmount);
    policyStats.addMisses(Math.max(0, idealChunksAmount - old.chunksAmount));

    // Total delay and latency
    double underflowDelay = calculateDelay(sourceDelay, old);
    policyStats.addDelay(underflowDelay);
    double latency = calculateLatency(sourceDelay, old);
    policyStats.addLatency(latency);
  }

  private void insertChunks(Prefix prefix) {
    while (!prefix.isFull() && currentFirstCacheSize < firstCacheSize) {
      extendPrefixFirstCache(prefix);
    }
    while (!prefix.isFull() && currentSecondCacheSize < secondCacheSize) {
      extendPrefixSecondCache(prefix);
    }

    while (true) {
      Prefix victim1 = findFirstCacheVictim();
      double sPlus = prefix.pipelineFirstCacheScoreAfterInsertion();
      double sMinus = victim1.pipelineFirstCacheScoreAfterEviction();

      if (prefix.isFull() || victim1.itemKey == prefix.itemKey || sPlus < sMinus) {
        break;
      }

      shrinkPrefixFirstCache(victim1); // remove last chunk from first cache

      if (prefix.secondCacheChunksAmount == 0) {
        // no chunks of item i in second cache.
        // then we can just insert to the first cache
        // the next chunk of item i, retrieved from the source
        extendPrefixFirstCache(prefix);
      } else {
        // there are chunks of item i in the second cache.
        // then we can retrieve the next chunk by moving
        // the first chunk from the second cache to the first cache
        shrinkPrefixSecondCache(prefix);
        extendPrefixFirstCache(prefix);
      }

      if (currentSecondCacheSize < secondCacheSize) {
        // if there's place in the second cache
        // then we can move the chunk we removed from the first cache
        // to the second cache.
        // It's possible to have space in 2nd cache due to the case where
        // we move a chunk from 2nd cache to 1st cache
        // for item i.
        extendPrefixSecondCache(victim1);
      } else {
        // there's no place in the second cache.
        // so we need to find victim there
        // remove its last chunk
        // and insert the previous victim chunk to the second cache
        Prefix victim2 = findSecondCacheVictim();
        shrinkPrefixSecondCache(victim2);
        extendPrefixSecondCache(victim1);
      }
    }
  }

  private void shrinkPrefixFirstCache(Prefix prefix) {
    prefix.removeChunkFromFirstCache();
    currentFirstCacheSize--;

    firstCacheScoreMinHeap.remove(prefix.itemKey);
    if (prefix.firstCacheChunksAmount > 0) {
      firstCacheScoreMinHeap.insert(prefix.itemKey, prefix);
    }

    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  private void extendPrefixFirstCache(Prefix prefix) {
    prefix.insertChunkToFirstCache();
    currentFirstCacheSize++;

    if (firstCacheScoreMinHeap.contains(prefix.itemKey)) {
      firstCacheScoreMinHeap.remove(prefix.itemKey);
    }
    firstCacheScoreMinHeap.insert(prefix.itemKey, prefix);

    policyStats.recordOperation();
    policyStats.recordAdmission();
  }

  private void shrinkPrefixSecondCache(Prefix prefix) {
    prefix.removeChunkFromSecondCache();
    currentSecondCacheSize--;

    secondCacheScoreMinHeap.remove(prefix.itemKey);
    if (prefix.secondCacheChunksAmount > 0) {
      secondCacheScoreMinHeap.insert(prefix.itemKey, prefix);
    }

    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  private void extendPrefixSecondCache(Prefix prefix) {
    prefix.insertChunkToSecondCache();
    currentSecondCacheSize++;

    if (secondCacheScoreMinHeap.contains(prefix.itemKey)) {
      secondCacheScoreMinHeap.remove(prefix.itemKey);
    }
    secondCacheScoreMinHeap.insert(prefix.itemKey, prefix);

    policyStats.recordOperation();
    policyStats.recordAdmission();
  }

  /**
   * @return the victim chunk to be evicted, or null if no suitable one is found
   */
  private Prefix findFirstCacheVictim() {
    return firstCacheScoreMinHeap.min().value();
  }

  private Prefix findSecondCacheVictim() {
    return secondCacheScoreMinHeap.min().value();
  }

  /**
   * Calculate the full latency of fetching a partial cached object
   *
   * @param sourceDelay in seconds
   * @param prefix      the prefix of the item
   * @return the delay in seconds
   */
  private static double calculateDelay(double sourceDelay, Prefix prefix) {
    return TimeCalculations.calculateUnderflowDelay(
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

  public int comparePrefixesFirstCache(long prefixKey1, long prefixKey2) {
    Prefix p1 = data.get(prefixKey1);
    Prefix p2 = data.get(prefixKey2);
    return p1.pipelineFirstCacheCompareTo(p2);
  }

  public int comparePrefixesSecondCache(long prefixKey1, long prefixKey2) {
    Prefix p1 = data.get(prefixKey1);
    Prefix p2 = data.get(prefixKey2);
    return p1.pipelineSecondCacheCompareTo(p2);
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
