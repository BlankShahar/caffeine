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
  final long fullCacheSize; // in chunks
  long firstCacheSize, secondCacheSize;
  long currentFirstCacheSize, currentSecondCacheSize; // in chunks
  final long refinementInterval;
  final double stepSize;
  double q, ratio;
  double previousTotalDelay, currentTotalDelay;
  final PolicyStats policyStats;
  final Source source;
  final SearchableMinHeap<Long, Prefix> firstCacheScoreMinHeap, secondCacheScoreMinHeap;

  public PipelineLrfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.data = new Long2ObjectOpenHashMap<>();
    this.requests = new ArrayDeque<>();
    currentTime = 0;

    q = 1;
    ratio = 0.5;
    refinementInterval = 1;
    stepSize = 0.05;
    previousTotalDelay = 0;
    currentTotalDelay = 0;

    this.firstCacheScoreMinHeap = new SearchableMinHeap<>((int) settings.maximumSize() * 1_000, this::comparePrefixesFirstCache);
    this.secondCacheScoreMinHeap = new SearchableMinHeap<>((int) settings.maximumSize() * 1_000, this::comparePrefixesSecondCache);

    this.source = new NormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);

    // Our cache size unit is in chunks, but the settings are in items/entries amount in cache.
    // So to reflect the settings in chunks, we multiply the settings size by the average chunks amount in item -
    //  which we assume is ~1024 chunks per item.
    // If we assume that a chunk size is 4KB, then an average item size is 4MB.
    this.fullCacheSize = settings.maximumSize() * Consts.ITEM_CHUNKS_AMOUNT;
    this.firstCacheSize = (long) Math.floor(ratio * fullCacheSize);
    this.secondCacheSize = (long) Math.ceil((1 - ratio) * fullCacheSize);
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
    updateParameters(sourceDelay);

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

    // Total delay and latency
    double underflowDelay = calculateDelay(sourceDelay, old);
    policyStats.addDelay(underflowDelay);
    double latency = calculateLatency(sourceDelay, old);
    policyStats.addLatency(latency);
  }

  private void updateParameters(double retrievalDelay) {
    currentTotalDelay += retrievalDelay;

    if (currentTime % refinementInterval == 0) {
      if (currentTotalDelay < previousTotalDelay) {
        q += stepSize;
      } else {
        q = Math.max(0, q - stepSize);
      }
      double previousFirstCacheSize = firstCacheSize, previousSecondCacheSize = secondCacheSize;

      ratio = 1 / Math.pow(2, q);
      firstCacheSize = (long) Math.floor(ratio * fullCacheSize);
      secondCacheSize = (long) Math.ceil((1 - ratio) * fullCacheSize);

      double x = Math.max(0, previousFirstCacheSize - firstCacheSize);
      double y = Math.max(0, previousSecondCacheSize - secondCacheSize);

      for (int k = 0; k < Math.min(x, firstCacheScoreMinHeap.size); k++) {
        // move the x lowest scored chunks from 1st cache to 2nd cache
        Prefix victim = findFirstCacheVictim();
        shrinkPrefixFirstCache(victim);
        extendPrefixSecondCache(victim);
      }
      for (int k = 0; k < Math.min(y, secondCacheScoreMinHeap.size); k++) {
        // move the y lowest scored chunks from 2nd cache to 1st cache
        Prefix victim = findSecondCacheVictim();
        shrinkPrefixSecondCache(victim);
        extendPrefixFirstCache(victim);
      }

      previousTotalDelay = currentTotalDelay;
      currentTotalDelay = 0;
    }
  }


  private void waterFill(Prefix prefix) {
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
    if (prefix.firstCacheChunksAmount == 0) {
      return;
    }
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
    if (prefix.firstCacheChunksAmount == prefix.fullItemChunksAmount) {
      return;
    }
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
    if (prefix.secondCacheChunksAmount == 0) {
      return;
    }
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
    if (prefix.secondCacheChunksAmount == prefix.fullItemChunksAmount) {
      return;
    }
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
