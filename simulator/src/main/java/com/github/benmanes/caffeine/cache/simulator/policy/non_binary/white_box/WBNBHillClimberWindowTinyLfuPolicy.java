package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.white_box;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.SearchableMinHeap;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.LogNormalSource;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;
import com.typesafe.config.Config;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

import static com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations.calculateLatency;


@Policy.PolicySpec(name = "non-binary.white-box.HillClimberWindowTinyLFU")
public final class WBNBHillClimberWindowTinyLfuPolicy implements Policy {
  final Queue<Long> requests;
  static long currentTime;
  final long maximumCacheSize; // in chunks
  long firstCacheSize, secondCacheSize;
  long currentFirstCacheSize, currentSecondCacheSize; // in chunks
  final long REFINEMENT_INTERVAL;
  final double STEP_SIZE;
  double q, ratio;
  double previousTotalDelay, currentTotalDelay;
  final PolicyStats policyStats;
  final Source source;
  final SearchableMinHeap<Long, Prefix> firstCacheScoreMinHeap, secondCacheScoreMinHeap;

  public WBNBHillClimberWindowTinyLfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.requests = new ArrayDeque<>();

    this.firstCacheScoreMinHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::comparePrefixesFirstCache);
    this.secondCacheScoreMinHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::comparePrefixesSecondCache);

    this.source = new LogNormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);


    this.maximumCacheSize = settings.maximumSize();
    this.firstCacheSize = (long) Math.floor(ratio * maximumCacheSize);
    this.secondCacheSize = (long) Math.ceil((1 - ratio) * maximumCacheSize);
    this.currentFirstCacheSize = 0;
    this.currentSecondCacheSize = 0;

    currentTime = 0;
    q = 1;
    ratio = 0.5;
    REFINEMENT_INTERVAL = 1_000_000;
    STEP_SIZE = 0.05;
    previousTotalDelay = 0;
    currentTotalDelay = 0;
  }


  @Override
  public void record(AccessEvent event) {
    currentTime++;
    switch (event.operation()) {
      case READ:
        onRead(event);
        break;
      case WRITE:
        onWrite(event);
        break;
      case DELETE:
        onDelete(event);
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + event.operation());
    }
  }

  private void onWrite(AccessEvent event) {
    policyStats.recordOperation();
    onDelete(event);
    onRead(event);
  }

  private void onDelete(AccessEvent event) {
    var existingPrefix = firstCacheScoreMinHeap.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      firstCacheScoreMinHeap.remove(existingPrefix.itemKey);
      currentFirstCacheSize -= existingPrefix.chunksAmount;
      policyStats.recordEviction();
      policyStats.recordOperation();
    }

    existingPrefix = secondCacheScoreMinHeap.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      secondCacheScoreMinHeap.remove(existingPrefix.itemKey);
      currentSecondCacheSize -= existingPrefix.chunksAmount;
      policyStats.recordEviction();
      policyStats.recordOperation();
    }
  }

  private void onRead(AccessEvent event) {
    long itemKey = event.key();
    var existingPrefix = Optional.ofNullable(
      firstCacheScoreMinHeap.get(itemKey)).orElse(
      secondCacheScoreMinHeap.get(itemKey)
    );
    policyStats.recordOperation();

    if (existingPrefix != null) {
      // prefix exist (partial hit)
      existingPrefix.lastRequestTime = currentTime;
      onRequest(existingPrefix, event.retrievalDelay());
    } else {
      // prefix missing (full miss)
      long chunksAmount = event.itemSize(); // (long) Math.ceil(event.itemSize() / (Consts.CHUNK_SIZE * 1024 * 1024));
      var newPrefix = new Prefix(itemKey, chunksAmount, source, currentTime);
      onRequest(newPrefix, event.retrievalDelay());
    }
  }

  private void onRequest(Prefix prefix, double sourceDelay) {
    recordRequestStatistics(prefix, sourceDelay);
    handleRequestsFrequency(prefix);
    updateParameters(sourceDelay);

    if (maximumCacheSize > 0) waterFill(prefix);
  }

  private void handleRequestsFrequency(Prefix prefix) {
    prefix.requestsCountInPeriod++;

    requests.add(prefix.itemKey);
    if (requests.size() == Consts.REQUESTS_FREQUENCY_PERIOD + 1) {
      long lastRequestItemKey = requests.remove();
      var lastRequestedPrefix = Optional.ofNullable(
        firstCacheScoreMinHeap.get(lastRequestItemKey)).orElse(
        secondCacheScoreMinHeap.get(lastRequestItemKey)
      );
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

    double latency = calculateLatency(sourceDelay, old.fullItemSizeInMB(), old.sizeInMB(), Consts.BANDWIDTH);
    policyStats.addLatency(latency);
  }

  private void updateParameters(double retrievalDelay) {
    currentTotalDelay += retrievalDelay;

    if (currentTime % REFINEMENT_INTERVAL == 0) {
      if (currentTotalDelay < previousTotalDelay) {
        q += STEP_SIZE;
      } else {
        q = Math.max(0, q - STEP_SIZE);
      }
      double previousFirstCacheSize = firstCacheSize, previousSecondCacheSize = secondCacheSize;

      ratio = 1 / Math.pow(2, q);
      firstCacheSize = (long) Math.floor(ratio * maximumCacheSize);
      secondCacheSize = (long) Math.ceil((1 - ratio) * maximumCacheSize);

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

    if (prefix.isFull()) return;

    while (true) {
      if (firstCacheSize == 0) { // If there's only a second cache, act as regular non-binary
        Prefix victim = findSecondCacheVictim();
        double sPlus = prefix.lfuScoreAfterInsertion();
        double sMinus = victim.lfuScoreAfterEviction();

        if (prefix.isFull() || victim.itemKey == prefix.itemKey || sPlus < sMinus) {
          break;
        }

        shrinkPrefixSecondCache(victim);
        extendPrefixSecondCache(prefix);
        continue;
      }

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
        if (!secondCacheScoreMinHeap.isEmpty()) {
          Prefix victim2 = findSecondCacheVictim();
          shrinkPrefixSecondCache(victim2);
          extendPrefixSecondCache(victim1);
        }
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
      firstCacheScoreMinHeap.upsert(prefix.itemKey, prefix);
    }

    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  private void extendPrefixFirstCache(Prefix prefix) {
    if (prefix.isFull()) {
      return;
    }
    prefix.insertChunkToFirstCache();
    currentFirstCacheSize++;

    if (firstCacheScoreMinHeap.contains(prefix.itemKey)) {
      firstCacheScoreMinHeap.remove(prefix.itemKey);
    }
    firstCacheScoreMinHeap.upsert(prefix.itemKey, prefix);

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
      secondCacheScoreMinHeap.upsert(prefix.itemKey, prefix);
    }

    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  private void extendPrefixSecondCache(Prefix prefix) {
    if (prefix.isFull()) {
      return;
    }
    prefix.insertChunkToSecondCache();
    currentSecondCacheSize++;

    if (secondCacheScoreMinHeap.contains(prefix.itemKey)) {
      secondCacheScoreMinHeap.remove(prefix.itemKey);
    }
    secondCacheScoreMinHeap.upsert(prefix.itemKey, prefix);

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
   * Calculate the delay of fetching a partial cached object
   *
   * @param sourceDelay in seconds
   * @param prefix      the prefix of the item
   * @return the delay in seconds
   */
  private static double calculateDelay(double sourceDelay, Prefix prefix) {
    return TimeCalculations.calculateUnderflowDelay(sourceDelay, prefix.fullItemSizeInMB(), prefix.sizeInMB(), Consts.BANDWIDTH);
  }

  public int comparePrefixesFirstCache(long prefixKey1, long prefixKey2) {
    Prefix p1 = firstCacheScoreMinHeap.get(prefixKey1);
    Prefix p2 = firstCacheScoreMinHeap.get(prefixKey2);
    if (p1 == null || p2 == null) {
      throw new IllegalStateException("Prefix not found in heap: " + prefixKey1 + " or " + prefixKey2);
    }
    return p1.pipelineFirstCacheCompareTo(p2);
  }

  public int comparePrefixesSecondCache(long prefixKey1, long prefixKey2) {
    Prefix p1 = secondCacheScoreMinHeap.get(prefixKey1);
    Prefix p2 = secondCacheScoreMinHeap.get(prefixKey2);
    if (p1 == null || p2 == null) {
      throw new IllegalStateException("Prefix not found in heap: " + prefixKey1 + " or " + prefixKey2);
    }
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

  static public class Prefix {
    final long itemKey, fullItemChunksAmount;
    final Source source;
    long chunksAmount;
    long requestsCountInPeriod;
    long lastRequestTime;
    long firstCacheChunksAmount, secondCacheChunksAmount;

    public Prefix(long itemKey, long fullItemChunksAmount, Source source, long currentTime) {
      this.itemKey = itemKey;
      this.fullItemChunksAmount = fullItemChunksAmount;
      this.source = source;
      this.requestsCountInPeriod = 0;
      this.lastRequestTime = currentTime;
      this.chunksAmount = 0;
      this.firstCacheChunksAmount = 0;
      this.secondCacheChunksAmount = 0;
    }

    public double lfuScore() {
      // Idea - frequency times the probability of not experiencing delay
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
        sizeInMB(),
        Consts.BANDWIDTH
      );
      return frequency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double lfuScoreAfterInsertion() {
      if (isFull()) {
        return 0; // 1-CDF value is 0
      }

      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
        sizeInMB() + Consts.CHUNK_SIZE,
        Consts.BANDWIDTH
      );
      return frequency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double lfuScoreAfterEviction() {
      if (isEmpty()) {
        return frequency(); // 1-CDF value is 1
      }

      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
        sizeInMB() - Consts.CHUNK_SIZE,
        Consts.BANDWIDTH
      );
      return frequency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double pipelineFirstCacheScore() {
      // Idea - frequency times recency times the probability of not experiencing delay
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
        firstCacheChunksAmount * Consts.CHUNK_SIZE,
        Consts.BANDWIDTH
      );
      return recency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double pipelineFirstCacheScoreAfterInsertion() {
      if (firstCacheChunksAmount == fullItemChunksAmount) {
        return 0; // 1-CDF value is 0
      }

      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
        (firstCacheChunksAmount + 1) * Consts.CHUNK_SIZE,
        Consts.BANDWIDTH
      );
      return recency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double pipelineFirstCacheScoreAfterEviction() {
      if (firstCacheChunksAmount == 0) {
        return recency(); // 1-CDF value is 1
      }

      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
        (firstCacheChunksAmount - 1) * Consts.CHUNK_SIZE,
        Consts.BANDWIDTH
      );
      return recency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double pipelineSecondCacheScore() {
      return this.lfuScore();
    }

    public double pipelineSecondCacheScoreAfterInsertion() {
      return this.lfuScoreAfterInsertion();
    }

    public double pipelineSecondCacheScoreAfterEviction() {
      return this.lfuScoreAfterEviction();
    }

    public double frequency() {
      return (double) requestsCountInPeriod / Consts.REQUESTS_FREQUENCY_PERIOD;
    }

    public double recency() {
      return (double) 1 / (currentTime - lastRequestTime + 1);
    }

    public void insertChunkToFirstCache() {
      if (firstCacheChunksAmount < fullItemChunksAmount) {
        firstCacheChunksAmount++;
        chunksAmount++;
      }
    }

    public void removeChunkFromFirstCache() {
      if (firstCacheChunksAmount > 0) {
        firstCacheChunksAmount--;
        chunksAmount--;
      }
    }

    public void insertChunkToSecondCache() {
      if (secondCacheChunksAmount < fullItemChunksAmount) {
        secondCacheChunksAmount++;
        chunksAmount++;
      }
    }

    public void removeChunkFromSecondCache() {
      if (secondCacheChunksAmount > 0) {
        secondCacheChunksAmount--;
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

    public int pipelineFirstCacheCompareTo(Prefix other) {
      return Double.compare(this.pipelineFirstCacheScore(), other.pipelineFirstCacheScore());
    }

    public int pipelineSecondCacheCompareTo(Prefix other) {
      return Double.compare(this.pipelineSecondCacheScore(), other.pipelineSecondCacheScore());
    }
  }

}
