package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.generic;

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

import static com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent.Operation.READ;
import static com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations.calculateLatency;

@Policy.PolicySpec(name = "non-binary.SegmentedLRU")
public final class NBSegmentedLruPolicy implements Policy {
  final Queue<Long> requests;
  static long currentTime;

  final long maximumCacheSize;
  final long maxProtectedSize;
  final long maxProbationSize;

  long currentProbationSize;
  long currentProtectedSize;

  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Prefix> probationHeap;
  final SearchableMinHeap<Long, Prefix> protectedHeap;
  final Source source;

  public NBSegmentedLruPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.requests = new ArrayDeque<>();
    currentTime = 0;

    this.maximumCacheSize = settings.maximumSize();
    // Typically ~80% to protected, ~20% to probation
    this.maxProtectedSize = (long) (maximumCacheSize * 0.8);
    this.maxProbationSize = maximumCacheSize - maxProtectedSize;
    this.currentProbationSize = 0;
    this.currentProtectedSize = 0;

    this.probationHeap = new SearchableMinHeap<>((int) maxProbationSize, this::compareProbation);
    this.protectedHeap = new SearchableMinHeap<>((int) maxProtectedSize, this::compareProtected);

    this.source = new LogNormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);
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
    onDelete(event);
    onRead(event);
  }

  private void onDelete(AccessEvent event) {
    var existingPrefix = probationHeap.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      probationHeap.remove(existingPrefix.itemKey);
      policyStats.recordOperation();
      currentProbationSize -= existingPrefix.currentSize;
      policyStats.recordEviction();
    }

    existingPrefix = protectedHeap.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      protectedHeap.remove(existingPrefix.itemKey);
      policyStats.recordOperation();
      currentProtectedSize -= existingPrefix.currentSize;
      policyStats.recordEviction();
    }
  }

  private void onRead(AccessEvent event) {
    long itemKey = event.key();
    Prefix prefix = Optional.ofNullable(probationHeap.get(itemKey)).orElse(protectedHeap.get(itemKey));

    if (prefix == null) {
      // First time we see this item
      long currentSize = event.itemSize();
      prefix = new Prefix(itemKey, currentSize, source, currentTime);
      prefix.isInProtected = false;
      // We put new items in the probation segment
    } else prefix.lastRequestTime = currentTime;

    if (event.operation() == READ) {
      if (probationHeap.contains(itemKey) || protectedHeap.contains(itemKey))
        recordRequestStatistics(prefix, event.retrievalDelay());
      else {
        policyStats.addDelay(event.retrievalDelay());
        double latency = calculateLatency(event.retrievalDelay(), prefix.fullItemSize(), 0, Consts.BANDWIDTH);
        policyStats.addLatency(latency);
      }
    }
    handleRequestsFrequency(prefix);

    // On second reference, if not in protected, we attempt promotion
    if (!prefix.isInProtected && prefix.currentSize > 0) promoteToProtected(prefix);

    // Attempt partial caching expansions
    waterFill(prefix);
  }

  /**
   * Attempt to move the prefix from probation to protected.
   * If its current prefix size is bigger than <code>maxProtectedSize</code>,
   * we do not promote it (same as original logic).
   */
  private void promoteToProtected(Prefix prefix) {
    if (prefix.currentSize > maxProtectedSize) return;

    // Evict/demote from protected if necessary to make room
    while (prefix.currentSize + currentProtectedSize > maxProtectedSize && !protectedHeap.isEmpty()) {
      Prefix demote = protectedHeap.extractMin().value();
      policyStats.recordOperation();
      demote.isInProtected = false;
      currentProtectedSize -= demote.currentSize;

      if (demote.currentSize > maxProbationSize) continue;
      // Free up space in probation if needed
      while (demote.currentSize + currentProbationSize > maxProbationSize) {
        Prefix eviction = probationHeap.extractMin().value();
        policyStats.recordOperation();
        currentProbationSize -= eviction.currentSize;
        eviction.currentSize = 0;
        policyStats.recordEviction();
      }
      // Move demoted item to probation
      currentProbationSize += demote.currentSize;
      probationHeap.upsert(demote.itemKey, demote);
      policyStats.recordOperation();
    }

    assert currentProtectedSize <= maxProtectedSize : "Protected size exceeds maximum protected size (current time: " + currentTime + ")";
    assert currentProtectedSize >= 0 : "Protected size cannot be negative (current time: " + currentTime + ")";
    assert currentProbationSize <= maxProbationSize : "Probation size exceeds maximum probation size (current time: " + currentTime + ")";
    assert currentProbationSize >= 0 : "Probation size cannot be negative (current time: " + currentTime + ")";

    // Actually promote
    prefix.isInProtected = true;
    if (probationHeap.contains(prefix.itemKey)) {
      probationHeap.remove(prefix.itemKey);
      policyStats.recordOperation();
      currentProbationSize -= prefix.currentSize;
    }
    protectedHeap.upsert(prefix.itemKey, prefix);
    policyStats.recordOperation();
    currentProtectedSize += prefix.currentSize;

    assert currentProtectedSize <= maxProtectedSize : "Protected size exceeds maximum protected size (current time: " + currentTime + ")";
    assert currentProtectedSize >= 0 : "Protected size cannot be negative (current time: " + currentTime + ")";
    assert currentProbationSize <= maxProbationSize : "Probation size exceeds maximum probation size (current time: " + currentTime + ")";
    assert currentProbationSize >= 0 : "Probation size cannot be negative (current time: " + currentTime + ")";
  }

  private void handleRequestsFrequency(Prefix prefix) {
    prefix.requestsCountInPeriod++;
    requests.add(prefix.itemKey);

    if (requests.size() == Consts.REQUESTS_FREQUENCY_PERIOD + 1) {
      long lastRequestItemKey = requests.remove();
      var lastPrefix = Optional.ofNullable(probationHeap.get(lastRequestItemKey)).orElse(protectedHeap.get(lastRequestItemKey));
      if (lastPrefix != null) {
        lastPrefix.requestsCountInPeriod--;
      }
    }
  }

  private void recordRequestStatistics(Prefix prefix, double sourceDelay) {
    double underflowDelay = TimeCalculations.calculateUnderflowDelay(sourceDelay, prefix.fullItemSize(), prefix.currentSize(), Consts.BANDWIDTH);
    policyStats.addDelay(underflowDelay);
    double latency = calculateLatency(sourceDelay, prefix.fullItemSize(), prefix.currentSize(), Consts.BANDWIDTH);
    policyStats.addLatency(latency);
  }

  /**
   * The main routine that tries to expand the prefix (partial caching)
   * without exceeding the relevant segment’s capacity or the total capacity.
   */
  private void waterFill(Prefix prefix) {
    // 1) Expand as long as we are not full and haven't hit the
    //    capacity limit (probation or protected).
    long addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    if (prefix.isInProtected)
      while (!prefix.isFull() && currentProtectedSize + addSize <= maxProtectedSize) {
        extendPrefix(prefix);
        addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
      }
    else while (!prefix.isFull() && currentProbationSize + addSize <= maxProbationSize) {
      extendPrefix(prefix);
      addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    }

    if (prefix.isFull() || maximumCacheSize == 0) return;

    // 2) Possibly evict from other items (the "victim") to free space,
    //    if the new chunk would yield a bigger improvement than the victim's chunk.
    Prefix victim;
    do {
      extendPrefix(prefix);

      boolean isCacheOverflowing;
      do {
        if (prefix.isInProtected) victim = findVictimFromProtected();
        else victim = findVictimFromProbation();
        shrinkPrefix(victim);

        isCacheOverflowing = prefix.isInProtected ?
          currentProtectedSize > maxProtectedSize : currentProbationSize > maxProbationSize;
      } while (isCacheOverflowing);

    } while (!(prefix.isFull() || victim.itemKey == prefix.itemKey));
  }

  /**
   * Remove one chunk from victim.
   */
  private void shrinkPrefix(Prefix prefix) {
    if (prefix.isEmpty())
      return;

    long removedSize = prefix.removeChunk();
    if (prefix.isInProtected) {
      currentProtectedSize -= removedSize;

      if (!prefix.isEmpty()) {
        protectedHeap.upsert(prefix.itemKey, prefix);
      } else {
        protectedHeap.remove(prefix.itemKey);
        prefix.isInProtected = false;
      }
    } else {
      currentProbationSize -= removedSize;
      if (prefix.isEmpty()) probationHeap.remove(prefix.itemKey);
      else probationHeap.upsert(prefix.itemKey, prefix);
    }
    policyStats.recordOperation();
    policyStats.recordEviction();

    assert prefix.currentSize >= 0 : "Prefix size cannot be negative";
    assert currentProtectedSize >= 0 : "Current protected cache size cannot be negative";
    assert currentProbationSize >= 0 : "Current probation cache size cannot be negative";
  }

  /**
   * Add one chunk to prefix, if not full.
   */
  private void extendPrefix(Prefix prefix) {
    if (prefix.isFull())
      return;

    long addedSize = prefix.insertChunk();

    if (prefix.isInProtected) {
      currentProtectedSize += addedSize;
      protectedHeap.upsert(prefix.itemKey, prefix);
    } else {
      currentProbationSize += addedSize;
      probationHeap.upsert(prefix.itemKey, prefix);
    }
    policyStats.recordOperation();
    policyStats.recordAdmission();

    assert prefix.currentSize <= prefix.fullItemSize : "Prefix size exceeds its full size (current time: " + currentTime + ")";
  }

  /**
   * Standard SLRU approach: always evict from probation if not empty,
   * else evict from protected.
   * <p>
   * If you want to evict purely based on the minimal LRU score across
   * _both_ queues, you could compare the min of each heap instead.
   */
  private Prefix findVictimFromProtected() {
    if (!protectedHeap.isEmpty()) return protectedHeap.min().value();
    return null;
  }

  private Prefix findVictimFromProbation() {
    if (!probationHeap.isEmpty()) return probationHeap.min().value();
    return null;
  }

  public int compareProbation(long key1, long key2) {
    Prefix p1 = probationHeap.get(key1);
    Prefix p2 = probationHeap.get(key2);
    assert p1 != null;
    assert p2 != null;
    return p1.lruCompareTo(p2);
  }

  public int compareProtected(long key1, long key2) {
    Prefix p1 = protectedHeap.get(key1);
    Prefix p2 = protectedHeap.get(key2);
    assert p1 != null;
    assert p2 != null;
    return p1.lruCompareTo(p2);
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

  /**
   * A partial-caching "Prefix" that supports the SLRU logic.
   */
  static class Prefix {
    final long itemKey;
    final long fullItemSize;
    final Source source;
    long currentSize;
    long requestsCountInPeriod;
    long lastRequestTime;
    boolean isInProtected;

    Prefix(long itemKey, long fullItemSize, Source source, long currentTime) {
      this.itemKey = itemKey;
      this.fullItemSize = fullItemSize;
      this.source = source;
      this.lastRequestTime = currentTime;
      this.requestsCountInPeriod = 0;
      this.currentSize = 0;
      this.isInProtected = false;
    }

    double lruScore() {
      double prefixTxTime = TimeCalculations.calculateTransmissionTime(currentSize, Consts.BANDWIDTH);
      return recency() * (1 - source.calculateCDF(prefixTxTime));
    }

    double recency() {
      return 1.0 / (currentTime - lastRequestTime + 1);
    }

    double currentSize() {
      return currentSize;
    }

    double fullItemSize() {
      return fullItemSize;
    }

    boolean isFull() {
      return currentSize == fullItemSize;
    }

    boolean isEmpty() {
      return (currentSize == 0);
    }

    public long insertChunk() {
      long addSize = Math.min(fullItemSize - currentSize, Consts.CHUNK_SIZE);
      currentSize += addSize;
      return addSize;
    }

    public long removeChunk() {
      long remainder = currentSize % Consts.CHUNK_SIZE;
      long removeSize = (remainder > 0) ? remainder : Consts.CHUNK_SIZE;
      currentSize -= removeSize;
      return removeSize;
    }

    int lruCompareTo(Prefix other) {
      return Double.compare(this.lruScore(), other.lruScore());
    }
  }
}
