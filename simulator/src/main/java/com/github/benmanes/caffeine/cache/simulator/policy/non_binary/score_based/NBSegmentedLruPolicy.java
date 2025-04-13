package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.score_based;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.SearchableMinHeap;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.NormalSource;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayDeque;
import java.util.Queue;

@Policy.PolicySpec(name = "non-binary.score-based.SegmentedLRU")
public final class NBSegmentedLruPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
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

    this.data = new Long2ObjectOpenHashMap<>();
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

    this.source = new NormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);
  }

  @Override
  public void record(AccessEvent event) {
    long itemKey = event.key();
    Prefix prefix = data.get(itemKey);
    policyStats.recordOperation();
    currentTime++;

    if (prefix == null) {
      // First time we see this item
      prefix = new Prefix(itemKey, event.itemSize(), source, currentTime);
      data.put(itemKey, prefix);
      prefix.isInProtected = false;
      // We put new items in the probation segment
      // but no chunks allocated yet -> see waterFill()
    } else {
      prefix.lastRequestTime = currentTime;
    }

    recordRequestStatistics(prefix, event.retrievalDelay());
    handleRequestsFrequency(prefix);

    // On second reference, if not in protected, we attempt promotion
    if (!prefix.isInProtected && prefix.chunksAmount > 0) {
      promoteToProtected(prefix);
    }

    // Attempt partial caching expansions
    waterFill(prefix, event.retrievalDelay());
  }

  /**
   * Attempt to move the prefix from probation to protected.
   * If its current prefix size is bigger than <code>maxProtectedSize</code>,
   * we do not promote it (same as original logic).
   */
  private void promoteToProtected(Prefix prefix) {
    if (prefix.chunksAmount > maxProtectedSize) {
      return;
    }

    // Evict/demote from protected if necessary to make room
    while (prefix.chunksAmount + currentProtectedSize > maxProtectedSize && !protectedHeap.isEmpty()) {
      Prefix demote = protectedHeap.extractMin().value();
      demote.isInProtected = false;
      if (protectedHeap.contains(prefix.itemKey))
        protectedHeap.remove(demote.itemKey); // not strictly needed if extractMin() did that
      currentProtectedSize -= demote.chunksAmount;

      // Move demoted item to probation
      currentProbationSize += demote.chunksAmount;
      probationHeap.insert(demote.itemKey, demote);
    }

    // Actually promote
    prefix.isInProtected = true;
    if (probationHeap.contains(prefix.itemKey)) {
      probationHeap.remove(prefix.itemKey);
      currentProbationSize -= prefix.chunksAmount; // FIX: must not go negative
    }

    protectedHeap.insert(prefix.itemKey, prefix);
    currentProtectedSize += prefix.chunksAmount;
  }

  private void handleRequestsFrequency(Prefix prefix) {
    prefix.requestsCountInPeriod++;
    requests.add(prefix.itemKey);

    if (requests.size() == Consts.REQUESTS_FREQUENCY_PERIOD + 1) {
      long lastRequestItemKey = requests.remove();
      var lastPrefix = data.get(lastRequestItemKey);
      if (lastPrefix != null) {
        lastPrefix.requestsCountInPeriod--;
      }
    }
  }

  private void recordRequestStatistics(Prefix prefix, double sourceDelay) {
    double idealSizeMB = Math.min(prefix.fullItemSizeInMB(), sourceDelay * Consts.BANDWIDTH);
    long idealChunks = (long) Math.ceil(idealSizeMB / Consts.CHUNK_SIZE);

    // Partial "hit" vs. "miss" in the sense of how many chunks are already present
    policyStats.addHits(prefix.chunksAmount);
    policyStats.addMisses(Math.max(0, idealChunks - prefix.chunksAmount));

    double underflowDelay = TimeCalculations.calculateUnderflowDelay(
      sourceDelay,
      prefix.fullItemSizeInMB(),
      prefix.sizeInMB(),
      Consts.BANDWIDTH
    );
    policyStats.addDelay(underflowDelay);
  }

  /**
   * The main routine that tries to expand the prefix (partial caching)
   * without exceeding the relevant segment’s capacity or the total capacity.
   */
  private void waterFill(Prefix prefix, double sourceDelay) {
    // 1) Expand as long as we are not full and haven't hit the
    //    capacity limit (probation or protected).
    while (!prefix.isFull()) {
      if (prefix.isInProtected) {
        // If in protected, check if we can add another chunk
        if (currentProtectedSize < maxProtectedSize
          && (currentProbationSize + currentProtectedSize) < maximumCacheSize) {
          extendPrefix(prefix);
        } else {
          break;
        }
      } else {
        // If in probation, check probation capacity
        if (currentProbationSize < maxProbationSize
          && (currentProbationSize + currentProtectedSize) < maximumCacheSize) {
          extendPrefix(prefix);
        } else {
          break;
        }
      }
    }

    // 2) Possibly evict from other items (the "victim") to free space,
    //    if the new chunk would yield a bigger improvement than the victim's chunk.
    while (!prefix.isFull()) {
      Prefix victim = findVictim();
      if (victim == null || victim.itemKey == prefix.itemKey) {
        break;
      }
      double sPlus = prefix.lruScoreAfterInsertion();
      double sMinus = victim.lruScoreAfterEviction();

      // If not worth evicting from 'victim' to add to 'prefix', stop
      if (sPlus < sMinus) {
        break;
      }
      shrinkPrefix(victim);
      extendPrefix(prefix);
    }
  }

  /**
   * Remove one chunk from victim.
   */
  private void shrinkPrefix(Prefix prefix) {
    if (prefix.isEmpty()) {
      return;
    }
    prefix.removeChunk();
    if (prefix.isInProtected) {
      if (protectedHeap.contains(prefix.itemKey)) protectedHeap.remove(prefix.itemKey);
      currentProtectedSize--;
      if (prefix.chunksAmount > 0) {
        if (protectedHeap.contains(prefix.itemKey)) protectedHeap.insert(prefix.itemKey, prefix);
      } else {
        prefix.isInProtected = false; // Possibly becomes empty -> no queue?
      }
    } else {
      if (probationHeap.contains(prefix.itemKey)) probationHeap.remove(prefix.itemKey);
      currentProbationSize--;
      if (prefix.chunksAmount > 0) {
        probationHeap.insert(prefix.itemKey, prefix);
      }
    }
    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  /**
   * Add one chunk to prefix, if not full.
   */
  private void extendPrefix(Prefix prefix) {
    if (prefix.isFull()) {
      return;
    }
    prefix.insertChunk();

    if (prefix.isInProtected) {
      if (protectedHeap.contains(prefix.itemKey)) protectedHeap.remove(prefix.itemKey);
      currentProtectedSize++;
      protectedHeap.insert(prefix.itemKey, prefix);
    } else {
      if (probationHeap.contains(prefix.itemKey)) probationHeap.remove(prefix.itemKey);
      currentProbationSize++;
      probationHeap.insert(prefix.itemKey, prefix);
    }
    policyStats.recordOperation();
    policyStats.recordAdmission();
  }

  /**
   * Standard SLRU approach: always evict from probation if not empty,
   * else evict from protected.
   * <p>
   * If you want to evict purely based on the minimal LRU score across
   * _both_ queues, you could compare the min of each heap instead.
   */
  private Prefix findVictim() {
    if (!probationHeap.isEmpty()) {
      return probationHeap.min().value();
    }
    if (!protectedHeap.isEmpty()) {
      return protectedHeap.min().value();
    }
    return null;
  }

  public int compareProbation(long key1, long key2) {
    return data.get(key1).lruCompareTo(data.get(key2));
  }

  public int compareProtected(long key1, long key2) {
    return data.get(key1).lruCompareTo(data.get(key2));
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
    final long fullItemChunksAmount;
    final Source source;
    long chunksAmount;          // how many chunks are currently cached
    long requestsCountInPeriod;
    long lastRequestTime;
    boolean isInProtected;

    Prefix(long itemKey, long fullItemChunksAmount, Source source, long currentTime) {
      this.itemKey = itemKey;
      this.fullItemChunksAmount = fullItemChunksAmount;
      this.source = source;
      this.lastRequestTime = currentTime;
      this.requestsCountInPeriod = 0;
      this.chunksAmount = 0;
      this.isInProtected = false;
    }

    double lruScore() {
      double prefixTxTime = TimeCalculations.calculateTransmissionTime(sizeInMB(), Consts.BANDWIDTH);
      return recency() * (1 - source.calculateCDF(prefixTxTime));
    }

    double lruScoreAfterInsertion() {
      if (isFull()) {
        return 0;
      }
      double txTime = TimeCalculations.calculateTransmissionTime(sizeInMB() + Consts.CHUNK_SIZE, Consts.BANDWIDTH);
      return recency() * (1 - source.calculateCDF(txTime));
    }

    double lruScoreAfterEviction() {
      if (isEmpty()) {
        // If we remove from an empty prefix, we do not have a chunk to remove
        // so let's just treat that as a minimal leftover
        return recency();
      }
      double txTime = TimeCalculations.calculateTransmissionTime(sizeInMB() - Consts.CHUNK_SIZE, Consts.BANDWIDTH);
      return recency() * (1 - source.calculateCDF(txTime));
    }

    /**
     * 1 / (now - lastRequestTime + 1)
     */
    double recency() {
      return 1.0 / (NBSegmentedLruPolicy.currentTime - lastRequestTime + 1);
    }

    double sizeInMB() {
      return chunksAmount * Consts.CHUNK_SIZE;
    }

    double fullItemSizeInMB() {
      return fullItemChunksAmount * Consts.CHUNK_SIZE;
    }

    boolean isFull() {
      return chunksAmount == fullItemChunksAmount;
    }

    boolean isEmpty() {
      return (chunksAmount == 0);
    }

    void insertChunk() {
      if (!isFull()) {
        chunksAmount++;
      }
    }

    void removeChunk() {
      if (!isEmpty()) {
        chunksAmount--;
      }
    }

    int lruCompareTo(Prefix other) {
      return Double.compare(this.lruScore(), other.lruScore());
    }
  }
}
