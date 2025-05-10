package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.generic2;

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

@Policy.PolicySpec(name = "non-binary.generic.SegmentedLRU")
public final class G2NBSegmentedLruPolicy implements Policy {
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

  public G2NBSegmentedLruPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.data = new Long2ObjectOpenHashMap<>();
    this.requests = new ArrayDeque<>();
    currentTime = 0;

    this.maximumCacheSize = settings.maximumSize();
    // ~80% to protected, ~20% to probation
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
    Prefix existingPrefix = data.get(itemKey);
    policyStats.recordOperation();
    currentTime++;

    if (existingPrefix != null) {
      // prefix exist (partial hit)
      existingPrefix.lastRequestTime = currentTime;
      onRequest(existingPrefix, event.retrievalDelay());
    } else {
      // prefix missing (full miss)
      var newPrefix = new Prefix(itemKey, event.itemSize(), source, currentTime);
      onRequest(newPrefix, event.retrievalDelay());
    }
  }

  private void onRequest(Prefix prefix, double sourceDelay) {
    recordRequestStatistics(prefix, sourceDelay);

    if (!data.containsKey(prefix.itemKey)) {
      data.put(prefix.itemKey, prefix);
    }
    handleRequest(prefix);
  }

  private void recordRequestStatistics(Prefix prefix, double sourceDelay) {
    double idealSizeMB = Math.min(prefix.fullItemSizeInMB(), sourceDelay * Consts.BANDWIDTH);
    long idealChunks = (long) Math.ceil(idealSizeMB / Consts.CHUNK_SIZE);

    policyStats.addHits(prefix.chunksAmount);
    policyStats.addMisses(Math.max(0, idealChunks - prefix.chunksAmount));

    double underflowDelay = TimeCalculations.calculateUnderflowDelay(sourceDelay, prefix.fullItemSizeInMB(), prefix.sizeInMB(), Consts.BANDWIDTH);
    policyStats.addDelay(underflowDelay);
  }

  private void handleRequest(Prefix prefix) {
    boolean isFirstRequest = prefix.chunksInProtected == 0;
    if (prefix.chunksInProbation >= 0) handleInProbation(prefix, isFirstRequest);
    else handleInProtected(prefix);
  }

  private void handleInProbation(Prefix prefix, boolean isFirstRequest) {
    if (isFirstRequest) waterFillProbation(prefix);
    else { // 2nd request - move chunks to protected (promote) in a water fill manner
      promoteFlowToProtected(prefix);
      waterFillProtected(prefix);
    }
  }

  private void handleInProtected(Prefix prefix) {
    waterFillProtected(prefix);
  }

  private void promoteFlowToProtected(Prefix prefix) {
    long chunksToMove = Math.min(
      prefix.chunksAmount,
      maxProbationSize - currentProbationSize
    );
    shrinkPrefixInProbation(prefix, chunksToMove);
    extendPrefixInProtected(prefix, chunksToMove);

    if (prefix.isFull()) return;

    Prefix protected_victim;
    do {
      protected_victim = findProtectedVictim(); // demoted
      shrinkPrefixInProtected(protected_victim, 1);
      shrinkPrefixInProbation(prefix, 1);
      extendPrefixInProbation(protected_victim, 1);
      extendPrefixInProtected(prefix, 1);
    } while (
      !(prefix.isFull() || protected_victim.itemKey == prefix.itemKey)
    );
  }

  private void waterFillProbation(Prefix prefix) {
    long chunksToFill = Math.min(
      prefix.fullItemChunksAmount - prefix.chunksAmount,
      maxProbationSize - currentProbationSize
    );
    extendPrefixInProbation(prefix, chunksToFill);

    Prefix victim;
    do {
      victim = findProbationVictim();
      shrinkPrefixInProbation(victim, 1);
      extendPrefixInProbation(prefix, 1);
    } while (!(prefix.isFull() || victim.itemKey == prefix.itemKey));
  }

  private void waterFillProtected(Prefix prefix) {
    // TODO: Implement water fill for protected prefixes -
    //  let x be the amount of chunks to fill the protected entirely
    //  let y be the amount of chunks to fill the item entirely
    //  insert min(x,y) chunks for the requested item.
    //  do:
    //    1. if the probation is full - remove 1 victim chunk from the probation
    //    2. remove 1 victim chunk from probation and insert it to the probation
    //    3. insert new chunk to protected for the requested item
    //  while: the requested item is not full AND the none of the victims is not the requested item
    long chunksToFill = Math.min(
      prefix.fullItemChunksAmount - prefix.chunksAmount,
      maxProtectedSize - currentProtectedSize
    );
    extendPrefixInProtected(prefix, chunksToFill);

    if (prefix.isFull()) return;

    Prefix protected_victim, probation_victim = null;
    do {
      if (probationHeap.isEmpty()) {
        probation_victim = findProbationVictim();
        shrinkPrefixInProbation(probation_victim, 1);
      }
      protected_victim = findProtectedVictim();
      shrinkPrefixInProtected(protected_victim, 1);
      extendPrefixInProtected(prefix, 1);
    } while (
      !(
        prefix.isFull() ||
          probation_victim != null && probation_victim.itemKey == prefix.itemKey
          || protected_victim.itemKey == prefix.itemKey
      )
    );
  }

  private void extendPrefixInProbation(Prefix prefix, long chunks) {
    if (prefix.chunksAmount + chunks > prefix.fullItemChunksAmount)
      throw new IllegalArgumentException("This amount of chunks will make the prefix exceed the full item size");
    if (prefix.chunksAmount + chunks > maxProbationSize)
      throw new IllegalArgumentException("This amount of chunks will make the probation segment exceed its maximum size");

    prefix.chunksAmount += chunks;
    prefix.chunksInProbation += chunks;
    currentProbationSize += chunks;
    probationHeap.upsert(prefix.itemKey, prefix);
  }

  private void extendPrefixInProtected(Prefix prefix, long chunks) {
    if (prefix.chunksAmount + chunks > prefix.fullItemChunksAmount)
      throw new IllegalArgumentException("This amount of chunks will make the prefix exceed the full item size");
    if (prefix.chunksAmount + chunks > maxProtectedSize)
      throw new IllegalArgumentException("This amount of chunks will make the protected segment exceed its maximum size");

    prefix.chunksAmount += chunks;
    prefix.chunksInProtected += chunks;
    currentProtectedSize += chunks;
    protectedHeap.upsert(prefix.itemKey, prefix);
  }

  private void shrinkPrefixInProbation(Prefix prefix, long chunks) {
    if (prefix.chunksAmount - chunks < 0)
      throw new IllegalArgumentException("This amount of chunks will make the prefix have negative size");
    if (currentProbationSize - chunks < 0)
      throw new IllegalArgumentException("This amount of chunks will make the probation segment have negative size");

    prefix.chunksAmount -= chunks;
    prefix.chunksInProbation -= chunks;
    currentProbationSize -= chunks;

    probationHeap.upsert(prefix.itemKey, prefix);
  }

  private void shrinkPrefixInProtected(Prefix prefix, long chunks) {
    if (prefix.chunksAmount - chunks < 0)
      throw new IllegalArgumentException("This amount of chunks will make the prefix have negative size");
    if (currentProtectedSize - chunks < 0)
      throw new IllegalArgumentException("This amount of chunks will make the protected segment have negative size");

    prefix.chunksAmount -= chunks;
    prefix.chunksInProtected -= chunks;
    currentProtectedSize -= chunks;
    protectedHeap.upsert(prefix.itemKey, prefix);
  }

  private Prefix findProtectedVictim() {
    if (!protectedHeap.isEmpty())
      return protectedHeap.min().value();
    throw new IllegalStateException("No victim can be found - protected heap is empty");
  }

  private Prefix findProbationVictim() {
    if (!probationHeap.isEmpty())
      return probationHeap.min().value();
    throw new IllegalStateException("No victim can be found - probation heap is empty");
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

  static class Prefix {
    final long itemKey;
    final long fullItemChunksAmount;
    final Source source;
    long chunksAmount, chunksInProbation, chunksInProtected;
    long requestsCountInPeriod;
    long lastRequestTime;

    Prefix(long itemKey, long fullItemChunksAmount, Source source, long currentTime) {
      this.itemKey = itemKey;
      this.fullItemChunksAmount = fullItemChunksAmount;
      this.source = source;
      this.lastRequestTime = currentTime;
      this.requestsCountInPeriod = 0;
      this.chunksAmount = 0;
      this.chunksInProbation = 0;
      this.chunksInProtected = 0;
    }

    double lruScore() {
      double prefixTxTime = TimeCalculations.calculateTransmissionTime(sizeInMB(), Consts.BANDWIDTH);
      return recency() * (1 - source.calculateCDF(prefixTxTime));
    }

    double recency() {
      return 1.0 / (G2NBSegmentedLruPolicy.currentTime - lastRequestTime + 1);
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
