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

@Policy.PolicySpec(name = "non-binary.SegmentedLRU")
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
  Source source;

  public NBSegmentedLruPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.data = new Long2ObjectOpenHashMap<>();
    this.requests = new ArrayDeque<>();
    currentTime = 0;

    this.maximumCacheSize = settings.maximumSize();
    this.maxProtectedSize = (long) (maximumCacheSize * 0.8);
    this.maxProbationSize = maximumCacheSize - maxProtectedSize;
    this.currentProbationSize = 0;
    this.currentProtectedSize = 0;

    this.probationHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::compareProbation);
    this.protectedHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::compareProtected);
    this.source = new NormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);
  }

  @Override
  public void record(AccessEvent event) {
    long itemKey = event.key();
    var existingPrefix = data.getOrDefault(itemKey, null);
    policyStats.recordOperation();
    currentTime++;

    if (existingPrefix != null) {
      existingPrefix.lastRequestTime = currentTime;
      onRequest(existingPrefix, event.retrievalDelay());
    } else {
      var newPrefix = new Prefix(itemKey, event.itemSize(), source, currentTime);
      onRequest(newPrefix, event.retrievalDelay());
    }
  }

  private void onRequest(Prefix prefix, double sourceDelay) {
    recordRequestStatistics(prefix, sourceDelay);
    handleRequestsFrequency(prefix);

    if (!data.containsKey(prefix.itemKey)) {
      data.put(prefix.itemKey, prefix);
      prefix.isInProtected = false;
    } else if (!prefix.isInProtected) {
      promoteToProtected(prefix);
    }

    waterFill(prefix);
  }

  private void promoteToProtected(Prefix prefix) {
    if (prefix.chunksAmount > maxProtectedSize) { // too large to fit
      return;
    }
    while (prefix.chunksAmount + currentProtectedSize > maxProtectedSize && !protectedHeap.isEmpty()) {
      Prefix demote = protectedHeap.extractMin().value();
      demote.isInProtected = false;
      currentProtectedSize -= demote.chunksAmount;
      currentProbationSize += demote.chunksAmount;
      probationHeap.insert(demote.itemKey, demote);
    }

    prefix.isInProtected = true;
    if (probationHeap.contains(prefix.itemKey)) probationHeap.remove(prefix.itemKey);
    protectedHeap.insert(prefix.itemKey, prefix);
    currentProbationSize -= prefix.chunksAmount;
    currentProtectedSize += prefix.chunksAmount;
  }

  private void handleRequestsFrequency(Prefix prefix) {
    prefix.requestsCountInPeriod++;
    requests.add(prefix.itemKey);
    if (requests.size() == Consts.REQUESTS_FREQUENCY_PERIOD + 1) {
      long lastRequestItemKey = requests.remove();
      var lastPrefix = data.getOrDefault(lastRequestItemKey, null);
      if (lastPrefix != null) {
        lastPrefix.requestsCountInPeriod--;
      }
    }
  }

  private void recordRequestStatistics(Prefix prefix, double sourceDelay) {
    double idealSize = Math.min(prefix.fullItemSizeInMB(), sourceDelay * Consts.BANDWIDTH);
    long idealChunks = (long) Math.ceil(idealSize / Consts.CHUNK_SIZE);
    policyStats.addHits(prefix.chunksAmount);
    policyStats.addMisses(Math.max(0, idealChunks - prefix.chunksAmount));
    double delay = TimeCalculations.calculateUnderflowDelay(sourceDelay, prefix.fullItemSizeInMB(), prefix.sizeInMB(), Consts.BANDWIDTH);
    double latency = TimeCalculations.calculateNonBinaryLatency(sourceDelay, prefix.fullItemSizeInMB(), prefix.sizeInMB(), Consts.BANDWIDTH);
    policyStats.addDelay(delay);
    policyStats.addLatency(latency);
  }

  private void waterFill(Prefix prefix) {
    while (!prefix.isFull() && (currentProbationSize + currentProtectedSize) < maximumCacheSize) {
      extendPrefix(prefix);
    }
    while (true) {
      Prefix victim = findVictim();
      double sPlus = prefix.lruScoreAfterInsertion();
      double sMinus = victim.lruScoreAfterEviction();
      if (prefix.isFull() || victim.itemKey == prefix.itemKey || sPlus < sMinus) break;
      shrinkPrefix(victim);
      extendPrefix(prefix);
    }
  }

  private void shrinkPrefix(Prefix prefix) {
    if (prefix.isEmpty()) return;
    prefix.removeChunk();
    if (prefix.isInProtected) {
      if (protectedHeap.contains(prefix.itemKey)) protectedHeap.remove(prefix.itemKey);
      currentProtectedSize--;
      if (prefix.chunksAmount > 0) protectedHeap.insert(prefix.itemKey, prefix);
    } else {
      if (probationHeap.contains(prefix.itemKey)) probationHeap.remove(prefix.itemKey);
      currentProbationSize--;
      if (prefix.chunksAmount > 0) probationHeap.insert(prefix.itemKey, prefix);
    }
    policyStats.recordOperation();
    policyStats.recordEviction();
  }

  private void extendPrefix(Prefix prefix) {
    if (prefix.isFull()) return;
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

  private Prefix findVictim() {
    if (!probationHeap.isEmpty()) return probationHeap.min().value();
    return protectedHeap.min().value();
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
    final long itemKey, fullItemChunksAmount;
    final Source source;
    long chunksAmount;
    long requestsCountInPeriod;
    long lastRequestTime;
    boolean isInProtected;

    public Prefix(long itemKey, long fullItemChunksAmount, Source source, long currentTime) {
      this.itemKey = itemKey;
      this.fullItemChunksAmount = fullItemChunksAmount;
      this.source = source;
      this.lastRequestTime = currentTime;
      this.requestsCountInPeriod = 0;
      this.chunksAmount = 0;
      this.isInProtected = false;
    }

    public double lruScore() {
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(sizeInMB(), Consts.BANDWIDTH);
      return recency(currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double lruScoreAfterInsertion() {
      if (isFull()) return 0;
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(sizeInMB() + Consts.CHUNK_SIZE, Consts.BANDWIDTH);
      return recency(currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double lruScoreAfterEviction() {
      if (isEmpty()) return recency(currentTime);
      double prefixTime = TimeCalculations.calculateTransmissionTime(sizeInMB() - Consts.CHUNK_SIZE, Consts.BANDWIDTH);
      return recency(currentTime) * (1 - source.calculateCDF(prefixTime));
    }

    public double recency(long now) {
      return (double) 1 / (now - lastRequestTime + 1);
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

    public void insertChunk() {
      if (!isFull()) chunksAmount++;
    }

    public void removeChunk() {
      if (chunksAmount > 0) chunksAmount--;
    }

    public int lruCompareTo(Prefix other) {
      return Double.compare(this.lruScore(), other.lruScore());
    }
  }
}
