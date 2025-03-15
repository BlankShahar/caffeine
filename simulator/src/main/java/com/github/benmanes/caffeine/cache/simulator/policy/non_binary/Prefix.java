package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;

public class Prefix {
  final long itemKey, fullItemChunksAmount;
  final Source source;
  long chunksAmount;
  long requestsCountInPeriod;
  long lastRequestTime;

  public Prefix(long itemKey, long fullItemChunksAmount, Source source, long currentTime) {
    this.itemKey = itemKey;
    this.fullItemChunksAmount = fullItemChunksAmount;
    this.source = source;
    this.requestsCountInPeriod = 0;
    this.chunksAmount = 0;
    this.lastRequestTime = currentTime;
  }

  public double lfu_score() {
    // Idea - frequency times the probability of not experiencing delay
    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB(),
      Consts.BANDWIDTH
    );
    return frequency() * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double lfu_score_after_insertion() {
    if (isFull()) {
      return frequency(); // CDF value is 1
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB() + Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return frequency() * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double lfu_score_after_eviction() {
    if (chunksAmount == 0) {
      return 0;
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB() - Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return frequency() * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double lru_score() {
    // Idea - recency times the probability of not experiencing delay
    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB(),
      Consts.BANDWIDTH
    );
    return recency(LruPrefixPolicy.currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double lru_score_after_insertion() {
    if (isFull()) {
      return recency(LruPrefixPolicy.currentTime); // CDF value is 1
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB() + Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return recency(LruPrefixPolicy.currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double lru_score_after_eviction() {
    if (chunksAmount == 0) {
      return 0;
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB() - Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return recency(LruPrefixPolicy.currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double frequency() {
    return (double) requestsCountInPeriod / Consts.REQUESTS_FREQUENCY_PERIOD;
  }

  public double recency(long currentTime) {
    return (double) 1 / (currentTime - lastRequestTime + 1);
  }

  public void insertChunk() {
    chunksAmount++;
  }

  public void removeChunk() {
    if (chunksAmount > 0) {
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

  public int LruCompareTo(Prefix other) {
    return Double.compare(this.lru_score(), other.lru_score());
  }

  public int LfuCompareTo(Prefix other) {
    return Double.compare(this.lfu_score(), other.lfu_score());
  }
}
