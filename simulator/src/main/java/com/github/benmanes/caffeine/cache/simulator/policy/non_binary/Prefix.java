package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;

public class Prefix implements Comparable<Prefix> {
  final long itemKey, fullItemChunksAmount;
  long chunksAmount;
  long requestsCountInPeriod;
  Source source;

  public Prefix(long itemKey, long fullItemChunksAmount, Source source) {
    this.itemKey = itemKey;
    this.fullItemChunksAmount = fullItemChunksAmount;
    this.source = source;
    this.requestsCountInPeriod = 0;
    this.chunksAmount = 0;
  }

  public double lfu_score_after_insertion() {
    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB() + Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return frequency() * source.calculateCDF(prefixTransmissionTime);
  }

  public double lfu_score_after_eviction() {
    if (chunksAmount == 0) {
      return 0;
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB() - Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return frequency() * source.calculateCDF(prefixTransmissionTime);
  }

  public double lfu_score() {
    // Idea - frequency times the probability of experiencing delay without the last chunk
    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB(),
      Consts.BANDWIDTH
    );
    return frequency() * source.calculateCDF(prefixTransmissionTime);
  }

  public double frequency() {
    return (double) requestsCountInPeriod / Consts.REQUESTS_FREQUENCY_PERIOD;
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

  @Override
  public int compareTo(Prefix other) {
    return (int) Math.round(this.lfu_score() - other.lfu_score());
  }
}
