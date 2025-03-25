package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;

public class Prefix {
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

  public double lruScore() {
    // Idea - recency times the probability of not experiencing delay
    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB(),
      Consts.BANDWIDTH
    );
    return recency(LruPolicy.currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double lruScoreAfterInsertion() {
    if (isFull()) {
      return 0; // 1-CDF value is 0
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB() + Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return recency(LruPolicy.currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double lruScoreAfterEviction() {
    if (isEmpty()) {
      return recency(LruPolicy.currentTime); // 1-CDF is 1
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB() - Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return recency(LruPolicy.currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double convexLrfuScore(double alpha, double maxFrequency, double maxRecency) {
    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB(),
      Consts.BANDWIDTH
    );
    return alpha * recency(ConvexLrfuPolicy.currentTime) / maxRecency * (1 - source.calculateCDF(prefixTransmissionTime)) +
      (1 - alpha) * frequency() / maxFrequency * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double convexLrfuScoreAfterInsertion(double alpha, double maxFrequency, double maxRecency) {
    if (isFull()) {
      return 0; // 1-CDF value is 0
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB() + Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return alpha * recency(ConvexLrfuPolicy.currentTime) / maxRecency * (1 - source.calculateCDF(prefixTransmissionTime)) +
      (1 - alpha) * frequency() / maxFrequency * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double convexLrfuScoreAfterEviction(double alpha, double maxFrequency, double maxRecency) {
    if (isEmpty()) { // 1-CDF is 1
      return alpha * recency(ConvexLrfuPolicy.currentTime) / maxRecency + (1 - alpha) * frequency() / maxFrequency;
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB() - Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return alpha * recency(ConvexLrfuPolicy.currentTime) / maxRecency * (1 - source.calculateCDF(prefixTransmissionTime)) +
      (1 - alpha) * frequency() / maxFrequency * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double hyperbolicScore() {
    // Idea - frequency times recency times the probability of not experiencing delay
    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB(),
      Consts.BANDWIDTH
    );
    return frequency() * recency(HyperbolicPolicy.currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double hyperbolicScoreAfterInsertion() {
    if (isFull()) {
      return 0; // 1-CDF value is 0
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB() + Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return frequency() * recency(HyperbolicPolicy.currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double hyperbolicScoreAfterEviction() {
    if (isEmpty()) {
      return frequency() * recency(HyperbolicPolicy.currentTime); // 1-CDF value is 1
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      sizeInMB() - Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return frequency() * recency(HyperbolicPolicy.currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double pipelineFirstCacheScore() {
    // Idea - frequency times recency times the probability of not experiencing delay
    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      firstCacheChunksAmount * Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return recency(PipelineLrfuPolicy.currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double pipelineFirstCacheScoreAfterInsertion() {
    if (firstCacheChunksAmount == fullItemChunksAmount) {
      return 0; // 1-CDF value is 0
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      (firstCacheChunksAmount + 1) * Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return recency(PipelineLrfuPolicy.currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
  }

  public double pipelineFirstCacheScoreAfterEviction() {
    if (firstCacheChunksAmount == 0) {
      return recency(PipelineLrfuPolicy.currentTime); // 1-CDF value is 1
    }

    double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
      (firstCacheChunksAmount - 1) * Consts.CHUNK_SIZE,
      Consts.BANDWIDTH
    );
    return recency(PipelineLrfuPolicy.currentTime) * (1 - source.calculateCDF(prefixTransmissionTime));
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

  public double recency(long currentTime) {
    return (double) 1 / (currentTime - lastRequestTime + 1);
  }

  public void insertChunk() {
    if (!isFull()) {
      chunksAmount++;
    }
  }

  public void removeChunk() {
    if (chunksAmount > 0) {
      chunksAmount--;
    }
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

  public int lruCompareTo(Prefix other) {
    return Double.compare(this.lruScore(), other.lruScore());
  }

  public int lfuCompareTo(Prefix other) {
    return Double.compare(this.lfuScore(), other.lfuScore());
  }

  public int convexLrfuCompareTo(Prefix other) {
    return Double.compare(
      this.convexLrfuScore(ConvexLrfuPolicy.alpha, ConvexLrfuPolicy.maxRecency, ConvexLrfuPolicy.maxFrequency),
      other.convexLrfuScore(ConvexLrfuPolicy.alpha, ConvexLrfuPolicy.maxRecency, ConvexLrfuPolicy.maxFrequency)
    );
  }

  public int hyperbolicCompareTo(Prefix other) {
    return Double.compare(this.hyperbolicScore(), other.hyperbolicScore());
  }

  public int pipelineFirstCacheCompareTo(Prefix other) {
    return Double.compare(this.pipelineFirstCacheScore(), other.pipelineFirstCacheScore());
  }

  public int pipelineSecondCacheCompareTo(Prefix other) {
    return Double.compare(this.pipelineSecondCacheScore(), other.pipelineSecondCacheScore());
  }
}
