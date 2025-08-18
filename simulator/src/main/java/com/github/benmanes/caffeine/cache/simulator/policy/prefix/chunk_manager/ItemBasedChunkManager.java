package com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager;

import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;

import java.util.ArrayList;
import java.util.HashMap;

public class ItemBasedChunkManager implements ChunkManager {
  private final HashMap<Long, ArrayList<Double>> itemToDelays;
  private double averageDelay, averageStd, m2;
  private int requestsCount;

  public ItemBasedChunkManager() {
    this.itemToDelays = new HashMap<>();
    this.requestsCount = 0;
    this.averageDelay = 0.0;
    this.averageStd = 0.0;
    this.m2 = 0.0;
  }

  @Override
  public long getChunkSize(long itemKey, long itemSize) {
    var delays = itemToDelays.get(itemKey);
    if (delays == null || delays.isEmpty()) {
      long chunkSize = Math.round((averageDelay + 2 * averageStd) * Consts.BANDWIDTH);
      return Math.min(chunkSize, itemSize);
    }
    double mean = delays.stream().mapToDouble(Double::doubleValue).average().orElse(averageDelay);
    double denom = Math.max(1, delays.size() - 1); // sample variance
    double var = delays.stream().mapToDouble(d -> {
      double x = d - mean;
      return x * x;
    }).sum() / denom;
    long chunk = Math.round((mean + 2 * Math.sqrt(var)) * Consts.BANDWIDTH);

    chunk = Math.min(chunk, itemSize);
    return Math.max(1L, chunk);
  }


  public void addDelay(long itemKey, double delay) {
    itemToDelays.computeIfAbsent(itemKey, k -> new ArrayList<>()).add(delay);

    int nPrev = requestsCount;
    requestsCount = nPrev + 1;

    // Welford's online mean/variance
    double delta = delay - averageDelay;
    averageDelay += delta / requestsCount;
    double delta2 = delay - averageDelay;
    m2 += delta * delta2;

    // sample std (use /requestsCount for population std)
    averageStd = (requestsCount > 1) ? Math.sqrt(m2 / (requestsCount - 1)) : 0.0;
  }

}
