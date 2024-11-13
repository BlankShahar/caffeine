package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources;

import java.util.Random;

public class NormalSource implements Source {
  private final Random random;
  private final double mean, standardDeviation; // in ms

  public NormalSource(double mean, double standardDeviation, long key) {
    this.mean = mean;
    this.standardDeviation = standardDeviation;
    this.random = new Random(key);
  }

  @Override
  public double getNextProcessingTime() {
    return mean + standardDeviation * random.nextGaussian();
  }
}
