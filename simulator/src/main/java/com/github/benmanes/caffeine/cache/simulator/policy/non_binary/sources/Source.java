package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources;

public abstract class Source {
  public final long id;

  protected Source(long id) {
    this.id = id;
  }

  abstract public double calculateCDF(double time);
  abstract public double sampleProcessingTime();
}
