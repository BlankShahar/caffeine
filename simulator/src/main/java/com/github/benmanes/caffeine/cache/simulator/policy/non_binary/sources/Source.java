package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources;

public interface Source {
  double sampleProcessingTime();
  double calculateCDF(double time);
}
