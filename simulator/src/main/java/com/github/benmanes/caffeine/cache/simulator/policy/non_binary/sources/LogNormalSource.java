package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources;

public class LogNormalSource extends NormalSource {
  public LogNormalSource(long id, double mean, double standardDeviation) {
    super(id, mean, standardDeviation);
  }

  @Override
  public double calculateCDF(double time) {
    time = Math.log(time);
    if (resultsCache.containsKey(time)) {
      return resultsCache.get(time);
    }
    double x = (time - mean) / standardDeviation;
    double roundedX = Math.round(x * 100) / 100.0;
    if (roundedX < -3.99) {
      return 0;
    } else if (roundedX > 3.99) {
      return 1;
    }
    double result = phi(roundedX);
    resultsCache.put(time, result);
    return result;
  }

}
