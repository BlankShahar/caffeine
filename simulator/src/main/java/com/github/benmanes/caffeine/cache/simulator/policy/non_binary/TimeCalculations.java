package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

public final class TimeCalculations {
  public static double calculateSourceLatency(double sourceDelay, double itemSize, long bandwidth) {
    double transmissionTime = 2 * calculateTransmissionTime(itemSize, bandwidth);
    return sourceDelay + transmissionTime;
  }

  public static double calculateNonBinaryLatency(double sourceDelay, double itemSize, double prefixSize, long bandwidth) {
    double prefixTransmissionTime = calculateTransmissionTime(prefixSize, bandwidth);
    double restTransmissionTime = 2 * calculateTransmissionTime(itemSize - prefixSize, bandwidth);
    double delay = calculateDelay(sourceDelay, itemSize, prefixSize, bandwidth);
    return prefixTransmissionTime + Math.max(0, delay) + restTransmissionTime;
  }

  /**
   * Calculate the delay of fetching a partial cached object.
   * If the whole object is cached - the delay is 0 (due to not requesting the source).
   * If the delay is positive, there's "underflow" - the object is fetched faster than the source delay.
   * If the delay is negative, there's "overflow" - the object is fetched slower than the source delay.
   *
   * @param sourceDelay in seconds
   * @param itemSize    in MB
   * @param prefixSize  in MB
   * @param bandwidth   in MBps
   * @return the delay in seconds
   */
  public static double calculateDelay(double sourceDelay, double itemSize, double prefixSize, long bandwidth) {
    if (itemSize == prefixSize) {
      // If the whole item is cached, there's no delay whatsoever.
      // Even if the source delay is very big, if the whole item is cached, there will not be a request to source, therefore no delay.
      return 0;
    }
    return sourceDelay - calculateTransmissionTime(prefixSize, bandwidth);
  }

  public static double calculateTransmissionTime(double size, long bandwidth) {
    return size / bandwidth;
  }

  public static HashMap<Source, Double> getNextProcessingTimes(List<Source> sources) {
    HashMap<Source, Double> nextProcessingTimes = new HashMap<>();
    for (Source source : sources) {
      nextProcessingTimes.put(source, source.getNextProcessingTime());
    }
    return nextProcessingTimes;
  }
}
