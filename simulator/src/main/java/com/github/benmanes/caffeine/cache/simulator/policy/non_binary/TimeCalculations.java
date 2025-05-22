package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

public final class TimeCalculations {
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
  public static double calculateUnderflowDelay(double sourceDelay, double itemSize, double prefixSize, long bandwidth) {
    if (itemSize == prefixSize) {
      // If the whole item is cached, there's no delay whatsoever.
      // Even if the source delay is very big, if the whole item is cached, there will not be a request to source, therefore no delay.
      return 0;
    }
    return Math.max(0, sourceDelay - calculateTransmissionTime(prefixSize, bandwidth));
  }

  public static double calculateTransmissionTime(double size, long bandwidth) {
    return size / bandwidth;
  }
}
