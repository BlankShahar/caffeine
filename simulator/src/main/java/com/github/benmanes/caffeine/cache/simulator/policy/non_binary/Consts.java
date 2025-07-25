package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

public final class Consts {
  public static final long CHUNK_SIZE = 750_000; // in B
  public static final long BANDWIDTH = 125_000_000; // in Bps
  public static final long REQUESTS_FREQUENCY_PERIOD = 1_000_000;
  public static final int SOURCE_KEY = 1;
  public static final double SOURCE_MEAN = -2.15115527898;
  public static final double SOURCE_STD = 0.1022;
}
