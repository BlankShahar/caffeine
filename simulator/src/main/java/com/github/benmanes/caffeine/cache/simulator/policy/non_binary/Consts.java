package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

public final class Consts {
  public static final double CHUNK_SIZE = 2; // in MB
  public static final long BANDWIDTH = 100; // in MBps
  public static final long REQUESTS_FREQUENCY_PERIOD = 10_000;
  public static final int SOURCE_KEY = 1;
  public static final double SOURCE_MEAN = 0.04;
  public static final double SOURCE_STD = 0.0004;
}
