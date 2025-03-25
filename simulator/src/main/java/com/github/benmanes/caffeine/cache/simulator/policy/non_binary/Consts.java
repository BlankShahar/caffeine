package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

public final class Consts {
  public static final long ITEM_CHUNKS_AMOUNT = 1024;
  public static final double CHUNK_SIZE = 0.004; // in MB (4 KB)
  public static final long BANDWIDTH = 1000; // in MBps
  public static final long REQUESTS_FREQUENCY_PERIOD = 10_000;
  public static final int SOURCE_KEY = 1;
  public static final double SOURCE_MEAN = 0.003;
  public static final double SOURCE_STD = 0.00075;
}
