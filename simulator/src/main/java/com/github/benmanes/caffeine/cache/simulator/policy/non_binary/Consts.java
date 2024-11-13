package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

public final class Consts {
  public static final long ITEM_CHUNKS_AMOUNT = 1024;
  public static final double CHUNK_SIZE = 0.001; // in MB (1 KB)
  public static final long BANDWIDTH = 1250; // in MBps

  public static final double MEAN_PROCESSING_TIME = 0.2; // average delay in seconds (e.g., 200 ms)
  public static final double STANDARD_DEVIATION_PROCESSING_TIME = 0.05; // standard deviation in seconds (e.g., 50 ms)

  public static final int REAL_SEED = 1337, APPROXIMATED_SEED = 1234;
}
