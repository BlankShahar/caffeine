package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.NormalSource;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;

import java.util.ArrayList;
import java.util.Arrays;

public final class Consts {
  public static final long ITEM_CHUNKS_AMOUNT = 1024;
  public static final double CHUNK_SIZE = 0.001; // in MB (1 KB)
  public static final long BANDWIDTH = 1000; // in MBps
  public static final long REQUESTS_FREQUENCY_PERIOD = Integer.MAX_VALUE - 1;

  public static final int SOURCE_PICKER_SEED = 1234;

  public static final ArrayList<Source> APPROXIMATED_SOURCES = new ArrayList<>(
    Arrays.asList(
      new NormalSource(0.2, 0.05, 1),
      new NormalSource(0.3, 0.07, 2),
      new NormalSource(0.5, 0.1, 3)
    )
  );

  public static final ArrayList<Source> REAL_SOURCES = new ArrayList<>(
    Arrays.asList(
      new NormalSource(0.22, 0.06, 1),   // Slightly higher mean and std deviation
      new NormalSource(0.28, 0.08, 2),   // Slightly lower mean and higher std deviation
      new NormalSource(0.52, 0.09, 3)    // Slightly higher mean, slightly lower std deviation
    )
  );

}
