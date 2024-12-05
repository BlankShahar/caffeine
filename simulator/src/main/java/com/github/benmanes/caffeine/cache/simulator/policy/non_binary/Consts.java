package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.NormalSource;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;

import java.util.ArrayList;
import java.util.Arrays;

public final class Consts {
  public static final long ITEM_CHUNKS_AMOUNT = 1024;
  public static final double CHUNK_SIZE = 0.004; // in MB (4 KB)
  public static final long BANDWIDTH = 1000; // in MBps
  public static final long REQUESTS_FREQUENCY_PERIOD = 10_000;

  public static final int SOURCE_PICKER_SEED = 1234;


  public static final ArrayList<Source> SOURCES = new ArrayList<>(
    Arrays.asList(
      new NormalSource(0.2, 0.05, 1),
      new NormalSource(0.3, 0.07, 2),
      new NormalSource(0.5, 0.1, 3)
    )
  );

}
