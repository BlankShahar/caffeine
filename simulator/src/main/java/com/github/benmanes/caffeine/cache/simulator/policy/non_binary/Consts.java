package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.NormalSource;

import java.util.ArrayList;

public final class Consts {
  public static final long CHUNK_SIZE = 1; // in B
  public static final long BANDWIDTH = 125_000_000; // in Bps
  public static final long REQUESTS_FREQUENCY_PERIOD = 1_000_000;
  public static final int SOURCE_KEY = 1;
  public static final double SOURCE_MEAN = 0.675;
  public static final double SOURCE_STD = 0.17;

  public static final ArrayList<NormalSource> SOURCES = new ArrayList<>();
  static {
    SOURCES.add(new NormalSource(0, 0.675, 0.01431));
    SOURCES.add(new NormalSource(1, 0.12, 0.0608));
    SOURCES.add(new NormalSource(2, 0.5132, 0.0608));
    SOURCES.add(new NormalSource(3, 0.3384, 0.0608));
    SOURCES.add(new NormalSource(4, 0.1583, 0.0608));
    SOURCES.add(new NormalSource(5, 0.847, 0.0608));
  }
}
