package com.github.benmanes.caffeine.cache.simulator.policy.prefix;

import com.github.benmanes.caffeine.cache.simulator.policy.prefix.sources.NormalSource;

import java.util.ArrayList;

public final class Consts {
  public static final long BANDWIDTH = 125_000_000; // in Bps
  public static final ArrayList<NormalSource> SOURCES = new ArrayList<>();
  static {
    SOURCES.add(new NormalSource(0, 0.4793, 0.013));
    SOURCES.add(new NormalSource(1, 0.3455, 0.013));
    SOURCES.add(new NormalSource(2, 0.6863, 0.013));
    SOURCES.add(new NormalSource(3, 0.8039, 0.013));
    SOURCES.add(new NormalSource(4, 0.1283, 0.013));
    SOURCES.add(new NormalSource(5, 0.1793, 0.013));
  }
}
