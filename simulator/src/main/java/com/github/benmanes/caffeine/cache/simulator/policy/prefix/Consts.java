package com.github.benmanes.caffeine.cache.simulator.policy.prefix;

import com.github.benmanes.caffeine.cache.simulator.policy.prefix.sources.NormalSource;

import java.util.ArrayList;

public final class Consts {
  public static final long BANDWIDTH = 125_000_000; // in Bps
  public static final ArrayList<NormalSource> SOURCES = new ArrayList<>();

  // minimum observations before we start using an item's (mean,std) to update clusters
  public static final int MIN_OBS_FOR_CLUSTER = 5;
  // safety floor for std to avoid zero-variance artifacts
  public static final double EPS_STD = 1e-9;
  // fallback latency (seconds) before any cluster exists
  public static final double DEFAULT_LATENCY_S = 0.2;

  static {
    SOURCES.add(new NormalSource(0, 0.675, 0.01431));
  }
//  static {
//    SOURCES.add(new NormalSource(0, 0.4793, 0.013));
//    SOURCES.add(new NormalSource(1, 0.3455, 0.013));
//    SOURCES.add(new NormalSource(2, 0.6863, 0.013));
//    SOURCES.add(new NormalSource(3, 0.8039, 0.013));
//    SOURCES.add(new NormalSource(4, 0.1283, 0.013));
//    SOURCES.add(new NormalSource(5, 0.1793, 0.013));
//  }
}
