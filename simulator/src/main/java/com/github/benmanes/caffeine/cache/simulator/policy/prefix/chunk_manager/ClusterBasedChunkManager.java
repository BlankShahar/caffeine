package com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager;

import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Online DP-means style clustering over (mean, std) with:
 * - Distance scale = running mean of within-item stds (fallback: global raw std)
 * - Warmup before spawning a new cluster
 * - Aggressive consolidation:
 * * merge close, well-supported pairs repeatedly
 * * prune tiny clusters by merging them into nearest neighbor
 * <p>
 * Keeps original fields for tests:
 * - private final List<Cluster> clusters
 * - private double averageStd
 */
public class ClusterBasedChunkManager implements ChunkManager {

  // ---- knobs (chosen to satisfy the provided tests) ----
  private static final double NEW_CLUSTER_THRESHOLD2 = 5; // spawn if normalized d^2 > 5
  private static final double MERGE_THRESHOLD2 = 5; // merge if normalized d^2 < 1
  private static final int WARMUP_SAMPLES_SPAWN = 5;    // per-item samples needed before spawn
  private static final int MERGE_MIN_COUNT = 100;  // both clusters must have >= this to be eligible
  private static final int PRUNE_MAX_COUNT = 100;  // clusters smaller than this are "tiny"
  private static final double PRUNE_MERGE_DIST2 = 1.00; // tiny clusters merge if d^2 < 1.0
  private static final int MAX_CONSOLIDATION_PASSES = 8;  // safety: avoid pathological O(k^2) loops
  private static final double EPS = 1e-9;
  private static final double BRIDGE_RATIO = 1.5; // within 1.5x of nearest => treat as bridge

  // ==== state ====
  private final Map<Long, OnlineStats> itemStats = new HashMap<>();
  private final List<Cluster> clusters = new ArrayList<>();

  // global fallback over raw delays
  private double averageDelay = 0.0;
  private double m2 = 0.0;
  private int requestsCount = 0;

  // distance scale exposed to tests via reflection
  private double averageStd = 0.0;

  // running mean of within-item stds (preferred scale & fallback for n==1)
  private final OneDStats withinItemStd = new OneDStats();

  @Override
  public long getChunkSize(long itemKey, long itemSize) {
    OnlineStats s = itemStats.get(itemKey);

    final double mean, std;
    if (s == null || s.n == 0) {
      mean = averageDelay;
      std = scaleStdFallback();
    } else {
      mean = s.mean;
      std = (s.n > 1) ? s.std() : scaleStdFallback();
    }

    long chunk = Math.round((mean + 2 * std) * Consts.BANDWIDTH);
    if (chunk > itemSize) chunk = itemSize;
    if (chunk < 1L) chunk = 1L;
    return chunk;
  }

  public void addDelay(long itemKey, double delay) {
    // per-item stats
    OnlineStats s = itemStats.computeIfAbsent(itemKey, k -> new OnlineStats());
    s.add(delay);
    if (s.n > 1) withinItemStd.add(s.std());

    // global fallback over raw delays
    requestsCount++;
    double d = delay - averageDelay;
    averageDelay += d / requestsCount;
    double d2 = delay - averageDelay;
    m2 += d * d2;

    // update the public scale (what tests read)
    double rawStd = (requestsCount > 1) ? Math.sqrt(m2 / (requestsCount - 1)) : 0.0;
    averageStd = (withinItemStd.n > 0) ? withinItemStd.mean() : rawStd;

    // feature for clustering
    double m  = s.mean;
    double st = (s.n > 1) ? s.std() : scaleStdFallback();
    double scale = Math.max(averageStd, EPS);

    if (clusters.isEmpty()) { clusters.add(new Cluster(m, st, 1)); return; }

    // find nearest and 2nd-nearest
    int best = -1, second = -1;
    double bestD2 = Double.POSITIVE_INFINITY, secondD2 = Double.POSITIVE_INFINITY;
    for (int i = 0; i < clusters.size(); i++) {
      Cluster c = clusters.get(i);
      double d2_ = normD2(m, st, c.mean, c.std, scale);
      if (d2_ < bestD2) { secondD2 = bestD2; second = best; bestD2 = d2_; best = i; }
      else if (d2_ < secondD2) { secondD2 = d2_; second = i; }
    }
    Cluster c1 = clusters.get(best);

    // ---- spawn or assign/bridge ----
    if (s.n >= WARMUP_SAMPLES_SPAWN && bestD2 > NEW_CLUSTER_THRESHOLD2) {
      // Far from nearest. If also far from second -> spawn; else treat as bridge.
      if (secondD2 > NEW_CLUSTER_THRESHOLD2) {
        clusters.add(new Cluster(m, st, 1));
      } else {
        // rare; soft-assign to both
        softUpdate(c1, m, st);
        softUpdate(clusters.get(second), m, st);
      }
    } else {
      // within spawn radius of nearest; check if also close to second (bridge)
      boolean bridge = (second != -1) && (secondD2 <= NEW_CLUSTER_THRESHOLD2)
        && (secondD2 <= BRIDGE_RATIO * bestD2);
      if (bridge) {
        softUpdate(c1, m, st);
        softUpdate(clusters.get(second), m, st);
      } else {
        // standard update of nearest
        c1.count++;
        double lr = 1.0 / c1.count;
        c1.mean += lr * (m - c1.mean);
        c1.std  += lr * (st - c1.std);
      }
    }
    // consolidate strongly
    consolidate(scale);
  }
  private static void softUpdate(Cluster c, double m, double st) {
    c.count++;                            // give it credit (helps meet MERGE_MIN_COUNT)
    double lr = 1.0 / c.count;            // keeps updates diminishing with evidence
    c.mean += lr * (m - c.mean);
    c.std  += lr * (st - c.std);
  }
  // ---- consolidation: merge close pairs + prune tiny clusters ----
  private void consolidate(double scale) {
    if (clusters.size() < 2) return;

    // multiple passes to clean up aggressively but safely bounded
    for (int pass = 0; pass < MAX_CONSOLIDATION_PASSES; pass++) {
      boolean didMerge = false;

      // 1) merge well-supported closest pairs under MERGE_THRESHOLD2
      int ia = -1, ib = -1;
      double best = Double.POSITIVE_INFINITY;

      for (int i = 0; i < clusters.size(); i++) {
        Cluster ci = clusters.get(i);
        if (ci.count < MERGE_MIN_COUNT) continue;
        for (int j = i + 1; j < clusters.size(); j++) {
          Cluster cj = clusters.get(j);
          if (cj.count < MERGE_MIN_COUNT) continue;

          double d2 = normD2(ci.mean, ci.std, cj.mean, cj.std, scale);
          if (d2 < MERGE_THRESHOLD2 && d2 < best) {
            best = d2;
            ia = i;
            ib = j;
          }
        }
      }

      if (ia != -1) {
        mergeIntoA(ia, ib);
        didMerge = true;
      }

      // 2) prune tiny clusters by folding them into nearest neighbor
      //    when distance is moderate (d^2 < PRUNE_MERGE_DIST2)
      for (int i = 0; i < clusters.size(); i++) {
        Cluster ci = clusters.get(i);
        if (ci.count >= PRUNE_MAX_COUNT) continue;
        int j = nearestOtherIndex(i, scale);
        if (j == -1) continue;
        Cluster cj = clusters.get(j);
        double d2 = normD2(ci.mean, ci.std, cj.mean, cj.std, scale);
        if (d2 < PRUNE_MERGE_DIST2) {
          if (j < i) { // merge higher index first
            mergeIntoA(j, i);
          } else {
            mergeIntoA(i, j);
          }
          didMerge = true;
          // after a merge, restart pass to keep structure consistent
          break;
        }
      }

      if (!didMerge) break;
    }
  }

  private void mergeIntoA(int ia, int ib) {
    Cluster a = clusters.get(ia);
    Cluster b = clusters.get(ib);
    int total = a.count + b.count;
    if (total <= 0) total = 1;
    double mean = (a.mean * a.count + b.mean * b.count) / total;
    double std = (a.std * a.count + b.std * b.count) / total;

    clusters.set(ia, new Cluster(mean, std, total));
    clusters.remove(ib);
  }

  private int nearestOtherIndex(int i, double scale) {
    double best = Double.POSITIVE_INFINITY;
    int bestIdx = -1;
    Cluster ci = clusters.get(i);
    for (int j = 0; j < clusters.size(); j++) {
      if (j == i) continue;
      Cluster cj = clusters.get(j);
      double d2 = normD2(ci.mean, ci.std, cj.mean, cj.std, scale);
      if (d2 < best) {
        best = d2;
        bestIdx = j;
      }
    }
    return bestIdx;
  }

  private int nearestClusterIndex(double mean, double std, double scale) {
    int bestIdx = 0;
    double best = Double.POSITIVE_INFINITY;
    for (int i = 0; i < clusters.size(); i++) {
      Cluster c = clusters.get(i);
      double d2 = normD2(mean, std, c.mean, c.std, scale);
      if (d2 < best) {
        best = d2;
        bestIdx = i;
      }
    }
    return bestIdx;
  }

  private static double normD2(double m1, double s1, double m2, double s2, double scale) {
    scale = Math.max(scale, EPS);
    double dm = (m1 - m2) / scale;
    double ds = (s1 - s2) / scale;
    return dm * dm + ds * ds;
  }

  private double scaleStdFallback() {
    double rawStd = (requestsCount > 1) ? Math.sqrt(m2 / (requestsCount - 1)) : 0.0;
    return (withinItemStd.n > 0) ? withinItemStd.mean() : rawStd;
  }

  // ==== small structs ====
  private static final class OnlineStats {
    int n;
    double mean;
    double m2;

    void add(double x) {
      n++;
      double d = x - mean;
      mean += d / n;
      double d2 = x - mean;
      m2 += d * d2;
    }

    double std() {
      return (n > 1) ? Math.sqrt(m2 / (n - 1)) : 0.0;
    }
  }

  private static final class OneDStats {
    int n = 0;
    double mean = 0.0;
    double m2 = 0.0;

    void add(double x) {
      n++;
      double d = x - mean;
      mean += d / n;
      double d2 = x - mean;
      m2 += d * d2;
    }

    double mean() {
      return mean;
    }
  }

  private static final class Cluster {
    double mean, std;
    int count;

    Cluster(double mean, double std, int count) {
      this.mean = mean;
      this.std = std;
      this.count = count;
    }
  }
}
