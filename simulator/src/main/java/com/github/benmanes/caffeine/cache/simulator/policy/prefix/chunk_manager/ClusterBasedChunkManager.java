package com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager;

import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterBasedChunkManager implements ChunkManager {
  // Spawn a new cluster if normalized distance^2 exceeds this radius.
  // Distances are normalized by the global averageStd to stay scale-free.
  private static final double NEW_CLUSTER_THRESHOLD2 = 1.0;
  private static final double EPS = 1e-9;
  // add alongside your thresholds
  private static final double MERGE_THRESHOLD2 = 0.25; // smaller than NEW_CLUSTER_THRESHOLD2


  // Per-item online stats
  private final Map<Long, OnlineStats> itemStats = new HashMap<>();
  // Cluster centroids in (mean, std)
  private final List<Cluster> clusters = new ArrayList<>();

  // Global aggregate stats (fallback when the item has no samples)
  private double averageDelay = 0.0;
  private double m2 = 0.0;
  private double averageStd = 0.0;
  private int requestsCount = 0;

  @Override
  public long getChunkSize(long itemKey, long itemSize) {
    OnlineStats s = itemStats.get(itemKey);

    final double mean, std;
    if (s == null || s.n == 0) {
      // No prior samples → use global
      mean = averageDelay;
      std = averageStd;
    } else {
      // At least one sample → use item's own stats
      mean = s.mean;
      std = s.std(); // 0 if n==1, sample std if n>=2
    }

    long chunk = Math.round((mean + 2 * std) * Consts.BANDWIDTH);
    if (chunk > itemSize) chunk = itemSize;
    if (chunk < 1L) chunk = 1L;
    return chunk;
  }

  public void addDelay(long itemKey, double delay) {
    // Update per-item stats
    OnlineStats s = itemStats.computeIfAbsent(itemKey, k -> new OnlineStats());
    s.add(delay);

    // Update global stats (Welford)
    requestsCount++;
    double d = delay - averageDelay;
    averageDelay += d / requestsCount;
    double d2 = delay - averageDelay;
    m2 += d * d2;
    averageStd = (requestsCount > 1) ? Math.sqrt(m2 / (requestsCount - 1)) : 0.0;

    // Associate the item with a cluster whenever it has ≥1 sample
    double m = s.mean;
    double st = s.std(); // if n==1 this is 0, which is fine for association

    if (clusters.isEmpty()) {
      clusters.add(new Cluster(m, st, 1));
      consolidateIfClose();
      return;
    }

    int idx = nearestClusterIndex(m, st);
    Cluster c = clusters.get(idx);

    double scale = Math.max(averageStd, EPS);
    double dist2 = sq((m - c.mean) / scale) + sq((st - c.std) / scale);

    if (dist2 > NEW_CLUSTER_THRESHOLD2) {
      // Far → start a new cluster
      clusters.add(new Cluster(m, st, 1));
    } else {
      // Close → update the nearest cluster centroid online
      c.count++;
      double lr = 1.0 / c.count;
      c.mean += lr * (m - c.mean);
      c.std += lr * (st - c.std);
    }
    consolidateIfClose();
  }

  // ---------- helpers ----------

  // Merge any very-close clusters (repeat until none are close)
  private void consolidateIfClose() {
    if (clusters.size() < 2) return;

    final double scale = Math.max(averageStd, EPS);

    boolean merged;
    do {
      merged = false;
      int ia = -1, ib = -1;
      double best = Double.POSITIVE_INFINITY;

      // find closest pair
      for (int i = 0; i < clusters.size(); i++) {
        Cluster ci = clusters.get(i);
        for (int j = i + 1; j < clusters.size(); j++) {
          Cluster cj = clusters.get(j);
          double d2 = sq((ci.mean - cj.mean) / scale) + sq((ci.std - cj.std) / scale);
          if (d2 < MERGE_THRESHOLD2 && d2 < best) {
            best = d2;
            ia = i;
            ib = j;
          }
        }
      }

      if (ia != -1) {
        // weighted centroid
        Cluster a = clusters.get(ia), b = clusters.get(ib);
        int total = a.count + b.count;
        double mean = (a.mean * a.count + b.mean * b.count) / total;
        double std = (a.std * a.count + b.std * b.count) / total;

        clusters.set(ia, new Cluster(mean, std, total));
        clusters.remove(ib);
        merged = true;
      }
    } while (merged);
  }


  private int nearestClusterIndex(double mean, double std) {
    double scale = Math.max(averageStd, EPS);
    int bestIdx = 0;
    double best = Double.POSITIVE_INFINITY;
    for (int i = 0; i < clusters.size(); i++) {
      Cluster c = clusters.get(i);
      double d2 = sq((mean - c.mean) / scale) + sq((std - c.std) / scale);
      if (d2 < best) {
        best = d2;
        bestIdx = i;
      }
    }
    return bestIdx;
  }

  private static double sq(double x) {
    return x * x;
  }

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
