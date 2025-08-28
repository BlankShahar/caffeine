package com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager;

import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class ItemBasedChunkManager implements ChunkManager {

  /** Exponentially-weighted mean & variance (via second moment). */
  private static final class EmaStats {
    final double alpha;          // 0<alpha<=1 (e.g., 2/(span+1) or ln(2)/halfLife)
    boolean initialized = false;
    double m = 0.0;              // EMA of x
    double q = 0.0;              // EMA of x^2

    EmaStats(double alpha) { this.alpha = alpha; }

    void update(double x) {
      if (!initialized) {
        m = x; q = x * x; initialized = true; return;
      }
      double a = alpha, b = 1.0 - a;
      m = b * m + a * x;
      q = b * q + a * (x * x);
    }

    double mean() { return m; }
    double var()  { return Math.max(0.0, q - m * m); }  // EWMA variance
    double std()  { return Math.sqrt(var()); }
    boolean hasData() { return initialized; }
  }

  private final double alpha; // tune via span/half-life as you like
  private final Long2ObjectOpenHashMap<EmaStats> perItem = new Long2ObjectOpenHashMap<>();
  private final EmaStats global;

  public ItemBasedChunkManager() {
    this(0.2); // default smoothing (≈ span ~ 99, half-life ≈ 34)
  }

  public ItemBasedChunkManager(double alpha) {
    this.alpha = alpha;
    this.global = new EmaStats(alpha);
  }

  @Override
  public long getChunkSize(long itemKey, long itemSize) {
    EmaStats st = perItem.get(itemKey);
    double mu = (st != null && st.hasData()) ? st.mean() : global.mean();
    double sd = (st != null && st.hasData()) ? st.std()  : global.std();
    long bytes = Math.round((mu + 2.0 * sd) * Consts.BANDWIDTH);
    if (itemSize > 0) bytes = Math.min(bytes, itemSize);
    return Math.max(0L, bytes);
  }

  public void addDelay(long itemKey, double delaySeconds) {
    perItem.computeIfAbsent(itemKey, k -> new EmaStats(alpha)).update(delaySeconds);
    global.update(delaySeconds);
  }

  // In com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager.ItemBasedChunkManager

  /** Return a snapshot of this item's (mean,std) as used by the manager. */
  public synchronized boolean getMeanStd(long itemKey, double[] out) {
    EmaStats st = perItem.get(itemKey);           // whatever map you already have
    out[0] = st.mean();
    out[1] = Math.max(st.std(), Consts.EPS_STD);
    return true;
  }

  /** Enumerate the keys the manager is currently tracking. */
  public synchronized java.util.Set<Long> getTrackedItemKeys() {
    return new java.util.HashSet<>(perItem.keySet());
  }

}
