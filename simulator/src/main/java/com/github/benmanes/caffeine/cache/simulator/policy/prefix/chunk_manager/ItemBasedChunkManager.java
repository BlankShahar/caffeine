package com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager;

import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class ItemBasedChunkManager implements ChunkManager {

  private static final class Welford {
    long n = 0;
    double mean = 0.0, m2 = 0.0;

    void update(double x) {
      n++;
      double d = x - mean;
      mean += d / n;
      m2 += d * (x - mean);
    }

    double std() {
      return n > 1 ? Math.sqrt(m2 / (n - 1)) : 0.0;
    }
  }

  private final Long2ObjectOpenHashMap<Welford> perItem = new Long2ObjectOpenHashMap<>();
  private final Welford global = new Welford();

  @Override
  public long getChunkSize(long itemKey, long itemSize) {
    Welford st = perItem.get(itemKey);
    double mu = (st != null && st.n > 0) ? st.mean : global.mean;
    double sd = (st != null && st.n > 1) ? st.std() : global.std();
    long bytes = Math.round((mu + 2.0 * sd) * Consts.BANDWIDTH);
    if (itemSize > 0) bytes = Math.min(bytes, itemSize);
    return Math.max(1L, bytes);
  }

  public void addDelay(long itemKey, double delaySeconds) {
    perItem.computeIfAbsent(itemKey, k -> new Welford()).update(delaySeconds);
    global.update(delaySeconds);
  }
}
