package com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager;

public interface ChunkManager {
  public long getChunkSize(long itemKey, long itemSize);
}
