package com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager;

import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.sources.Source;

public class SourceBasedChunkManager implements ChunkManager {
  @Override
  public long getChunkSize(long itemKey, long itemSize) {
    Source source = Consts.SOURCES.get((int) (itemKey % Consts.SOURCES.size()));
    return Math.min(source.getChunkSize(), itemSize);
  }
}
