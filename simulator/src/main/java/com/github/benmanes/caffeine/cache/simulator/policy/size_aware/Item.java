package com.github.benmanes.caffeine.cache.simulator.policy.size_aware;

public class Item {
  public final long key;
  public final long size; // in chunks
  public long frequency;

  public Item(long key, long size) {
    this.key = key;
    this.size = size;
    this.frequency = 1;
  }
}
