package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Stack;
import java.util.Random;

@Policy.PolicySpec(name = "NonBinary")
public final class NonBinaryPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
  final long maximumSize; // in chunks
  long currentSize; // in chunks

  final long BANDWIDTH = 1250; // in MBps
  static final double MEAN = 0.2; // average delay in seconds (e.g., 200 ms)
  static final double STANDARD_DEVIATION = 0.05; // standard deviation in seconds (e.g., 50 ms)

  final Random random;

  public NonBinaryPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.data = new Long2ObjectOpenHashMap<>();
    this.maximumSize = settings.maximumSize();
    this.currentSize = 0;
    this.random = new Random();
  }

  @Override
  public void record(AccessEvent event) {
    long itemKey = event.key();
    Prefix old = data.get(itemKey);
    if (old != null) {
      // TODO: stat and document partial hit
    } else {
      // TODO: stat and document full miss
    }

    Prefix currentPrefix = old == null ? new Prefix(itemKey) : old;
    long idealSize = (long) (BANDWIDTH * sampleSourceProcessingTime()) + 1;
    insertChunks(currentPrefix, idealSize);
  }

  private double sampleSourceProcessingTime() {
    return MEAN + STANDARD_DEVIATION * random.nextGaussian();
  }

  private void insertChunks(Prefix prefix, long idealSize) {
    // TODO:  try to insert more chunks until we reach ideal prefix size,
    //    or we stop due to not benefiting from it

    while (true) {
      if (prefix.size() == idealSize) {
        return;
      }

      ArrayList<Chunk> endChunks = getAllEndChunks();
      // TODO: find smallest benefit chunk and compare to new one we want to add,
      //  insert only if the benefit of inserting is bigger or equal than the benefit of removing,
      //  otherwise break from loop.

    }
  }

  private ArrayList<Chunk> getAllEndChunks() {
    ArrayList<Chunk> endChunks = new ArrayList<>();
    for (Prefix prefix : data.values()) {
      endChunks.add(prefix.chunks.peek());
    }
    return endChunks;
  }

  @Override
  public void finished() {
    Policy.super.finished();
  }

  @Override
  public PolicyStats stats() {
    return null;
  }

  @Override
  public String name() {
    return Policy.super.name();
  }

  record Chunk(int requestIndex) {
  }

  static class Prefix {
    final long itemKey;
    Stack<Chunk> chunks;

    public Prefix(long itemKey) {
      this.itemKey = itemKey;
      this.chunks = new Stack<>();
    }

    public long size() {
      return chunks.size();
    }
  }
}
