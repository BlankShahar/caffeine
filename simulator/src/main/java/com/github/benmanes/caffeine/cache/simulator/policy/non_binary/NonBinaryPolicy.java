package com.github.benmanes.caffeine.cache.simulator.policy.non_binary;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import java.util.Stack;

@Policy.PolicySpec(name = "NonBinary")
public final class NonBinaryPolicy implements Policy {
  final Long2ObjectMap<Prefix> data;
  final long maximumSize; // in chunks
  long currentSize; // in chunks
  long currentRequest;

  final long BANDWIDTH = 1250; // in MBps
  static final double MEAN = 0.2; // average delay in seconds (e.g., 200 ms)
  static final double STANDARD_DEVIATION = 0.05; // standard deviation in seconds (e.g., 50 ms)

  final Random random;

  public NonBinaryPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.data = new Long2ObjectOpenHashMap<>();
    this.maximumSize = settings.maximumSize();
    this.currentSize = 0;
    this.currentRequest = 0;
    this.random = new Random();
  }

  @Override
  public void record(AccessEvent event) {
    currentRequest++;

    long itemKey = event.key();
    Prefix old = data.get(itemKey);
    if (old != null) {
      // TODO: stat and document partial hit
    } else {
      // TODO: stat and document full miss
    }
    Prefix currentPrefix = old == null ? new Prefix(itemKey, currentRequest) : old;
    long idealSize = (long) (BANDWIDTH * sampleSourceProcessingTime()) + 1;
    insertChunks(currentPrefix, idealSize);
    if (currentPrefix.size() > 0) {
      data.put(itemKey, currentPrefix);
    }
  }

  private double sampleSourceProcessingTime() {
    return MEAN + STANDARD_DEVIATION * random.nextGaussian();
  }

  private void insertChunks(Prefix prefix, long idealSize) {
    // try to insert more chunks until we reach ideal prefix size,
    //  or we stop due to not benefiting from it

    while (true) {
      if (prefix.size() == idealSize) {
        break;
      }

      ArrayList<Chunk> endChunks = getAllEndChunks();
      Chunk newChunk = new Chunk(prefix.itemKey, currentRequest);
      Chunk victim = findVictim(newChunk, endChunks);
      if (victim == null) {
        break;
      }

      Prefix victimPrefix = data.get(victim.itemKey);
      victimPrefix.chunks.pop();
      if (victimPrefix.size() == 0) {
        data.remove(victimPrefix.itemKey);
      }
    }
  }

  @Nullable
  private Chunk findVictim(Chunk newChunk, ArrayList<Chunk> victimCandidates) {
    ArrayList<Chunk> possibleVictims = new ArrayList<>();
    for (Chunk candidate : victimCandidates) {
      if (benefit(newChunk) >= benefit(candidate)) {
        possibleVictims.add(candidate);
      }
    }
    return possibleVictims.stream()
      .min(Comparator.comparingDouble(Chunk::frequency))
      .orElse(null);
  }

  private double benefit(Chunk chunk) {
    double possibilityForRequest = Math.exp(chunk.requestIndex - currentRequest);
    return possibilityForRequest * chunk.frequency;
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

  record Chunk(long itemKey, long requestIndex, long frequency) {

    public Chunk(long itemKey, long requestIndex) {
      this(itemKey, requestIndex, 1);
    }
  }

  static class Prefix {
    final long itemKey, requestIndex, frequency;
    Stack<Chunk> chunks;

    public Prefix(long itemKey, long requestIndex) {
      this.itemKey = itemKey;
      this.requestIndex = requestIndex;
      this.frequency = 1;
      this.chunks = new Stack<>();
    }

    public long size() {
      return chunks.size();
    }
  }
}
