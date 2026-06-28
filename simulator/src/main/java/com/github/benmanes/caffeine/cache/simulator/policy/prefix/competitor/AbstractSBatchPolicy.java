package com.github.benmanes.caffeine.cache.simulator.policy.prefix.competitor;

import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateUnderflowDelay;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.TraceAwarePolicy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.typesafe.config.Config;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/** Shared implementation for oracle and monitored SBatch baselines. */
abstract class AbstractSBatchPolicy implements TraceAwarePolicy {
  enum Mode { ORACLE, MONITORED }

  private final Map<Long, ItemStats> itemStats;
  private final Long2LongOpenHashMap prefixByKey;
  private final LongOpenHashSet cachedPrefixes;
  private final PolicyStats policyStats;
  private final Mode mode;

  private final long maximumSize;
  private final long allocationGrainBytes;
  private final double serverCost;
  private final double proxyCost;

  private boolean allocationReady;
  private long sequenceTime;
  private long allocatedBytes;
  private long allocatedItems;

  AbstractSBatchPolicy(Config config, Mode mode, String name) {
    var settings = new BasicSettings(config);
    this.mode = mode;
    this.maximumSize = settings.maximumSize();
    this.policyStats = new PolicyStats(name);
    this.policyStats.addMetric("Allocated Prefix Bytes", () -> allocatedBytes);
    this.policyStats.addMetric("Allocated Items", () -> allocatedItems);

    this.allocationGrainBytes = config.getLong("prefix.sbatch.allocation-grain-bytes");
    this.serverCost = config.getDouble("prefix.sbatch.server-cost");
    this.proxyCost = config.getDouble("prefix.sbatch.proxy-cost");
    this.itemStats = new HashMap<>();
    this.prefixByKey = new Long2LongOpenHashMap();
    this.prefixByKey.defaultReturnValue(0L);
    this.cachedPrefixes = new LongOpenHashSet();

    if (mode == Mode.MONITORED && settings.trace().warmupEvents() <= 0L) {
      throw new IllegalStateException("prefix.competitor.SBatch.Monitored requires "
          + "trace.warmup-percent or trace.warmup-events");
    }
  }

  @Override
  public boolean needsTracePreparation() {
    return mode == Mode.ORACLE;
  }

  @Override
  public void prepare(AccessEvent event) {
    if (mode == Mode.ORACLE) {
      collect(event);
    }
  }

  @Override
  public void prepareFinished() {
    if (mode == Mode.ORACLE) {
      allocate();
    }
  }

  @Override
  public void warmupFinished() {
    if (mode == Mode.MONITORED && !allocationReady) {
      allocate();
    }
    policyStats.reset();
  }

  @Override
  public void record(AccessEvent event) {
    sequenceTime++;
    if (mode == Mode.MONITORED && !allocationReady) {
      collect(event);
      return;
    }
    switch (event.operation()) {
      case READ:
        onRead(event);
        break;
      case WRITE:
        onWrite(event);
        break;
      case DELETE:
        onDelete(event.key());
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + event.operation());
    }
  }

  private void collect(AccessEvent event) {
    if (event.operation() != AccessEvent.Operation.READ) {
      return;
    }
    var stats = itemStats.computeIfAbsent(event.key(), ignored -> new ItemStats(event.key()));
    stats.itemSize = Math.max(stats.itemSize, event.itemSize());
    stats.requestCount++;
    if (event.lambda() > 0.0) {
      stats.lambda = event.lambda();
    }
    long time = (event.timestamp() >= 0L) ? event.timestamp() : sequenceTime;
    stats.firstTimestamp = Math.min(stats.firstTimestamp, time);
    stats.lastTimestamp = Math.max(stats.lastTimestamp, time);
  }

  private void allocate() {
    if (allocationReady) {
      return;
    }
    itemStats.values().forEach(ItemStats::finalizeLambdaIfNeeded);
    allocateByGreedyMarginal();
    for (long key : prefixByKey.keySet()) {
      cachedPrefixes.add(key);
    }
    allocatedItems = prefixByKey.size();
    allocationReady = true;
  }

  /**
   * For SBatch, the saving curve is separable and concave in the prefix size, so repeatedly taking
   * the largest next marginal saving yields the same discrete optimum as the paper's knapsack DP,
   * but is feasible for large IBM traces.
   */
  private void allocateByGreedyMarginal() {
    var queue = new PriorityQueue<Candidate>(Comparator.comparingDouble(Candidate::gain).reversed());
    for (ItemStats item : itemStats.values()) {
      if (item.itemSize <= 0L || item.lambda <= 0.0) {
        continue;
      }
      Candidate candidate = Candidate.next(item, 0L, allocationGrainBytes, serverCost, proxyCost);
      if (candidate != null && candidate.deltaBytes() <= maximumSize && candidate.gain() > 0.0) {
        queue.add(candidate);
      }
    }

    while (!queue.isEmpty()) {
      Candidate candidate = queue.poll();
      if (candidate.gain() <= 0.0) {
        break;
      }
      if ((allocatedBytes + candidate.deltaBytes()) > maximumSize) {
        continue;
      }
      allocatedBytes += candidate.deltaBytes();
      prefixByKey.put(candidate.item().key, candidate.nextPrefixBytes());

      Candidate next = Candidate.next(candidate.item(), candidate.nextPrefixBytes(),
          allocationGrainBytes, serverCost, proxyCost);
      if (next != null && next.gain() > 0.0) {
        queue.add(next);
      }
    }
  }

  private void onWrite(AccessEvent event) {
    onDelete(event.key());
    onRead(event);
  }

  private void onDelete(long key) {
    if (cachedPrefixes.remove(key)) {
      policyStats.recordEviction();
      policyStats.recordOperation();
    }
  }

  private void onRead(AccessEvent event) {
    long prefixSize = Math.min(event.itemSize(), prefixByKey.get(event.key()));
    boolean hit = prefixSize > 0L && cachedPrefixes.contains(event.key());
    long cachedSize = hit ? prefixSize : 0L;

    double delay = calculateUnderflowDelay(event.retrievalDelay(), event.itemSize(), cachedSize,
        Consts.BANDWIDTH);
    double latency = calculateLatency(event.retrievalDelay(), event.itemSize(), cachedSize,
        Consts.BANDWIDTH);
    policyStats.addDelay(delay);
    policyStats.addLatency(latency);

    if (hit) {
      policyStats.recordHit();
    } else {
      policyStats.recordMiss();
      if (prefixSize > 0L) {
        cachedPrefixes.add(event.key());
        policyStats.recordAdmission();
      }
    }
    policyStats.recordOperation();
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  private static double cost(ItemStats item, long prefixBytes, double serverCost, double proxyCost) {
    double lambda = item.lambda;
    double prefixSeconds = prefixBytes / (double) Consts.BANDWIDTH;
    double serverBytes = (item.itemSize - prefixBytes) / (1.0 + lambda * prefixSeconds);
    return lambda * ((serverCost * serverBytes) + (proxyCost * item.itemSize));
  }

  private static final class Candidate {
    private final long nextPrefixBytes;
    private final long deltaBytes;
    private final ItemStats item;
    private final double gain;

    private Candidate(ItemStats item, long nextPrefixBytes, long deltaBytes, double gain) {
      this.item = item;
      this.nextPrefixBytes = nextPrefixBytes;
      this.deltaBytes = deltaBytes;
      this.gain = gain;
    }

    static Candidate next(ItemStats item, long currentPrefixBytes, long grainBytes,
        double serverCost, double proxyCost) {
      if (currentPrefixBytes >= item.itemSize) {
        return null;
      }
      long nextPrefixBytes = Math.min(item.itemSize, currentPrefixBytes + grainBytes);
      long deltaBytes = nextPrefixBytes - currentPrefixBytes;
      double gain = cost(item, currentPrefixBytes, serverCost, proxyCost)
          - cost(item, nextPrefixBytes, serverCost, proxyCost);
      return new Candidate(item, nextPrefixBytes, deltaBytes, gain);
    }

    ItemStats item() {
      return item;
    }

    long nextPrefixBytes() {
      return nextPrefixBytes;
    }

    long deltaBytes() {
      return deltaBytes;
    }

    double gain() {
      return gain;
    }
  }

  private static final class ItemStats {
    private final long key;
    private long firstTimestamp = Long.MAX_VALUE;
    private long lastTimestamp = Long.MIN_VALUE;
    private long requestCount;
    private long itemSize;
    private double lambda;

    ItemStats(long key) {
      this.key = key;
    }

    void finalizeLambdaIfNeeded() {
      if (lambda > 0.0 || requestCount == 0L) {
        return;
      }
      long durationMillis = lastTimestamp - firstTimestamp;
      if (durationMillis > 0L) {
        lambda = requestCount / (durationMillis / 1000.0);
      } else {
        lambda = requestCount;
      }
    }
  }
}
