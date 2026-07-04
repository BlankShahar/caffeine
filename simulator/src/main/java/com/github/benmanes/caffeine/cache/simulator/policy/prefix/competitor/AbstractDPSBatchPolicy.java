package com.github.benmanes.caffeine.cache.simulator.policy.prefix.competitor;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.TraceAwarePolicy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateLatency;
import static com.github.benmanes.caffeine.cache.simulator.policy.prefix.TimeCalculations.calculateUnderflowDelay;

/**
 * Shared exact-DP SBatch implementation for Oracle and Monitored variants.
 */
abstract class AbstractDPSBatchPolicy implements TraceAwarePolicy {
  enum Mode {ORACLE, MONITORED}

  private static final long DEFAULT_MAX_DP_CELLS = 250_000_000L;
  private static final long DEFAULT_MAX_DP_TRANSITIONS = 5_000_000_000L;
  private static final double EPSILON = 1.0e-12;

  private final Long2ObjectOpenHashMap<ItemStats> itemStatsByKey;
  private final Long2LongOpenHashMap prefixByKey;
  private final LongOpenHashSet cachedPrefixes;
  private final PolicyStats policyStats;
  private final Mode mode;

  private final long maximumSize;
  private final long allocationGrainBytes;
  private final double serverCost;
  private final double proxyCost;
  private final long maxDpCells;
  private final long maxDpTransitions;

  private boolean allocationReady;
  private long sequenceTime;
  private long allocatedBytes;
  private long allocatedReservedBytes;
  private long allocatedItems;

  AbstractDPSBatchPolicy(Config config, Mode mode, String name) {
    var settings = new BasicSettings(config);

    this.mode = mode;
    this.maximumSize = settings.maximumSize();
    this.allocationGrainBytes = config.getLong("prefix.sbatch.allocation-grain-bytes");
    this.serverCost = config.getDouble("prefix.sbatch.server-cost");
    this.proxyCost = config.getDouble("prefix.sbatch.proxy-cost");

    this.maxDpCells = config.hasPath("prefix.sbatch.dp.max-cells")
      ? config.getLong("prefix.sbatch.dp.max-cells")
      : DEFAULT_MAX_DP_CELLS;

    this.maxDpTransitions = config.hasPath("prefix.sbatch.dp.max-transitions")
      ? config.getLong("prefix.sbatch.dp.max-transitions")
      : DEFAULT_MAX_DP_TRANSITIONS;

    this.itemStatsByKey = new Long2ObjectOpenHashMap<>();
    this.prefixByKey = new Long2LongOpenHashMap();
    this.prefixByKey.defaultReturnValue(0L);
    this.cachedPrefixes = new LongOpenHashSet();

    this.policyStats = new PolicyStats(name);
    this.policyStats.addMetric("Allocated Prefix Bytes", () -> allocatedBytes);
    this.policyStats.addMetric("Allocated Reserved Bytes", () -> allocatedReservedBytes);
    this.policyStats.addMetric("Allocated Items", () -> allocatedItems);

    if (allocationGrainBytes <= 0L) {
      throw new IllegalArgumentException("prefix.sbatch.allocation-grain-bytes must be positive");
    }

    if (mode == Mode.MONITORED && settings.trace().warmupEvents() <= 0L) {
      throw new IllegalStateException("prefix.competitor.DPSBatch.Monitored requires "
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
      sequenceTime++;
      collect(event);
    }
  }

  @Override
  public void prepareFinished() {
    if (mode == Mode.ORACLE) {
      allocateByExactDp();
    }
  }

  @Override
  public void warmupFinished() {
    if (mode == Mode.MONITORED && !allocationReady) {
      allocateByExactDp();
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

    ItemStats stats = itemStatsByKey.computeIfAbsent(event.key(), ItemStats::new);
    stats.itemSize = Math.max(stats.itemSize, event.itemSize());
    stats.requestCount++;

    if (event.lambda() > 0.0) {
      stats.lambda = event.lambda();
    }

    long time = (event.timestamp() >= 0L) ? event.timestamp() : sequenceTime;
    stats.firstTimestamp = Math.min(stats.firstTimestamp, time);
    stats.lastTimestamp = Math.max(stats.lastTimestamp, time);
  }

  private void allocateByExactDp() {
    if (allocationReady) {
      return;
    }

    if (maximumSize <= 0L) {
      allocationReady = true;
      return;
    }

    long capacityUnitsLong = maximumSize / allocationGrainBytes;
    if (capacityUnitsLong <= 0L) {
      allocationReady = true;
      return;
    }
    if (capacityUnitsLong > Integer.MAX_VALUE - 1L) {
      throw new IllegalStateException("SBatch exact DP capacity is too large: "
        + capacityUnitsLong + " units. Increase prefix.sbatch.allocation-grain-bytes.");
    }

    int capacityUnits = (int) capacityUnitsLong;
    int states = capacityUnits + 1;

    List<ItemStats> items = new ArrayList<>();
    for (ItemStats item : itemStatsByKey.values()) {
      item.finalizeLambdaIfNeeded();
      if (item.itemSize > 0L && item.lambda > 0.0) {
        items.add(item);
      }
    }
    items.sort(Comparator.comparingLong(item -> item.key));

    long decisionCells = saturatedMultiply(items.size(), states);
    if (decisionCells > maxDpCells || decisionCells > Integer.MAX_VALUE) {
      throw new IllegalStateException("SBatch exact DP requires " + decisionCells
        + " decision cells. Increase prefix.sbatch.allocation-grain-bytes, "
        + "or increase prefix.sbatch.dp.max-cells if you have enough heap.");
    }

    long transitions = estimateTransitions(items, capacityUnits);
    if (transitions > maxDpTransitions) {
      throw new IllegalStateException("SBatch exact DP requires about " + transitions
        + " transitions. Increase prefix.sbatch.allocation-grain-bytes, "
        + "or increase prefix.sbatch.dp.max-transitions if this runtime is acceptable.");
    }

    double[] previous = new double[states];
    double[] current = new double[states];
    int[] decisions = new int[(int) decisionCells];

    for (int itemIndex = 0; itemIndex < items.size(); itemIndex++) {
      ItemStats item = items.get(itemIndex);
      int maxPrefixUnits = (int) Math.min(
        ceilDiv(item.itemSize, allocationGrainBytes),
        (long) capacityUnits);

      double[] savings = savingsByPrefixUnits(item, maxPrefixUnits);

      for (int cacheUnits = 0; cacheUnits <= capacityUnits; cacheUnits++) {
        double bestSaving = previous[cacheUnits];
        int bestPrefixUnits = 0;

        int maxChoice = Math.min(maxPrefixUnits, cacheUnits);
        for (int prefixUnits = 1; prefixUnits <= maxChoice; prefixUnits++) {
          double candidate = previous[cacheUnits - prefixUnits] + savings[prefixUnits];
          if (candidate > bestSaving + EPSILON) {
            bestSaving = candidate;
            bestPrefixUnits = prefixUnits;
          }
        }

        current[cacheUnits] = bestSaving;
        decisions[index(itemIndex, cacheUnits, states)] = bestPrefixUnits;
      }

      double[] tmp = previous;
      previous = current;
      current = tmp;
      Arrays.fill(current, 0.0);
    }

    traceBackAllocation(items, decisions, capacityUnits, states);

    for (long key : prefixByKey.keySet()) {
      cachedPrefixes.add(key);
    }

    allocatedItems = prefixByKey.size();
    allocationReady = true;
  }

  private double[] savingsByPrefixUnits(ItemStats item, int maxPrefixUnits) {
    double[] savings = new double[maxPrefixUnits + 1];
    double noCacheCost = cost(item, 0L);

    for (int units = 1; units <= maxPrefixUnits; units++) {
      long prefixBytes = prefixBytes(item, units);
      savings[units] = noCacheCost - cost(item, prefixBytes);
    }

    return savings;
  }

  private void traceBackAllocation(
    List<ItemStats> items, int[] decisions, int capacityUnits, int states) {
    int remainingUnits = capacityUnits;

    for (int itemIndex = items.size() - 1; itemIndex >= 0; itemIndex--) {
      int prefixUnits = decisions[index(itemIndex, remainingUnits, states)];
      if (prefixUnits > 0) {
        ItemStats item = items.get(itemIndex);
        long prefixBytes = prefixBytes(item, prefixUnits);

        if (prefixBytes > 0L) {
          prefixByKey.put(item.key, prefixBytes);
          allocatedBytes += prefixBytes;
          allocatedReservedBytes += prefixUnits * allocationGrainBytes;
        }

        remainingUnits -= prefixUnits;
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

    double delay = calculateUnderflowDelay(
      event.retrievalDelay(), event.itemSize(), cachedSize, Consts.BANDWIDTH);
    double latency = calculateLatency(
      event.retrievalDelay(), event.itemSize(), cachedSize, Consts.BANDWIDTH);

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

  private double cost(ItemStats item, long prefixBytes) {
    double lambda = item.lambda;
    double prefixSeconds = prefixBytes / (double) Consts.BANDWIDTH;
    double suffixBytes = Math.max(0L, item.itemSize - prefixBytes);
    double serverBytes = suffixBytes / (1.0 + lambda * prefixSeconds);

    return lambda * ((serverCost * serverBytes) + (proxyCost * item.itemSize));
  }

  private long prefixBytes(ItemStats item, int prefixUnits) {
    long rawBytes = saturatedMultiply(prefixUnits, allocationGrainBytes);
    return Math.min(item.itemSize, rawBytes);
  }

  private long estimateTransitions(List<ItemStats> items, int capacityUnits) {
    long total = 0L;

    for (ItemStats item : items) {
      long maxPrefixUnits = Math.min(ceilDiv(item.itemSize, allocationGrainBytes), capacityUnits);
      long itemTransitions;

      if (maxPrefixUnits >= capacityUnits) {
        itemTransitions = ((long) capacityUnits * (capacityUnits + 1L)) / 2L;
      } else {
        itemTransitions = (maxPrefixUnits * (capacityUnits + 1L))
          - ((maxPrefixUnits * (maxPrefixUnits + 1L)) / 2L);
      }

      total = saturatedAdd(total, itemTransitions);
      if (total > maxDpTransitions) {
        return total;
      }
    }

    return total;
  }

  private static int index(int itemIndex, int cacheUnits, int states) {
    return (itemIndex * states) + cacheUnits;
  }

  private static long ceilDiv(long value, long divisor) {
    return (value + divisor - 1L) / divisor;
  }

  private static long saturatedMultiply(long a, long b) {
    if (a == 0L || b == 0L) {
      return 0L;
    }
    if (a > Long.MAX_VALUE / b) {
      return Long.MAX_VALUE;
    }
    return a * b;
  }

  private static long saturatedAdd(long a, long b) {
    long r = a + b;
    return (r < 0L || r < a) ? Long.MAX_VALUE : r;
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
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
