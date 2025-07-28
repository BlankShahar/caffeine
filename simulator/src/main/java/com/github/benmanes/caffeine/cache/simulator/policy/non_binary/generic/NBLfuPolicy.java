package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.generic;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.countmin4.PeriodicResetCountMin4;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.SearchableMinHeap;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.LogNormalSource;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;
import com.typesafe.config.Config;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent.Operation.READ;
import static com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations.calculateLatency;


@Policy.PolicySpec(name = "non-binary.LFU")
public final class NBLfuPolicy implements Policy {
  final Queue<Long> requests;
  final long maximumCacheSize;
  long currentCacheSize;
  final PolicyStats policyStats;
  final Source source;
  final SearchableMinHeap<Long, Prefix> scoreMinHeap;
  private final PeriodicResetCountMin4 sketch;
  int currentTime;

  private static final String CSV_FILE_PATH = "/home/nadavk/shahar-thesis/caffeine/simulator/build/reports/simulate/stats_per_request.csv";
  private static BufferedWriter csvWriter;
  private static int linesSinceFlush = 0;
  private static final int FLUSH_INTERVAL = 1_000_000; // flush every 1M lines
  private double lastTotalDelay = 0;
  private double lastTotalLatency = 0;
  private long lastTotalOperations = 0;

  public NBLfuPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.requests = new ArrayDeque<>();

    this.scoreMinHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::comparePrefixes);
    this.source = new LogNormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);
    this.sketch = new PeriodicResetCountMin4(settings.config());


    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0;
    this.currentTime = 0;

    try {
      csvWriter = new BufferedWriter(new FileWriter(CSV_FILE_PATH, true));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void record(AccessEvent event) {
    currentTime++;
    switch (event.operation()) {
      case READ:
        onRead(event);
        break;
      case WRITE:
        onWrite(event);
        break;
      case DELETE:
        onDelete(event);
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + event.operation());
    }

    Prefix prefix = scoreMinHeap.get(event.key());
    if (prefix != null) appendRequestStatsToCsv(prefix.currentSize, prefix.fullItemSize);
    else appendRequestStatsToCsv(0, event.itemSize());

  }

  private void onWrite(AccessEvent event) {
    onDelete(event);
    onRead(event);
  }

  private void onDelete(AccessEvent event) {
    var existingPrefix = scoreMinHeap.get(event.key());
    if (existingPrefix != null) {
      // prefix exists, remove it
      scoreMinHeap.remove(existingPrefix.itemKey);
      currentCacheSize -= existingPrefix.currentSize;
      policyStats.recordEviction();
      policyStats.recordOperation();
    }
  }

  private void onRead(AccessEvent event) {
    if (currentCacheSize >= maximumCacheSize)
      sketch.ensureCapacity(2_000_000);

    long itemKey = event.key();
    var existingPrefix = scoreMinHeap.get(itemKey);

    if (existingPrefix != null) {
      // prefix exist (partial hit)
      onRequest(existingPrefix, event.retrievalDelay(), event.operation());
    } else {
      // prefix missing (full miss)
      long currentSize = event.itemSize();
      var newPrefix = new Prefix(itemKey, currentSize, source);
      onRequest(newPrefix, event.retrievalDelay(), event.operation());
    }
  }

  private void onRequest(Prefix prefix, double sourceDelay, AccessEvent.Operation operation) {
    if (operation == READ) recordRequestStatistics(prefix, sourceDelay);
    handleRequestsFrequency(prefix);
    waterFill(prefix);
  }

  private void handleRequestsFrequency(Prefix prefix) {
    sketch.increment(prefix.itemKey);
    prefix.frequency = sketch.frequency(prefix.itemKey);

    if (scoreMinHeap.contains(prefix.itemKey)) {
      scoreMinHeap.upsert(prefix.itemKey, prefix);
      policyStats.recordOperation();
    }
  }

  private void recordRequestStatistics(Prefix old, double sourceDelay) {
    // Total delay
    double underflowDelay = calculateDelay(sourceDelay, old);
    policyStats.addDelay(underflowDelay);
    double latency = calculateLatency(sourceDelay, old.fullItemSize(), old.currentSize(), Consts.BANDWIDTH);
    policyStats.addLatency(latency);
  }

  private void abortEvictions(HashMap<Prefix, Long> victimToOriginalSize) {
    for (Map.Entry<Prefix, Long> entry : victimToOriginalSize.entrySet()) {
      Prefix victim = entry.getKey();
      long originalSize = entry.getValue();

      currentCacheSize += originalSize - victim.currentSize;
      victim.currentSize = originalSize;
      scoreMinHeap.upsert(victim.itemKey, victim);
      policyStats.recordOperation(); // TODO: should it be here or only if we didn't abort?
    }
  }

  private boolean makeRoom(Prefix prefix) {
    HashMap<Prefix, Long> victimToOriginalSize = new HashMap<>();
    long addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    while (currentCacheSize + addSize > maximumCacheSize) {
      Prefix victim = findVictim();
      if (
        prefix.lfuScoreAfterInsertion() < victim.lfuScoreAfterEviction()
          || prefix.itemKey == victim.itemKey
      ) {
        abortEvictions(victimToOriginalSize);
        return false; // Abort and break
      }

      if (!victimToOriginalSize.containsKey(victim))
        victimToOriginalSize.put(victim, victim.currentSize);

      shrinkPrefix(victim); // remove chunk from the cache & update the heap
      policyStats.recordOperation(); // TODO: should it be here or only if we didn't abort?
    }

    return true;
  }

  private void waterFill(Prefix prefix) {
    long addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    while (!prefix.isFull() && // stop if the prefix is full
      currentCacheSize + addSize <= maximumCacheSize // stop if adding another chunk would exceed the maximum cache size
    ) {
      extendPrefix(prefix);
      addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    }

    if (maximumCacheSize == 0) return;

    while (!prefix.isFull()) {
      if (!makeRoom(prefix))
        break; // If makeRoom returns false, we abort the process
      extendPrefix(prefix);
    }

    // Prefix prefix
    // while(prefix.currentSize < prefix.fullItemSize) {

    // boolean function (makeRoom):
    // @return true iff evicted enough victims to make space for extending the prefix
    // while (min(chunk,tail) doesn't fit in the cache) {
    // victim=findVictim()
    // if (prefix.lfuScoreAfterInsertion() < victim.lfuScoreAfterEviction() || prefix==victim)
    // abort + break
    // shrinkPrefix(victim); // remove chunk from the cache & update the heap
    // addVictimToList(victim);
    // }

    // if makeRoom == false
    //    break

    // extendPrefix(prefix); // add min(chunk,tail) to the prefix & update the heap
    // }

    assert currentCacheSize <= maximumCacheSize : "Current cache size exceeds the maximum cache size (current time: " + currentTime + ")";
    assert currentCacheSize >= 0 : "Current cache size cannot be negative (current time: " + currentTime + ")";
  }

  private void shrinkPrefix(Prefix prefix) {
    if (prefix.isEmpty())
      return;

    long removedSize = prefix.removeChunk();
    currentCacheSize -= removedSize;
    if (prefix.isEmpty())
      scoreMinHeap.remove(prefix.itemKey);
    else
      scoreMinHeap.upsert(prefix.itemKey, prefix);

    policyStats.recordOperation();
    policyStats.recordEviction();

    assert prefix.currentSize >= 0 : "Prefix size cannot be negative (current time: " + currentTime + ")";
    assert currentCacheSize >= 0 : "Current cache size cannot be negative (current time: " + currentTime + ")";
  }

  private void extendPrefix(Prefix prefix) {
    if (prefix.isFull())
      return;

    long addedSize = prefix.insertChunk();
    currentCacheSize += addedSize;
    scoreMinHeap.upsert(prefix.itemKey, prefix);

    policyStats.recordOperation();
    policyStats.recordAdmission();

    assert prefix.currentSize <= prefix.fullItemSize : "Prefix size exceeds its full size (current time: " + currentTime + ")";
  }

  /**
   * @return the victim chunk to be evicted, or null if no suitable one is found
   */
  private Prefix findVictim() {
    return scoreMinHeap.min().value();
  }

  /**
   * Calculate the delay of fetching a partial cached object
   *
   * @param sourceDelay in seconds
   * @param prefix      the prefix of the item
   * @return the delay in seconds
   */
  private static double calculateDelay(double sourceDelay, Prefix prefix) {
    return TimeCalculations.calculateUnderflowDelay(sourceDelay, prefix.fullItemSize(), prefix.currentSize(), Consts.BANDWIDTH);
  }

  public int comparePrefixes(long prefixKey1, long prefixKey2) {
    Prefix p1 = scoreMinHeap.get(prefixKey1);
    Prefix p2 = scoreMinHeap.get(prefixKey2);
    assert p1 != null;
    assert p2 != null;
    return p1.lfuCompareTo(p2);
  }


  private void appendRequestStatsToCsv(long currentPrefixSize, long fullItemSize) {
    double currentDelay = policyStats.totalDelay();
    double currentLatency = policyStats.totalLatency();
    long currentOperations = policyStats.operationCount();

    double deltaDelay = currentDelay - lastTotalDelay;
    double deltaLatency = currentLatency - lastTotalLatency;
    long deltaOperations = currentOperations - lastTotalOperations;


    try {
      csvWriter.write(deltaDelay + ","
        + deltaLatency + ","
        + deltaOperations + ","
        + scoreMinHeap.idxMap.size() + ","
        + currentPrefixSize + ","
        + Math.ceil((double) currentPrefixSize / Consts.CHUNK_SIZE) + ","
        + fullItemSize + ","
      );

      csvWriter.newLine();
      linesSinceFlush++;

      if (linesSinceFlush >= FLUSH_INTERVAL) {
        csvWriter.flush();
        linesSinceFlush = 0;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    // עדכון הערכים האחרונים
    lastTotalDelay = currentDelay;
    lastTotalLatency = currentLatency;
    lastTotalOperations = currentOperations;
  }

  @Override
  public void finished() {
    Policy.super.finished();
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  @Override
  public String name() {
    return Policy.super.name();
  }

  static public class Prefix {
    final long itemKey, fullItemSize;
    final Source source;
    long currentSize;
    long frequency;

    public Prefix(long itemKey, long fullItemSize, Source source) {
      this.itemKey = itemKey;
      this.fullItemSize = fullItemSize;
      this.source = source;
      this.frequency = 0;
      this.currentSize = 0;
    }

    public double lfuScoreAfterInsertion() {
      // Idea - frequency times the probability of not experiencing delay
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
        Math.min(currentSize + Consts.CHUNK_SIZE, fullItemSize), Consts.BANDWIDTH);
      return frequency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double lfuScoreAfterEviction() {
      // Idea - frequency times the probability of not experiencing delay
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
        Math.max(0, currentSize - Consts.CHUNK_SIZE), Consts.BANDWIDTH);
      return frequency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double frequency() {
      return (double) frequency / Consts.REQUESTS_FREQUENCY_PERIOD;
    }

    public long insertChunk() {
      long addSize = Math.min(fullItemSize - currentSize, Consts.CHUNK_SIZE);
      currentSize += addSize;
      return addSize;
    }

    public long removeChunk() {
      long remainder = currentSize % Consts.CHUNK_SIZE;
      long removeSize = (remainder > 0) ? remainder : Consts.CHUNK_SIZE;
      currentSize -= removeSize;
      return removeSize;
    }

    public double currentSize() {
      return currentSize;
    }

    public double fullItemSize() {
      return fullItemSize;
    }

    public boolean isFull() {
      return currentSize == fullItemSize;
    }

    public boolean isEmpty() {
      return currentSize == 0;
    }


    public int lfuCompareTo(Prefix other) {
      return Double.compare(this.lfuScoreAfterEviction(), other.lfuScoreAfterEviction());
    }
  }
}
