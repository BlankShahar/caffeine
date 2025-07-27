package com.github.benmanes.caffeine.cache.simulator.policy.non_binary.generic;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
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
import java.util.Queue;

import static com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent.Operation.READ;
import static com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations.calculateLatency;


@Policy.PolicySpec(name = "non-binary.LRU")
public final class NBLruPolicy implements Policy {
  final Queue<Long> requests;
  static long currentTime;
  final long maximumCacheSize;
  long currentCacheSize;
  final PolicyStats policyStats;
  final SearchableMinHeap<Long, Prefix> scoreMinHeap;
  Source source;

  private static final String CSV_FILE_PATH = "C:\\Users\\gil\\Desktop\\2nd Degree\\Thesis\\caffeine\\simulator\\build\\reports\\simulate\\NB_LRU-request_stats.csv";
  private static BufferedWriter csvWriter;
  private static int linesSinceFlush = 0;
  private static final int FLUSH_INTERVAL = 10000; // flush every 10k lines
  private double lastTotalDelay = 0;
  private double lastTotalLatency = 0;
  private long lastTotalOperations = 0;

  public NBLruPolicy(Config config) {
    var settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());

    this.requests = new ArrayDeque<>();
    currentTime = 0;

    this.scoreMinHeap = new SearchableMinHeap<>((int) settings.maximumSize(), this::comparePrefixes);
    this.source = new LogNormalSource(Consts.SOURCE_KEY, Consts.SOURCE_MEAN, Consts.SOURCE_STD);

    this.maximumCacheSize = settings.maximumSize();
    this.currentCacheSize = 0;

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
    appendRequestStatsToCsv();
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
    long itemKey = event.key();
    var existingPrefix = scoreMinHeap.get(itemKey);
    if (existingPrefix != null) {
      // prefix exist (partial hit)
      existingPrefix.lastRequestTime = currentTime;
      onRequest(existingPrefix, event.retrievalDelay(), event.operation());
    } else {
      // prefix missing (full miss)
      long currentSize = event.itemSize();
      var newPrefix = new Prefix(itemKey, currentSize, source, currentTime);
      onRequest(newPrefix, event.retrievalDelay(), event.operation());
    }
  }

  private void onRequest(Prefix prefix, double sourceDelay, AccessEvent.Operation operation) {
    if (operation == READ) recordRequestStatistics(prefix, sourceDelay);
    handleRequestsFrequency(prefix);
    waterFill(prefix);
  }

  private void handleRequestsFrequency(Prefix prefix) {
    prefix.requestsCountInPeriod++;

    requests.add(prefix.itemKey);
    if (requests.size() == Consts.REQUESTS_FREQUENCY_PERIOD + 1) {
      long lastRequestItemKey = requests.remove();
      var lastRequestedPrefix = scoreMinHeap.get(lastRequestItemKey);

      if (lastRequestedPrefix != null) {
        lastRequestedPrefix.requestsCountInPeriod--;
      }
    }
  }

  private void recordRequestStatistics(Prefix old, double sourceDelay) {
    // Total delay
    double underflowDelay = calculateUnderflowDelay(sourceDelay, old);
    policyStats.addDelay(underflowDelay);
    double latency = calculateLatency(sourceDelay, old.fullItemSize(), old.currentSize(), Consts.BANDWIDTH);
    policyStats.addLatency(latency);
  }

  private void waterFill(Prefix prefix) {
    long addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    while (!prefix.isFull() && // stop if the prefix is full
      currentCacheSize + addSize <= maximumCacheSize // stop if adding another chunk would exceed the maximum cache size
    ) {
      extendPrefix(prefix);
      addSize = Math.min(prefix.fullItemSize - prefix.currentSize, Consts.CHUNK_SIZE);
    }

    if (prefix.isFull() || maximumCacheSize == 0) return;

    Prefix victim;
    do {
      extendPrefix(prefix);
      do {
        victim = findVictim();
        shrinkPrefix(victim);
      } while (currentCacheSize > maximumCacheSize);

    } while (!(prefix.isFull() || victim.itemKey == prefix.itemKey));

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

    assert prefix.currentSize >= 0 : "Prefix size cannot be negative";
    assert currentCacheSize >= 0 : "Current cache size cannot be negative";
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
  private static double calculateUnderflowDelay(double sourceDelay, Prefix prefix) {
    return TimeCalculations.calculateUnderflowDelay(
      sourceDelay,
      prefix.fullItemSize(),
      prefix.currentSize(),
      Consts.BANDWIDTH
    );
  }

  public int comparePrefixes(long prefixKey1, long prefixKey2) {
    Prefix p1 = scoreMinHeap.get(prefixKey1);
    Prefix p2 = scoreMinHeap.get(prefixKey2);
    if (p1 == null || p2 == null) {
      throw new IllegalArgumentException("Prefixes not found in the heap");
    }
    return p1.lruCompareTo(p2);
  }

  private void appendRequestStatsToCsv() {
    double currentDelay = policyStats.totalDelay();
    double currentLatency = policyStats.totalLatency();
    long currentOperations = policyStats.operationCount();

    double deltaDelay = currentDelay - lastTotalDelay;
    double deltaLatency = currentLatency - lastTotalLatency;
    long deltaOperations = currentOperations - lastTotalOperations;

    try {
      csvWriter.write(deltaDelay + "," + deltaLatency + "," + deltaOperations);
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
    try {
      csvWriter.flush();
      csvWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
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
    long requestsCountInPeriod;
    long lastRequestTime;

    public Prefix(long itemKey, long fullItemSize, Source source, long currentTime) {
      this.itemKey = itemKey;
      this.fullItemSize = fullItemSize;
      this.source = source;
      this.requestsCountInPeriod = 0;
      this.lastRequestTime = currentTime;
      this.currentSize = 0;
    }

    public double lruScore() {
      // Idea - recency times the probability of not experiencing delay
      double prefixTransmissionTime = TimeCalculations.calculateTransmissionTime(
        currentSize(),
        Consts.BANDWIDTH
      );
      return recency() * (1 - source.calculateCDF(prefixTransmissionTime));
    }

    public double recency() {
      return (double) 1 / (currentTime - lastRequestTime + 1);
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

    public int lruCompareTo(Prefix other) {
      return Double.compare(this.lruScore(), other.lruScore());
    }
  }
}
