package com.github.benmanes.caffeine.cache.simulator.admission.countmin4;

import static com.google.common.base.Preconditions.checkArgument;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.Frequency;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import com.typesafe.config.Config;

import java.util.HashMap;
import java.util.Map;

/**
 * A probabilistic multiset for estimating the popularity of an element within a time window. The
 * maximum frequency of an element is limited to 15 (4-bits) and extensions provide the aging
 * process.
 *
 * Refactored to use a HashMap<Integer, Long> instead of long[] or ArrayList<Long>
 * to avoid allocating a huge array when maximumSize is extremely large.
 *
 * The key is the index in the virtual table, and the value is a long with 16 counters (4 bits each).
 *
 * @author ben.manes
 */
public abstract class CountMin4 implements Frequency {
  static final long[] SEED = { // A mixture of seeds from FNV-1a, CityHash, and Murmur3
    0xc3a5c85c97cb3127L, 0xb492b66fbe98f273L, 0x9ae16a3b2f90404fL, 0xcbf29ce484222325L};
  static final long RESET_MASK = 0x7777777777777777L;

  protected final boolean conservative;

  protected int tableMask;
  protected Map<Integer, Long> table; // Changed to HashMap
  protected int step = 1;

  /**
   * Creates a frequency sketch that can accurately estimate the popularity of elements given
   * the maximum size of the cache.
   */
  @SuppressWarnings({"this-escape", "Varifier"})
  protected CountMin4(Config config) {
    var settings = new BasicSettings(config);
    conservative = settings.tinyLfu().conservative();

    double countersMultiplier = settings.tinyLfu().countMin4().countersMultiplier();
    long counters = (long) (countersMultiplier * settings.maximumSize());
    ensureCapacity(counters);
  }

  /**
   * Ensures the logical capacity is enough, but no actual large allocation is done.
   *
   * @param maximumSize the maximum size of the cache
   */
  @SuppressWarnings("Varifier")
  protected void ensureCapacity(long maximumSize) {
    checkArgument(maximumSize >= 0);
    int maximum = (int) Math.min(maximumSize, Integer.MAX_VALUE >>> 1);
    int capacity = (maximum == 0) ? 1 : nextPowerOfTwo(maximum);

    if (table == null) {
      table = new HashMap<>();
    }
    tableMask = Math.max(0, capacity - 1);
  }

  /**
   * Returns the estimated number of occurrences of an element, up to the maximum (15).
   *
   * @param e the element to count occurrences of
   * @return the estimated number of occurrences of the element; possibly zero but never negative
   */
  @Override
  @SuppressWarnings("Varifier")
  public int frequency(long e) {
    int hash = spread(Long.hashCode(e));
    int start = (hash & 3) << 2;
    @Var int frequency = Integer.MAX_VALUE;
    for (int i = 0; i < 4; i++) {
      int index = indexOf(hash, i);
      long value = table.getOrDefault(index, 0L);
      int count = (int) ((value >>> ((start + i) << 2)) & 0xfL);
      frequency = Math.min(frequency, count);
    }
    return frequency;
  }

  /**
   * Increments the popularity of the element if it does not exceed the maximum (15).
   *
   * @param e the element to add
   */
  @Override
  public void increment(long e) {
    if (conservative) {
      conservativeIncrement(e);
    } else {
      regularIncrement(e);
    }
  }

  /** Increments all of the associated counters. */
  void regularIncrement(long e) {
    int hash = spread(Long.hashCode(e));
    int start = (hash & 3) << 2;

    int index0 = indexOf(hash, 0);
    int index1 = indexOf(hash, 1);
    int index2 = indexOf(hash, 2);
    int index3 = indexOf(hash, 3);

    @Var boolean added = incrementAt(index0, start, step);
    added |= incrementAt(index1, start + 1, step);
    added |= incrementAt(index2, start + 2, step);
    added |= incrementAt(index3, start + 3, step);

    tryReset(added);
  }

  /** Increments the associated counters that are at the observed minimum. */
  void conservativeIncrement(long e) {
    int hash = spread(Long.hashCode(e));
    int start = (hash & 3) << 2;

    int[] index = new int[4];
    int[] count = new int[4];
    @Var int min = Integer.MAX_VALUE;
    for (int i = 0; i < 4; i++) {
      index[i] = indexOf(hash, i);
      long value = table.getOrDefault(index[i], 0L);
      count[i] = (int) ((value >>> ((start + i) << 2)) & 0xfL);
      min = Math.min(min, count[i]);
    }

    if (min == 15) {
      tryReset(false);
      return;
    }

    for (int i = 0; i < 4; i++) {
      if (count[i] == min) {
        incrementAt(index[i], start + i, step);
      }
    }
    tryReset(true);
  }

  /** Performs the aging process after an addition to allow old entries to fade away. */
  protected abstract void tryReset(boolean added);

  /**
   * Increments the specified counter by 1 if it is not already at the maximum value (15).
   *
   * @param i the table index (16 counters)
   * @param j the counter to increment
   * @param step the increase amount
   * @return if incremented
   */
  @CanIgnoreReturnValue
  boolean incrementAt(int i, int j, long step) {
    int offset = j << 2;
    long mask = (0xfL << offset);
    long value = table.getOrDefault(i, 0L);
    if ((value & mask) != mask) {
      long current = (value & mask) >>> offset;
      long update = Math.min(current + step, 15);
      table.put(i, (value & ~mask) | (update << offset));
      return true;
    }
    return false;
  }

  /**
   * Returns the table index for the counter at the specified depth.
   *
   * @param item the element's hash
   * @param i the counter depth
   * @return the table index
   */
  int indexOf(int item, int i) {
    @Var long hash = (item + SEED[i]) * SEED[i];
    hash += (hash >>> 32);
    return ((int) hash) & tableMask;
  }

  /**
   * Applies a supplemental hash function to a given hashCode, which defends against poor quality
   * hash functions.
   */
  int spread(@Var int x) {
    x = ((x >>> 16) ^ x) * 0x45d9f3b;
    x = ((x >>> 16) ^ x) * 0x45d9f3b;
    return (x >>> 16) ^ x;
  }

  /** Returns the smallest power of two >= value */
  private static int nextPowerOfTwo(int value) {
    return Integer.highestOneBit(value - 1) << 1;
  }
}
