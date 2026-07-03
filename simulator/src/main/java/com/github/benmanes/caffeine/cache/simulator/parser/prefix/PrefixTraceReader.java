/*
 * Copyright 2019 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.caffeine.cache.simulator.parser.prefix;

import com.github.benmanes.caffeine.cache.simulator.parser.TextTraceReader;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic;
import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.stream.Stream;

public final class PrefixTraceReader extends TextTraceReader {

  public PrefixTraceReader(String filePath) {
    super(filePath);
  }

  @Override
  public Set<Characteristic> characteristics() {
    return ImmutableSet.of();
  }

  @Override
  public Stream<AccessEvent> events() {
    return lines()
      .filter(line -> !line.isBlank())
      .filter(line -> !line.startsWith("#"))
      .map(line -> line.split(","))
      .filter(array -> !isHeader(array))
      .map(this::parse);
  }

  private boolean isHeader(String[] array) {
    if (array.length == 0) {
      return false;
    }
    var first = array[0].trim().toLowerCase();
    return first.equals("item_id") || first.equals("timestamp");
  }

  private AccessEvent parse(String[] array) {
    if (array.length == 4) {
      long key = Long.parseLong(array[0].trim());
      int operation = Integer.parseInt(array[1].trim());
      long itemSize = Long.parseLong(array[2].trim());
      double retrievalDelay = Double.parseDouble(array[3].trim());

      return AccessEvent.forKeyAndOperationAndSizeAndDelay(
        key, operation, itemSize, retrievalDelay);
    }

    if (array.length == 5) {
      long key = Long.parseLong(array[0].trim());
      int operation = Integer.parseInt(array[1].trim());
      long itemSize = Long.parseLong(array[2].trim());
      double retrievalDelay = Double.parseDouble(array[3].trim());
      long timestamp = parseTimestamp(array[4]);

      return AccessEvent.forKeyAndOperationAndSizeAndDelayAndTimestamp(
        key, operation, itemSize, retrievalDelay, timestamp);
    }

    if (array.length == 6) {
      long key = Long.parseLong(array[0].trim());
      int operation = Integer.parseInt(array[1].trim());
      long itemSize = Long.parseLong(array[2].trim());
      double retrievalDelay = Double.parseDouble(array[3].trim());
      long timestamp = parseTimestamp(array[4]);
      double lambda = Double.parseDouble(array[5].trim());

      return AccessEvent.forKeyAndOperationAndSizeAndDelayAndTimestampAndLambda(
        key, operation, itemSize, retrievalDelay, timestamp, lambda);
    }

    throw new IllegalArgumentException(
      "Invalid prefix trace row; expected 4, 5, or 6 columns but got " + array.length);
  }

  private static long parseTimestamp(String value) {
    return (long) Double.parseDouble(value.trim());
  }
}
