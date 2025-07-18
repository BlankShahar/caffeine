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
package com.github.benmanes.caffeine.cache.simulator.parser.non_binary;

import com.github.benmanes.caffeine.cache.simulator.parser.TextTraceReader;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic;
import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.stream.Stream;


public final class NonBinaryTraceReader extends TextTraceReader {

  public NonBinaryTraceReader(String filePath) {
    super(filePath);
  }


  @Override
  public Set<Characteristic> characteristics() {
    return ImmutableSet.of();
  }

  @Override
  public Stream<AccessEvent> events() {
    return lines()
      .map(line -> line.split(",", 3))
      .map(array -> {
        long key = Long.parseLong(array[0]);
        int operation = Integer.parseInt(array[1]);
        long itemSize = Long.parseLong(array[2]);
        double underflowDelay = Double.parseDouble(array[3]);
        return AccessEvent.forKeyAndOperationAndSizeAndDelay(key, operation, itemSize, underflowDelay);
      });
  }
}
