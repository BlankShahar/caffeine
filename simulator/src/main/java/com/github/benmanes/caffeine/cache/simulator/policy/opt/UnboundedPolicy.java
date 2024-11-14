/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
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
package com.github.benmanes.caffeine.cache.simulator.policy.opt;

import static com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic.WEIGHTED;

import java.util.HashMap;
import java.util.Random;
import java.util.Set;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.PolicySpec;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;
import com.google.common.primitives.Ints;
import com.typesafe.config.Config;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * A cache that has no maximum size. This demonstrates the upper bound of the hit rate due to
 * compulsory misses (first reference misses), which can only be avoided if the application can
 * intelligently prefetch the data prior to the request.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@PolicySpec(name = "opt.Unbounded", characteristics = WEIGHTED)
public final class UnboundedPolicy implements Policy {
  private final PolicyStats policyStats;
  private final LongSet data;

  private final Random sourcePicker;
  private final HashMap<Long, Source> itemToSource;

  public UnboundedPolicy(Config config, Set<Characteristic> characteristics) {
    var settings = new BasicSettings(config);
    int initialSize = characteristics.contains(WEIGHTED)
      ? LongOpenHashSet.DEFAULT_INITIAL_SIZE
      : Ints.saturatedCast(settings.maximumSize());
    data = new LongOpenHashSet(initialSize);
    policyStats = new PolicyStats(name());

    sourcePicker = new Random(Consts.SOURCE_PICKER_SEED);
    itemToSource = new HashMap<>();
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  @Override
  public void record(AccessEvent event) {
    policyStats.recordOperation();
    long key = event.key();

    if (!itemToSource.containsKey(key)) {
      int sourceKey = sourcePicker.nextInt(Consts.REAL_SOURCES.size());
      Source source = Consts.REAL_SOURCES.get(sourceKey);
      itemToSource.put(event.key(), source);
    }

    double itemSize = Consts.ITEM_CHUNKS_AMOUNT * Consts.CHUNK_SIZE;
    if (data.add(key)) {
      policyStats.recordWeightedMiss(event.weight());

      double realSourceProcessingTime = itemToSource.get(key).getNextProcessingTime();
      policyStats.addLatency(TimeCalculations.calculateSourceLatency(realSourceProcessingTime, itemSize, Consts.BANDWIDTH));
      policyStats.addDelay(realSourceProcessingTime);
    } else {
      policyStats.recordWeightedHit(event.weight());
      policyStats.addLatency(TimeCalculations.calculateTransmissionTime(itemSize, Consts.BANDWIDTH));
    }
  }
}
