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
package com.github.benmanes.caffeine.cache.simulator.policy.product;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.PolicySpec;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.Consts;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.TimeCalculations;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.NormalSource;
import com.github.benmanes.caffeine.cache.simulator.policy.non_binary.sources.Source;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.typesafe.config.Config;

import java.util.HashMap;
import java.util.Set;

import static com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic.WEIGHTED;

/**
 * Guava cache implementation.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@PolicySpec(name = "product.Guava", characteristics = WEIGHTED)
public final class GuavaPolicy implements Policy {
  private final Cache<Long, AccessEvent> cache;
  private final PolicyStats policyStats;

  private final Source source;
  private final HashMap<Long, Source> itemToSource;

  public GuavaPolicy(Config config, Set<Characteristic> characteristics) {
    policyStats = new PolicyStats(name());
    var settings = new BasicSettings(config);
    CacheBuilder<Long, AccessEvent> builder = CacheBuilder.newBuilder()
      .removalListener(notification -> policyStats.recordEviction());
    if (characteristics.contains(WEIGHTED)) {
      builder.maximumWeight(settings.maximumSize());
      builder.weigher((key, value) -> value.weight());
    } else {
      builder.maximumSize(settings.maximumSize());
    }
    cache = builder.build();

    source = new NormalSource(1, 0.003, 0.00075);
    itemToSource = new HashMap<>();
  }

  @Override
  public void record(AccessEvent event) {
    long key = event.key();
    AccessEvent value = cache.getIfPresent(key);
    double itemSize = Consts.ITEM_CHUNKS_AMOUNT * Consts.CHUNK_SIZE;
    if (!itemToSource.containsKey(key)) {
      itemToSource.put(event.key(), source);
    }

    if (value == null) {
      cache.put(event.key(), event);
      policyStats.recordWeightedMiss(event.weight());

      double sourceProcessingTime = itemToSource.get(key).sampleProcessingTime();
      policyStats.addLatency(TimeCalculations.calculateSourceLatency(sourceProcessingTime, itemSize, Consts.BANDWIDTH));
      policyStats.addDelay(sourceProcessingTime);
    } else {
      policyStats.recordWeightedHit(event.weight());
      policyStats.addLatency(TimeCalculations.calculateTransmissionTime(itemSize, Consts.BANDWIDTH));

      if (event.weight() != value.weight()) {
        cache.put(event.key(), event);
      }
    }
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }
}
