package com.github.benmanes.caffeine.cache.simulator.policy.prefix.competitor;

import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.typesafe.config.Config;

@Policy.PolicySpec(name = "prefix.competitor.SBatch.Greedy.Monitored")
public final class GreedySBatchMonitoredPolicy extends AbstractGreedySBatchPolicy {
  public GreedySBatchMonitoredPolicy(Config config) {
    super(config, Mode.MONITORED, "prefix.competitor.SBatch.Greedy.Monitored");
  }
}
