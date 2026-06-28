package com.github.benmanes.caffeine.cache.simulator.policy.prefix.competitor;

import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.typesafe.config.Config;

@Policy.PolicySpec(name = "prefix.competitor.SBatch.Monitored")
public final class SBatchMonitoredPolicy extends AbstractSBatchPolicy {
  public SBatchMonitoredPolicy(Config config) {
    super(config, Mode.MONITORED, "prefix.competitor.SBatch.Monitored");
  }
}
