package com.github.benmanes.caffeine.cache.simulator.policy.prefix.competitor;

import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.typesafe.config.Config;

@Policy.PolicySpec(name = "prefix.competitor.SBatch.DP.Monitored")
public final class DPSBatchMonitoredPolicy extends AbstractDPSBatchPolicy {
  public DPSBatchMonitoredPolicy(Config config) {
    super(config, Mode.MONITORED, "prefix.competitor.SBatch.DP.Monitored");
  }
}
