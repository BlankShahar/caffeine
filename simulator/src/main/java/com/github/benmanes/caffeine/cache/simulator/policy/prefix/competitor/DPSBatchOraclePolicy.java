package com.github.benmanes.caffeine.cache.simulator.policy.prefix.competitor;

import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.typesafe.config.Config;

@Policy.PolicySpec(name = "prefix.competitor.SBatch.DP.Oracle")
public final class DPSBatchOraclePolicy extends AbstractDPSBatchPolicy {
  public DPSBatchOraclePolicy(Config config) {
    super(config, Mode.ORACLE, "prefix.competitor.SBatch.DP.Oracle");
  }
}
