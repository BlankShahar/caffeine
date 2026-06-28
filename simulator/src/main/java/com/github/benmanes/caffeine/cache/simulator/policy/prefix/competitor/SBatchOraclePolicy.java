package com.github.benmanes.caffeine.cache.simulator.policy.prefix.competitor;

import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.typesafe.config.Config;

@Policy.PolicySpec(name = "prefix.competitor.SBatch.Oracle")
public final class SBatchOraclePolicy extends AbstractSBatchPolicy {
  public SBatchOraclePolicy(Config config) {
    super(config, Mode.ORACLE, "prefix.competitor.SBatch.Oracle");
  }
}
