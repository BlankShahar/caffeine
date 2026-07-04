package com.github.benmanes.caffeine.cache.simulator.policy.prefix.competitor;

import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.typesafe.config.Config;

@Policy.PolicySpec(name = "prefix.competitor.SBatch.Greedy.Oracle")
public final class GreedySBatchOraclePolicy extends AbstractGreedySBatchPolicy {
  public GreedySBatchOraclePolicy(Config config) {
    super(config, Mode.ORACLE, "prefix.competitor.SBatch.Greedy.Oracle");
  }
}
