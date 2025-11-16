package com.github.benmanes.caffeine.cache.simulator.policy;

import com.typesafe.config.Config;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.processing.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Registry_Factory extends Registry.Factory {

  private final Class<? extends Policy> policyClass;

  private final Function<Config, Set<Policy>> creator;

  AutoValue_Registry_Factory(
      Class<? extends Policy> policyClass,
      Function<Config, Set<Policy>> creator) {
    if (policyClass == null) {
      throw new NullPointerException("Null policyClass");
    }
    this.policyClass = policyClass;
    if (creator == null) {
      throw new NullPointerException("Null creator");
    }
    this.creator = creator;
  }

  @Override
  Class<? extends Policy> policyClass() {
    return policyClass;
  }

  @Override
  Function<Config, Set<Policy>> creator() {
    return creator;
  }

  @Override
  public String toString() {
    return "Factory{"
        + "policyClass=" + policyClass + ", "
        + "creator=" + creator
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Registry.Factory) {
      Registry.Factory that = (Registry.Factory) o;
      return this.policyClass.equals(that.policyClass())
          && this.creator.equals(that.creator());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= policyClass.hashCode();
    h$ *= 1000003;
    h$ ^= creator.hashCode();
    return h$;
  }

}
