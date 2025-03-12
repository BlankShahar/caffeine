package com.github.benmanes.caffeine.cache.simulator.policy;

import com.google.common.collect.ImmutableSet;
import javax.annotation.processing.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_PolicyStats_Metric extends PolicyStats.Metric {

  private final String name;

  private final Object value;

  private final PolicyStats.Metric.MetricType type;

  private final boolean required;

  private final ImmutableSet<Policy.Characteristic> characteristics;

  private AutoValue_PolicyStats_Metric(
      String name,
      Object value,
      PolicyStats.Metric.MetricType type,
      boolean required,
      ImmutableSet<Policy.Characteristic> characteristics) {
    this.name = name;
    this.value = value;
    this.type = type;
    this.required = required;
    this.characteristics = characteristics;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Object value() {
    return value;
  }

  @Override
  public PolicyStats.Metric.MetricType type() {
    return type;
  }

  @Override
  public boolean required() {
    return required;
  }

  @Override
  public ImmutableSet<Policy.Characteristic> characteristics() {
    return characteristics;
  }

  @Override
  public String toString() {
    return "Metric{"
        + "name=" + name + ", "
        + "value=" + value + ", "
        + "type=" + type + ", "
        + "required=" + required + ", "
        + "characteristics=" + characteristics
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof PolicyStats.Metric) {
      PolicyStats.Metric that = (PolicyStats.Metric) o;
      return this.name.equals(that.name())
          && this.value.equals(that.value())
          && this.type.equals(that.type())
          && this.required == that.required()
          && this.characteristics.equals(that.characteristics());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= name.hashCode();
    h$ *= 1000003;
    h$ ^= value.hashCode();
    h$ *= 1000003;
    h$ ^= type.hashCode();
    h$ *= 1000003;
    h$ ^= required ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= characteristics.hashCode();
    return h$;
  }

  static final class Builder extends PolicyStats.Metric.Builder {
    private String name;
    private Object value;
    private PolicyStats.Metric.MetricType type;
    private boolean required;
    private ImmutableSet.Builder<Policy.Characteristic> characteristicsBuilder$;
    private ImmutableSet<Policy.Characteristic> characteristics;
    private byte set$0;
    Builder() {
    }
    @Override
    public PolicyStats.Metric.Builder name(String name) {
      if (name == null) {
        throw new NullPointerException("Null name");
      }
      this.name = name;
      return this;
    }
    @Override
    public PolicyStats.Metric.Builder value(Object value) {
      if (value == null) {
        throw new NullPointerException("Null value");
      }
      this.value = value;
      return this;
    }
    @Override
    public PolicyStats.Metric.Builder type(PolicyStats.Metric.MetricType type) {
      if (type == null) {
        throw new NullPointerException("Null type");
      }
      this.type = type;
      return this;
    }
    @Override
    public PolicyStats.Metric.Builder required(boolean required) {
      this.required = required;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    public ImmutableSet.Builder<Policy.Characteristic> characteristicsBuilder() {
      if (characteristicsBuilder$ == null) {
        characteristicsBuilder$ = ImmutableSet.builder();
      }
      return characteristicsBuilder$;
    }
    @Override
    public PolicyStats.Metric build() {
      if (characteristicsBuilder$ != null) {
        this.characteristics = characteristicsBuilder$.build();
      } else if (this.characteristics == null) {
        this.characteristics = ImmutableSet.of();
      }
      if (set$0 != 1
          || this.name == null
          || this.value == null
          || this.type == null) {
        StringBuilder missing = new StringBuilder();
        if (this.name == null) {
          missing.append(" name");
        }
        if (this.value == null) {
          missing.append(" value");
        }
        if (this.type == null) {
          missing.append(" type");
        }
        if ((set$0 & 1) == 0) {
          missing.append(" required");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_PolicyStats_Metric(
          this.name,
          this.value,
          this.type,
          this.required,
          this.characteristics);
    }
  }

}
