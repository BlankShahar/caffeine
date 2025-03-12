package com.github.benmanes.caffeine.cache.simulator.parser.wikipedia;

import javax.annotation.processing.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_WikipediaTraceReader_Replacement extends WikipediaTraceReader.Replacement {

  private final String search;

  private final String replace;

  AutoValue_WikipediaTraceReader_Replacement(
      String search,
      String replace) {
    if (search == null) {
      throw new NullPointerException("Null search");
    }
    this.search = search;
    if (replace == null) {
      throw new NullPointerException("Null replace");
    }
    this.replace = replace;
  }

  @Override
  String search() {
    return search;
  }

  @Override
  String replace() {
    return replace;
  }

  @Override
  public String toString() {
    return "Replacement{"
        + "search=" + search + ", "
        + "replace=" + replace
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof WikipediaTraceReader.Replacement) {
      WikipediaTraceReader.Replacement that = (WikipediaTraceReader.Replacement) o;
      return this.search.equals(that.search())
          && this.replace.equals(that.replace());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= search.hashCode();
    h$ *= 1000003;
    h$ ^= replace.hashCode();
    return h$;
  }

}
