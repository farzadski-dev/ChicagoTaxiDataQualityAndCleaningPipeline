package dev.farzadski.core.rules;

import java.util.List;

public class RuleEngine<T> {
  private final List<IRule<T>> rules;

  public RuleEngine(List<IRule<T>> rules) {
    this.rules = rules;
  }

  public T apply(T input) {
    T result = input;
    for (IRule<T> rule : rules) {
      result = rule.apply(result);
    }
    return result;
  }
}
