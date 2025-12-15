package dev.farzadski.core.rules;

public interface IRule<T> {
  T apply(T input);
}
