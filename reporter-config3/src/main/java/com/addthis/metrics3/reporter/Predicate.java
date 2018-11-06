package com.addthis.metrics3.reporter;

public interface Predicate<T> {

    boolean test(T t);
}
