package org.oba.jedis.extra.utils.utils;

/**
 * https://www.baeldung.com/java-trifunction
 * @param <T>
 * @param <U>
 * @param <V>
 * @param <R>
 */
@FunctionalInterface
public interface TriFunction<T, U, V, R> {

    R apply(T t, U u, V v);

}