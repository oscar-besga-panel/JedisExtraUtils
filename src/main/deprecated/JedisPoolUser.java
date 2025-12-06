package org.oba.jedis.extra.utils.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.function.Consumer;
import java.util.function.Function;

public interface JedisPoolUser {

    JedisPool getJedisPooled();

    default void withResource(Consumer<Jedis> consumer) {
        try (Jedis jedis = getJedisPooled().getResource()) {
            consumer.accept(jedis);
        }
    }

    default <K> K withResourceGet(Function<Jedis, K> function) {
        try (Jedis jedis = getJedisPooled().getResource()) {
            return function.apply(jedis);
        }
    }

}
