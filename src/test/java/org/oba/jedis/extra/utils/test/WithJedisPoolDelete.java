package org.oba.jedis.extra.utils.test;

import redis.clients.jedis.JedisPool;

import java.util.List;

public final class WithJedisPoolDelete {

    private WithJedisPoolDelete() {
        // Prevent instantiation
    }

    public static void doDelete(JedisPool jedisPool, String key) {
        if (jedisPool == null || key == null || key.isEmpty()) {
            throw new IllegalArgumentException("JedisPool and key must not be null or empty");
        }
        try (var jedis = jedisPool.getResource()) {
            jedis.del(key);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to delete key: " + key, e);
        }
    }

    public static void doDelete(JedisPool jedisPool, List<String> keys) {
        if (jedisPool == null || keys == null) {
            throw new IllegalArgumentException("JedisPool and keys must not be null or empty");
        }
        try (var jedis = jedisPool.getResource()) {
            keys.forEach( jedis::del);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to delete keys: " + keys, e);
        }
    }

}
