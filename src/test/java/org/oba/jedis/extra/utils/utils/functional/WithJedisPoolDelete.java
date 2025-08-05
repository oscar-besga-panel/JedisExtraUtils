package org.oba.jedis.extra.utils.utils.functional;

import redis.clients.jedis.JedisPool;

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
            throw new RuntimeException("Failed to delete key: " + key, e);
        }
    }

}
