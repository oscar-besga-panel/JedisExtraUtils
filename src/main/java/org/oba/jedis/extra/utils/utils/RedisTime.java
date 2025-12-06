package org.oba.jedis.extra.utils.utils;

import redis.clients.jedis.JedisPooled;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to call Redis TIME command via Lua script
 * As Jedis pooled does not have a direct method to call TIME command,
 */
public class RedisTime {

    private final JedisPooled jedisPooled;

    public RedisTime (JedisPooled jedisPooled) {
        this.jedisPooled = jedisPooled;
    }

    public List<String> callTime() {
        Object result = jedisPooled.eval("return redis.call('TIME')");
        if (result instanceof List && ((List<String>) result).size() == 2) {
            return  (List<String>) result;
        } else {
            throw new IllegalStateException("TIME command did not return a valid list");
        }
    }

    public BigInteger callTimeInSeconds() {
        List<String> timeParts = callTime();
        long seconds = Long.parseLong(timeParts.get(0));
        return BigInteger.valueOf(seconds);
    }

    public BigInteger callTimeInMillis() {
        List<String> timeParts = callTime();
        long seconds = Long.parseLong(timeParts.get(0));
        long micros = Long.parseLong(timeParts.get(1));
        return BigInteger.valueOf((seconds * 1_000L) + (micros / 1_000L));
    }

    public BigInteger callTimeInMicros() {
        List<String> timeParts = callTime();
        long seconds = Long.parseLong(timeParts.get(0));
        long micros = Long.parseLong(timeParts.get(1));
        return BigInteger.valueOf(seconds * 1_000_000L + micros);
    }

    public BigInteger callTimeInNanos() {
        List<String> timeParts = callTime();
        long seconds = Long.parseLong(timeParts.get(0));
        long micros = Long.parseLong(timeParts.get(1));
        return BigInteger.valueOf((seconds * 1_000_000_000L) + (micros * 1_000L));
    }

}
