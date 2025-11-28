package org.oba.jedis.extra.utils.rateLimiter;

import redis.clients.jedis.JedisPooled;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

/**
 *
 */
class CommonRateLimiter {

    public static final BigInteger BI_MILLION = BigInteger.valueOf(1_000_000L);
    public static final BigInteger BI_THOUSAND = BigInteger.valueOf(1_000L);

    public static final Long LUA_1_TRUE = Long.valueOf(1L);
    public static final String LUA_1_TRUE_STRING = "1";

    public static BigInteger toRedisMicros(long timeToRefill, TimeUnit timeUnit) {
        return BigInteger.valueOf(timeUnit.toMillis(timeToRefill)).multiply(BI_THOUSAND);
    }

    public static BigInteger fromRedisTimestampAsMicros(JedisPooled jedisPooled) {
        //TODO time
        // BAD FIX
        /*
        List<String> time = jedisPooled.time();
        return BigInteger.valueOf(Long.parseLong(time.get(0))).multiply(BI_MILLION).
                add(BigInteger.valueOf(Long.parseLong(time.get(1))));

         */
        jedisPooled.ping();
        String micros = Long.toString(System.currentTimeMillis() * 1000L);
        return new BigInteger(micros);
    }

    public static boolean scriptResultAsBoolean(Object result) {
        if (result == null) {
            return false;
        } else if (result.equals(LUA_1_TRUE) || result.equals(LUA_1_TRUE_STRING)) {
            return true;
        } else {
            return Boolean.parseBoolean(result.toString());
        }
    }
}
