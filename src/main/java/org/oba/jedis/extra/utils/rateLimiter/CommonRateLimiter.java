package org.oba.jedis.extra.utils.rateLimiter;

import redis.clients.jedis.Jedis;

import java.math.BigInteger;
import java.util.List;
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

    public static BigInteger fromRedisTimestampAsMicros(Jedis jedis) {
        List<String> time = jedis.time();
        return BigInteger.valueOf(Long.parseLong(time.get(0))).multiply(BI_MILLION).
                add(BigInteger.valueOf(Long.parseLong(time.get(1))));
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
