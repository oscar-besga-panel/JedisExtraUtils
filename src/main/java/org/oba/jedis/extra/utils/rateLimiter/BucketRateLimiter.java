package org.oba.jedis.extra.utils.rateLimiter;

import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * See reference on
 * * bucket4j (https://bucket4j.com/)
 * * https://vbukhtoyarov-java.blogspot.com/2021/11/non-formal-overview-of-token-bucket.html
 *
 */
public class BucketRateLimiter implements JedisPoolUser {

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketRateLimiter.class);


    enum Mode {
        GREEDY, INTERVAL;
    }

    public final static String CAPACITY = "capacity";
    public final static String AVAILABLE = "available";
    public final static String REFILL_MICROS = "refill_micros";
    public final static String MODE = "mode";
    public final static String LAST_REFILL_MICROS = "last_refill_micros";

    private final JedisPool jedisPool;
    private final String name;

    public BucketRateLimiter(JedisPool jedisPool, String name) {
        this.jedisPool = jedisPool;
        this.name = name;
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void create(long capacity, Mode mode, long timeToRefill, TimeUnit timeUnit) {
        withJedisPoolDo( jedis -> {
            List<String> tmp = jedis.time();
            double timeToRefillMicros = timeUnit.toMillis(timeToRefill) * 1000.0;
            double redisTimestampMicros = (Long.parseLong(tmp.get(0)) * 1000.0) + Long.parseLong(tmp.get(1));
            Map<String, String> internalData = new HashMap<>();
            internalData.put(CAPACITY, Long.toString(capacity));
            internalData.put(AVAILABLE, Long.toString(capacity));
            internalData.put(REFILL_MICROS, Double.toString(timeToRefillMicros));
            internalData.put(MODE, mode.name().toLowerCase());
            internalData.put(LAST_REFILL_MICROS, Double.toString(redisTimestampMicros));
            jedis.hset(name, internalData);
            LOGGER.debug("created with capacity {} mode {} timeToRefill {} timeUnit {} in redisTimestampMicros {}",
                    capacity, mode, timeToRefill, timeUnit, redisTimestampMicros);
        });
    }

    public boolean acquire() {
        return acquire(1L);
    }


    public boolean acquire(long permits) {
        Object result = withJedisPoolGet(jedis -> {
            return jedis.eval(ACQUIRE, Collections.singletonList(name), Collections.singletonList(Long.toString(permits)));
        });
        return Boolean.parseBoolean(result.toString());
    }



    public static final String ACQUIRE = "" +
            "local name = KEYS[1]" + "\n" +
            "local permits = ARGS[1]"  + "\n" +
        "redis.log(redis.LOG_WARNING, 'name ' .. name .. ' permits ' .. permits)"  + "\n" +
        "redis.call(ECHO, 'name ' .. name .. ' permits ' .. permits)"  + "\n" +
            "-- refill" + "\n" +
            "local tst = redis.call('time')" + "\n" +
            "local ts = tst[0] * 1000000 + tst[1]" + "\n" +
            "local refill = false" + "\n" +
            "local mode = redis.call('hget', name, 'mode')"  + "\n" +
        "redis.log(redis.LOG_WARNING, 'ts ' .. ts .. ' mode ' .. mode)"  + "\n" +
        "redis.call(ECHO, 'ts ' .. ts .. ' mode ' .. mode)"  + "\n" +

            "" + "\n" +
            "-- try acquire" + "\n" +
            "local acquire = false" + "\n" +
            "return acquire";

    /**
     * https://redis.io/docs/manual/programmability/lua-debugging/
     * https://redis.com/blog/5-6-7-methods-for-tracing-and-debugging-redis-lua-scripts/
     */

/*
    synchronized public boolean tryConsume(int numberTokens) {
        refill();
        if (availableTokens < numberTokens) {
            return false;
        } else {
            availableTokens -= numberTokens;
            return true;
        }
    }

    private void refill() {
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis > lastRefillTimestamp) {
            long millisSinceLastRefill = currentTimeMillis - lastRefillTimestamp;
            double refill = millisSinceLastRefill * refillTokensPerOneMillis;
            this.availableTokens = Math.min(capacity, availableTokens + refill);
            this.lastRefillTimestamp = currentTimeMillis;
        }
    }
 */



}
