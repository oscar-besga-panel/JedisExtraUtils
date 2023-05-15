package org.oba.jedis.extra.utils.rateLimiter;

import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.UniversalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.oba.jedis.extra.utils.rateLimiter.CommonRateLimiter.*;

/**
 * See reference on
 * * bucket4j (https://bucket4j.com/)
 * * https://vbukhtoyarov-java.blogspot.com/2021/11/non-formal-overview-of-token-bucket.html
 *
 */
public class BucketRateLimiter implements JedisPoolUser {

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketRateLimiter.class);

    public static final String SCRIPT_NAME = "bucketRateLimiter.lua";
    public static final String FILE_PATH = "./src/main/resources/bucketRateLimiter.lua";

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
    private final ScriptEvalSha1 script;

    public BucketRateLimiter(JedisPool jedisPool, String name) {
        this.jedisPool = jedisPool;
        this.name = name;
        this.script = new ScriptEvalSha1(jedisPool, new UniversalReader().
                withResoruce(SCRIPT_NAME).
                withFile(FILE_PATH));
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public boolean exists() {
        return withJedisPoolGet( jedis -> jedis.exists(name));
    }

    public BucketRateLimiter create(long capacity, Mode mode, long timeToRefillMillis) {
        this.create(capacity, mode, timeToRefillMillis, TimeUnit.MILLISECONDS);
        return this;
    }

    public BucketRateLimiter create(long capacity, Mode mode, long timeToRefill, TimeUnit timeUnit) {
        withJedisPoolDo( jedis ->
            createWithJedis(jedis, capacity, mode, timeToRefill, timeUnit)
        );
        return this;
    }

    private void createWithJedis(Jedis jedis, long capacity, Mode mode, long timeToRefill, TimeUnit timeUnit) {
        if (!jedis.exists(name)) {
            BigInteger timeToRefillMicros = toRedisMicros(timeToRefill, timeUnit);
            BigInteger redisTimestampMicros = fromRedisTimestampAsMicros(jedis);
            Map<String, String> internalData = new HashMap<>();
            internalData.put(CAPACITY, Long.toString(capacity));
            internalData.put(AVAILABLE, Long.toString(capacity));
            internalData.put(REFILL_MICROS, timeToRefillMicros.toString());
            internalData.put(MODE, mode.name().toLowerCase());
            internalData.put(LAST_REFILL_MICROS, redisTimestampMicros.toString());
            jedis.hset(name, internalData);
            LOGGER.debug("created with capacity {} mode {} timeToRefill {} timeUnit {} in redisTimestampMicros {}",
                    capacity, mode, timeToRefill, timeUnit, redisTimestampMicros);
        }
    }


    public boolean acquire() {
        return acquire(1L);
    }



    public boolean acquire(long permits) {
        Object result = script.evalSha(Collections.singletonList(name), Collections.singletonList(Long.toString(permits)));
        LOGGER.debug("permits {} result {}", permits, result);
        return scriptResultAsBoolean(result);
    }


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
    public static void main(String[] args) {

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(24); // original 128
        jedisPoolConfig.setMaxIdle(24); // original 128
        jedisPoolConfig.setMinIdle(4); // original 16
        // High performance
//        jedisPoolConfig.setMaxTotal(128);
//        jedisPoolConfig.setMaxIdle(128);
//        jedisPoolConfig.setMinIdle(16);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPoolConfig.setTestWhileIdle(true);
        jedisPoolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(30).toMillis());
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(10).toMillis());
        jedisPoolConfig.setNumTestsPerEvictionRun(1);
        jedisPoolConfig.setBlockWhenExhausted(true);
        String host = "127.0.0.1";
        int port = 6379;
        String pass = "";
        JedisPool jedisPool = null;
        if (pass != null && !pass.trim().isEmpty()) {
            jedisPool = new JedisPool(jedisPoolConfig, host, port, Protocol.DEFAULT_TIMEOUT, pass);
        } else {
            jedisPool = new JedisPool(jedisPoolConfig, host, port);
        }


        BucketRateLimiter bucketRateLimiter = new BucketRateLimiter(jedisPool, "bucketRateLimiter:test_" + System.currentTimeMillis());
        bucketRateLimiter.create(10, Mode.GREEDY, 1, TimeUnit.SECONDS);
        boolean result1 = bucketRateLimiter.acquire();
        boolean result100 = bucketRateLimiter.acquire(100);

        System.out.println("Acquire result " + result1);
        System.out.println("Acquire result " + result100);
        System.out.println("-");
        //System.out.println(ACQUIRE);
    }



}
