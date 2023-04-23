package org.oba.jedis.extra.utils.rateLimiter;

import org.oba.jedis.extra.utils.utils.JedisPoolUser;
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

    public void create(long capacity, Mode mode, long timeToRefillMillis) {
        this.create(capacity, mode, timeToRefillMillis, TimeUnit.MILLISECONDS);
    }

    public void create(long capacity, Mode mode, long timeToRefill, TimeUnit timeUnit) {
        withJedisPoolDo( jedis -> {
            createWithJedis(jedis, capacity, mode, timeToRefill, timeUnit);
        });
    }

    public static final BigInteger BI_MILLION = BigInteger.valueOf(1_000_000L);
    public static final BigInteger BI_THOUSAND = BigInteger.valueOf(1_000L);



    private void createWithJedis(Jedis jedis, long capacity, Mode mode, long timeToRefill, TimeUnit timeUnit) {
        if (!jedis.exists(name)) {
            List<String> tmp = jedis.time();
            BigInteger timeToRefillMicros = BigInteger.valueOf(timeUnit.toMillis(timeToRefill)).multiply(BI_THOUSAND);
            BigInteger redisTimestampMicros = BigInteger.valueOf(Long.parseLong(tmp.get(0))).multiply(BI_MILLION).
                    add(BigInteger.valueOf(Long.parseLong(tmp.get(1))));
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

    public boolean exists() {
        return withJedisPoolGet( j -> j.exists(name));
    }

    public boolean acquire() {
        return acquire(1L);
    }

    public static final Long LUA_1_TRUE = Long.valueOf(1L);
    public static final String LUA_1_TRUE_STRING = "1";


    public boolean acquire(long permits) {
        Object result = withJedisPoolGet(jedis ->
                jedis.eval(ACQUIRE, Collections.singletonList(name), Collections.singletonList(Long.toString(permits)))
        );
        if (result == null) {
            return false;
        } else if (result.equals(LUA_1_TRUE) || result.equals(LUA_1_TRUE_STRING)) {
            return true;
        } else {
            return Boolean.parseBoolean(result.toString());
        }
    }



    public static final String ACQUIRE = "" +
            "" + "\n" +
            "local name = KEYS[1]" + "\n" +
            "local permits = tonumber(ARGV[1])"  + "\n" +
        "redis.log(redis.LOG_WARNING, 'name ' .. name .. ' permits ' .. permits)"  + "\n" +
        "redis.call('ECHO', 'name ' .. name .. ' permits ' .. permits)"  + "\n" +
            "-- refill" + "\n" +
            "local tst = redis.call('time')" + "\n" +
            "local ts = tst[1] * 1000000 + tst[2]" + "\n" +
            "local refill = false" + "\n" +
            "local mode = redis.call('hget', name, 'mode')"  + "\n" +
        "redis.log(redis.LOG_WARNING, 'ts ' .. ts .. ' mode ' .. mode)"  + "\n" +
        "redis.call('ECHO', 'ts ' .. ts .. ' mode ' .. mode)"  + "\n" +
            "if refill then"  + "\n" +
        "redis.log(redis.LOG_WARNING, 'refill ok ')"  + "\n" +
        "redis.call('ECHO', 'refill ok')"  + "\n" +
            "end" + "\n" +
            "-- try acquire" + "\n" +
            "local acquire = false" + "\n" +
            "local available = tonumber(redis.call('hget', name, 'available'))" + "\n" +
          "redis.log(redis.LOG_WARNING, 'available ' .. available .. ' permits ' .. permits)"  + "\n" +
          "redis.call('ECHO',  'available ' .. available .. ' permits ' .. permits)"  + "\n" +
            "if (available >= permits) then" + "\n" +
            "  available = available - permits" + "\n" +
            "  redis.call('hset', name, 'available', available)" + "\n" +
            "  acquire = true" + "\n" +
          "redis.log(redis.LOG_WARNING, 'available ' .. available .. ' acquire ' .. tostring(acquire))"  + "\n" +
          "redis.call('ECHO',  'available ' .. available .. ' acquire ' .. tostring(acquire))"  + "\n" +
            "end" + "\n" +
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
    }



}
