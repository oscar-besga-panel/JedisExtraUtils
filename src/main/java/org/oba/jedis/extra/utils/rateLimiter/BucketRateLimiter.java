package org.oba.jedis.extra.utils.rateLimiter;

import org.oba.jedis.extra.utils.utils.Named;
import org.oba.jedis.extra.utils.utils.RedisTime;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.UniversalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.oba.jedis.extra.utils.rateLimiter.CommonRateLimiter.scriptResultAsBoolean;
import static org.oba.jedis.extra.utils.rateLimiter.CommonRateLimiter.toRedisMicros;

/**
 * See reference on
 * * bucket4j (https://bucket4j.com/)
 * * https://vbukhtoyarov-java.blogspot.com/2021/11/non-formal-overview-of-token-bucket.html
 *
 */
public class BucketRateLimiter implements Named {

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketRateLimiter.class);

    public static final String SCRIPT_NAME = "bucketRateLimiter.lua";
    public static final String FILE_PATH = "./src/main/resources/bucketRateLimiter.lua";

    public enum Mode {
        GREEDY, INTERVAL;
    }

    public final static String CAPACITY = "capacity";
    public final static String AVAILABLE = "available";
    public final static String REFILL_MICROS = "refill_micros";
    public final static String MODE = "mode";
    public final static String LAST_REFILL_MICROS = "last_refill_micros";

    private final JedisPooled jedisPooled;
    private final String name;
    private final ScriptEvalSha1 script;

    public BucketRateLimiter(JedisPooled jedisPooled, String name) {
        this.jedisPooled = jedisPooled;
        this.name = name;
        this.script = new ScriptEvalSha1(jedisPooled, new UniversalReader().
                withResoruce(SCRIPT_NAME).
                withFile(FILE_PATH));
    }

    @Override
    public String getName() {
        return name;
    }

    public boolean exists() {
        return jedisPooled.exists(name);
    }

    public BucketRateLimiter createIfNotExists(long capacity, Mode mode, long timeToRefillMillis) {
        if (!exists()) {
            return create(capacity, mode, timeToRefillMillis, TimeUnit.MILLISECONDS);
        } else {
            return this;
        }
    }

    public BucketRateLimiter create(long capacity, Mode mode, long timeToRefillMillis) {
        this.create(capacity, mode, timeToRefillMillis, TimeUnit.MILLISECONDS);
        return this;
    }

    public BucketRateLimiter createIfNotExists(long capacity, Mode mode, long timeToRefill, TimeUnit timeUnit) {
        if (!exists()) {
            return create(capacity, mode, timeToRefill, timeUnit);
        } else {
            return this;
        }
    }

    public BucketRateLimiter create(long capacity, Mode mode, long timeToRefill, TimeUnit timeUnit) {
        if (!jedisPooled.exists(name)) {
            RedisTime redisTime = new RedisTime(jedisPooled);
            BigInteger timeToRefillMicros = toRedisMicros(timeToRefill, timeUnit);
            BigInteger redisTimestampMicros = redisTime.callTimeInMicros();
            Map<String, String> internalData = new HashMap<>();
            internalData.put(CAPACITY, Long.toString(capacity));
            internalData.put(AVAILABLE, Long.toString(capacity));
            internalData.put(REFILL_MICROS, timeToRefillMicros.toString());
            internalData.put(MODE, mode.name().toLowerCase());
            internalData.put(LAST_REFILL_MICROS, redisTimestampMicros.toString());
            jedisPooled.hset(name, internalData);
            LOGGER.debug("created with capacity {} mode {} timeToRefill {} timeUnit {} in redisTimestampMicros {}",
                    capacity, mode, timeToRefill, timeUnit, redisTimestampMicros);
        }
        return this;
    }

    public boolean acquire() {
        return acquire(1L);
    }

    public boolean acquire(long permits) {
        Object result = script.evalSha(Collections.singletonList(name), Collections.singletonList(Long.toString(permits)));
        LOGGER.debug("permits {} result {}", permits, result);
        return scriptResultAsBoolean(result);
    }

    public void delete() {
        jedisPooled.del(name);
    }

}
