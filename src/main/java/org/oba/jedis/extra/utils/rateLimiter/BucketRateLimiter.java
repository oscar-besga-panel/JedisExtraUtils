package org.oba.jedis.extra.utils.rateLimiter;

import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.oba.jedis.extra.utils.utils.Named;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.UniversalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.oba.jedis.extra.utils.rateLimiter.CommonRateLimiter.fromRedisTimestampAsMicros;
import static org.oba.jedis.extra.utils.rateLimiter.CommonRateLimiter.scriptResultAsBoolean;
import static org.oba.jedis.extra.utils.rateLimiter.CommonRateLimiter.toRedisMicros;

/**
 * See reference on
 * * bucket4j (https://bucket4j.com/)
 * * https://vbukhtoyarov-java.blogspot.com/2021/11/non-formal-overview-of-token-bucket.html
 *
 */
public class BucketRateLimiter implements JedisPoolUser, Named {

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

    public String getName() {
        return name;
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public boolean exists() {
        return withJedisPoolGet( jedis -> jedis.exists(name));
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

    public void delete() {
        withJedisPoolDo( jedis ->
                jedis.del(name)
        );
    }

}
