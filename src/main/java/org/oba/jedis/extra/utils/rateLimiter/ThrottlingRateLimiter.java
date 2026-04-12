package org.oba.jedis.extra.utils.rateLimiter;

import org.oba.jedis.extra.utils.utils.Named;
import org.oba.jedis.extra.utils.utils.RedisTime;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.UniversalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.UnifiedJedis;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.oba.jedis.extra.utils.rateLimiter.CommonRateLimiter.scriptResultAsBoolean;
import static org.oba.jedis.extra.utils.rateLimiter.CommonRateLimiter.toRedisMicros;

/**
 * Idea form https://bucket4j.com/
 */
public class ThrottlingRateLimiter implements Named {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThrottlingRateLimiter.class);

    public static final String SCRIPT_NAME = "throttlingRateLimiter.lua";
    public static final String FILE_PATH = "./src/main/resources/throttlingRateLimiter.lua";

    public final static String LAST_ALLOW_MICROS = "last_allow_micros";
    public final static String ALLOW_MICROS = "allow_micros";

    private final UnifiedJedis redisClient;
    private final String name;
    private final ScriptEvalSha1 script;


    public ThrottlingRateLimiter(UnifiedJedis redisClient, String name) {
        this.redisClient = redisClient;
        this.name = name;
        this.script = new ScriptEvalSha1(redisClient, new UniversalReader().
                withResoruce(SCRIPT_NAME).
                withFile(FILE_PATH));
    }

    @Override
    public String getName() {
        return name;
    }

    public boolean exists() {
        return redisClient.exists(name);
    }

    public ThrottlingRateLimiter createIfNotExists(long timeToAllowMillis) {
        if (!exists()) {
            return create(timeToAllowMillis, TimeUnit.MILLISECONDS);
        } else {
            return this;
        }
    }

    public ThrottlingRateLimiter create(long timeToAllowMillis) {
        return this.create(timeToAllowMillis, TimeUnit.MILLISECONDS);
    }

    public ThrottlingRateLimiter createIfNotExists(long timeToAllow, TimeUnit timeUnit) {
        if (!exists()) {
            return create(timeToAllow, timeUnit);
        } else {
            return this;
        }
    }

    public ThrottlingRateLimiter create(long timeToAllow, TimeUnit timeUnit) {
        return createWithJedis(timeToAllow, timeUnit);
    }

    private ThrottlingRateLimiter createWithJedis(long timeToAllow, TimeUnit timeUnit) {
        if (!redisClient.exists(name)) {
            RedisTime redisTime = new RedisTime(redisClient);
            BigInteger timeToAllowMicros = toRedisMicros(timeToAllow, timeUnit);
            BigInteger redisTimestampMicros = redisTime.callTimeInMicros();
            Map<String, String> internalData = new HashMap<>();
            internalData.put(ALLOW_MICROS, timeToAllowMicros.toString());
            internalData.put(LAST_ALLOW_MICROS, redisTimestampMicros.toString());
            redisClient.hset(name, internalData);
            LOGGER.debug("created with timeToAllow {} timeUnit {} in redisTimestampMicros {}",
                    timeToAllow, timeUnit, redisTimestampMicros);
        }
        return this;
    }

    public boolean allow() {
        Object result = script.evalSha(Collections.singletonList(name), Collections.emptyList());
        LOGGER.debug("result {}", result);
        return scriptResultAsBoolean(result);
    }

    public void delete() {
        redisClient.del(name);
    }

}
