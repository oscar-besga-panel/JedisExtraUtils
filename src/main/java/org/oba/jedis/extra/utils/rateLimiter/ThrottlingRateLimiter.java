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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.oba.jedis.extra.utils.rateLimiter.CommonRateLimiter.*;

public class ThrottlingRateLimiter implements JedisPoolUser {

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketRateLimiter.class);

    public static final String SCRIPT_NAME = "throttlingRateLimiter.lua";
    public static final String FILE_PATH = "./src/main/resources/throttlingRateLimiter.lua";

    public final static String LAST_ALLOW_MICROS = "last_allow_micros";
    public final static String ALLOW_MICROS = "allow_micros";

    private final JedisPool jedisPool;
    private final String name;
    private final ScriptEvalSha1 script;


    public ThrottlingRateLimiter(JedisPool jedisPool, String name) {
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

    public ThrottlingRateLimiter create(long timeToAllowMillis) {
        return this.create(timeToAllowMillis, TimeUnit.MILLISECONDS);
    }

    public ThrottlingRateLimiter create(long timeToAllow, TimeUnit timeUnit) {
        withJedisPoolDo( jedis ->
                createWithJedis(jedis, timeToAllow, timeUnit)
        );
        return this;
    }

    private void createWithJedis(Jedis jedis, long timeToAllow, TimeUnit timeUnit) {
        if (!jedis.exists(name)) {
            List<String> tmp = jedis.time();
            BigInteger timeToAllowMicros = toRedisMicros(timeToAllow, timeUnit);
            BigInteger redisTimestampMicros = fromRedisTimestampAsMicros(jedis);
            Map<String, String> internalData = new HashMap<>();
            internalData.put(ALLOW_MICROS, timeToAllowMicros.toString());
            internalData.put(LAST_ALLOW_MICROS, redisTimestampMicros.toString());
            jedis.hset(name, internalData);
            LOGGER.debug("created with timeToAllow {} timeUnit {} in redisTimestampMicros {}",
                    timeToAllow, timeUnit, redisTimestampMicros);
        }
    }

    public boolean allow() {
        Object result = script.evalSha(Collections.singletonList(name), Collections.emptyList());
        LOGGER.debug("result {}", result);
        return scriptResultAsBoolean(result);
    }

    public static void main(String[] args) throws InterruptedException {

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

        ThrottlingRateLimiter throttlingRateLimiter = new ThrottlingRateLimiter(jedisPool, "throttlingRateLimiter:test_" + System.currentTimeMillis());
        throttlingRateLimiter.create(1, TimeUnit.SECONDS);
        Thread.sleep(1050);
        boolean result1 = throttlingRateLimiter.allow();
        boolean result100 = throttlingRateLimiter.allow();
        Thread.sleep(1050);
        boolean result1050 = throttlingRateLimiter.allow();
        Thread.sleep(1050);
        boolean result2050 = throttlingRateLimiter.allow();

        System.out.println("Acquire result 1    > " + result1);
        System.out.println("Acquire result 100  > " + result100);
        System.out.println("Acquire result 1050 > " + result1050);
        System.out.println("Acquire result 2050 > " + result2050);
        System.out.println("-");
        //System.out.println(ACQUIRE);
    }


}
