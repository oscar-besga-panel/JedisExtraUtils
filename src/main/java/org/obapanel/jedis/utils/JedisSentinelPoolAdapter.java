package org.obapanel.jedis.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This class creates a new 'pool' of redis connections
 * from a sentinel pool
 *
 * This is an adapter for
 * This class is not, in any way, thread safe.
 * Use with caution
 */
public class JedisSentinelPoolAdapter extends JedisPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisSentinelPoolAdapter.class);

    private static final String CLOSE = "close";

    private final JedisSentinelPool jedisSentinelPool;

    /**
     * Creates a pool from a single connection
     * @param jedisSentinelPool connection to redis
     * @return pool of connection
     */
    public static JedisSentinelPoolAdapter poolFromSentinel(JedisSentinelPool jedisSentinelPool) {
        return new JedisSentinelPoolAdapter(jedisSentinelPool);
    }

    /**
     * Creates a JedisPoolAdapter from a single connection
     * @param jedisSentinelPool
     */
    public JedisSentinelPoolAdapter(JedisSentinelPool jedisSentinelPool) {
        if (jedisSentinelPool == null) {
            throw new IllegalArgumentException("JedisSentinelPool should not be null");
        }
        this.jedisSentinelPool = jedisSentinelPool;
    }

    /**
     * Execute the consumer with the resource
     * @param action Consumer of redis connection
     */
    public void withResource(Consumer<Jedis> action) {
        try(Jedis jedis = getResource()) {
            action.accept(jedis);
        }
    }

    /**
     * Execute the consumer with the resource
     * and gets the result
     * @param action Function of redis connection
     * @return result of function
     */
    public <T> T withResourceFunction(Function<Jedis, T> action) {
        try(Jedis jedis = getResource()) {
            return action.apply(jedis);
        }
    }


    @Override
    public Jedis getResource() {
        return jedisSentinelPool.getResource();
    }

    @Override
    public void returnBrokenResource(Jedis resource) {
        jedisSentinelPool.returnBrokenResource(resource);
    }

    @Override
    public void returnResource(Jedis resource) {
        jedisSentinelPool.returnResource(resource);
    }

    @Override
    public void close() {
        jedisSentinelPool.close();
        super.close();
    }

    @Override
    public void destroy() {
        jedisSentinelPool.destroy();
    }

    @Override
    public int getNumActive() {
        return jedisSentinelPool.getNumActive();
    }

    @Override
    public int getNumIdle() {
        return jedisSentinelPool.getNumIdle();
    }

    @Override
    public int getNumWaiters() {
        return jedisSentinelPool.getNumWaiters();
    }

    @Override
    public void addObjects(int count) {
        jedisSentinelPool.addObjects(count);
        //NOPE
    }



}
