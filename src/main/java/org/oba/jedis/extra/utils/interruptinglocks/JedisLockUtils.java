package org.oba.jedis.extra.utils.interruptinglocks;

import org.oba.jedis.extra.utils.utils.JedisPoolAdapter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.function.Supplier;

public final class JedisLockUtils {

    private JedisLockUtils() {}




    /**
     * Will execute the task between locking
     * Helper method that creates the lock for simpler use
     * The steps are: create lock - obtain lock - execute task - free lock
     * A simple lock without time limit and interrumpiblity is used
     * @param jedisPool Jedis pool client
     * @param name Name of the lock
     * @param task Task to execute
     */
    public static <T> T underLockTask(JedisPool jedisPool, String name, Supplier<T> task) {
        JedisLock jedisLock = new JedisLock(jedisPool, name);
        return jedisLock.underLock(task);
    }

    /**
     * Will execute the task between locking
     * Helper method that creates the lock for simpler use
     * The steps are: create lock - obtain lock - execute task - free lock
     * A simple lock without time limit and interruptibility is used
     * @param jedis Jedis client
     * @param name Name of the lock
     * @param task Task to execute
     */
    public static <T> T underLockTask(Jedis jedis, String name, Supplier<T> task) {
        return underLockTask(JedisPoolAdapter.poolFromJedis(jedis), name, task);
    }



    /**
     * Will execute the task between locking and return the result
     * Helper method that creates the lock for simpler use
     * The steps are: obtain lock - execute task - free lock - return result
     * A simple lock without time limit and interruptibility is used
     * @param jedisPool Jedis pool client
     * @param name Name of the lock
     * @param task Task to execute with return type
     */
    public static void underLockTask(JedisPool jedisPool, String name, Runnable task) {
        JedisLock jedisLock = new JedisLock(jedisPool, name);
        jedisLock.underLock(task);
    }

    /**
     * Will execute the task between locking and return the result
     * Helper method that creates the lock for simpler use
     * The steps are: obtain lock - execute task - free lock - return result
     * A simple lock without time limit and interruptibility is used
     * @param jedis Jedis client
     * @param name Name of the lock
     * @param task Task to execute with return type
     */
    public static void underLockTask(Jedis jedis, String name, Runnable task) {
        underLockTask(JedisPoolAdapter.poolFromJedis(jedis), name, task);
    }

}
