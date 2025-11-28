package org.oba.jedis.extra.utils.interruptinglocks;

import redis.clients.jedis.JedisPooled;

import java.util.function.Supplier;

public final class JedisLockUtils {

    private JedisLockUtils() {}




    /**
     * Will execute the task between locking
     * Helper method that creates the lock for simpler use
     * The steps are: create lock - obtain lock - execute task - free lock
     * A simple lock without time limit and interrumpiblity is used
     * @param jedisPooled Jedis pool client
     * @param name Name of the lock
     * @param task Task to execute
     */
    public static <T> T underLockTask(JedisPooled jedisPooled, String name, Supplier<T> task) {
        JedisLock jedisLock = new JedisLock(jedisPooled, name);
        return jedisLock.underLock(task);
    }



    /**
     * Will execute the task between locking and return the result
     * Helper method that creates the lock for simpler use
     * The steps are: obtain lock - execute task - free lock - return result
     * A simple lock without time limit and interruptibility is used
     * @param jedisPooled Jedis pool client
     * @param name Name of the lock
     * @param task Task to execute with return type
     */
    public static void underLockTask(JedisPooled jedisPooled, String name, Runnable task) {
        JedisLock jedisLock = new JedisLock(jedisPooled, name);
        jedisLock.underLock(task);
    }

}
