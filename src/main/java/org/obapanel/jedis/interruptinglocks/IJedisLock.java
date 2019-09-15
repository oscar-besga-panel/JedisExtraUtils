package org.obapanel.jedis.interruptinglocks;

import redis.clients.jedis.Jedis;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public interface IJedisLock {

    /**
     * Name of the lock
     * @return name
     */
    String getName();

    /**
     * Lease time of the lock, null if none
     * If null, lock is locked until manually unlocked
     * @return leaseTime
     */
    Long getLeaseTime();

    /**
     * Timeunit of leased time, null if none
     * @return timeUnit
     */
    TimeUnit getTimeUnit();


    /**
     * Attempts to get the lock, It will try one time and return
     * @return true if lock obtained, false otherwise
     */
    boolean tryLock();

    /**
     * Tries to obtain lock for a time, if time is consumed and no lock obtained, the method desists and returns false
     * This does NOT have anything to do with the lock lease time, if it has one
     * @param time Time to expend trying to obtain lock
     * @param unit Unit of the time
     * @return true if lock obtained, false otherwise
     * @throws InterruptedException If someone interrputs the action
     */
    boolean tryLockForAWhile(long time, TimeUnit unit) throws InterruptedException;

    /**
     * Try to lock, sleeping while it tries
     * Can NOT be interrupted
     */
    void lock();

    /**
     * Try to lock, sleeping while it tries
     * @throws InterruptedException can be interrupted
     */
    void lockInterruptibly() throws InterruptedException;

    /**
     * Attempts to unlock the lock
     */
    void unlock();

    /**
     * Returns true if the lock is retained with this object
     * If lock is retained, and the time has expired, a unlock will be performed
     * This method will use and consume the lock
     * @return true if lock is retained here
     */
    boolean isLocked();


    /**
     * Will execute the task between locking
     * The steps are: obtain lock - execute task - free lock
     * A simple lock without time limit and interrumpiblity is used
     * This method will use and consume the lock
     * @param task Task to execute
     */
    void underLock(Runnable task);


    /**
     * Will execute the task between locking and return the result
     * The steps are: obtain lock - execute task - free lock - return result
     * A simple lock without time limit and interrumpiblity is used
     * @param task Task to execute with return type
     */
    <T> T underLock(Callable<T> task) throws Exception;




    /**
     * Will execute the task between locking
     * Helper method that creates the lock for simpler use
     * The steps are: create lock - obtain lock - execute task - free lock
     * A simple lock without time limit and interrumpiblity is used
     * @param jedis Jedis client
     * @param name Name of the lock
     * @param task Task to execute
     */
    public static <T> T underLockTask(Jedis jedis, String name, Callable<T> task) throws Exception {
        JedisLock jedisLock = new JedisLock(jedis, name);
        jedisLock.lock();
        T result = task.call();
        jedisLock.unlock();
        return result;
    }

    /**
     * Will execute the task between locking and return the result
     * Helper method that creates the lock for simpler use
     * The steps are: obtain lock - execute task - free lock - return result
     * A simple lock without time limit and interrumpiblity is used
     * @param jedis Jedis client
     * @param name Name of the lock
     * @param task Task to execute with return type
     */
    public static void underLockTask(Jedis jedis, String name, Runnable task) {
        JedisLock jedisLock = new JedisLock(jedis, name);
        jedisLock.lock();
        task.run();
        jedisLock.unlock();
    }


}
