package org.oba.jedis.extra.utils.interruptinglocks;


import org.oba.jedis.extra.utils.lock.IJedisLock;
import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.oba.jedis.extra.utils.utils.Named;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;


/**
 * java.util.concurrent.locks.Lock from a Redis Lock
 */
public class LockFromRedis implements Lock, AutoCloseable, Named, JedisPoolUser {


    private final IJedisLock jedisLock;

    /**
     * Creates a java.util.concurrent.locks.Lock from a Redis Lock
     * @param jedisLock Redis Lock
     */
    public LockFromRedis(IJedisLock jedisLock){
        this.jedisLock = jedisLock;
    }


    public String getName() {
        return jedisLock.getName();
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisLock.getJedisPool();
    }


    @Override
    public void lock() {
        jedisLock.lock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        jedisLock.lockInterruptibly();
    }

    @Override
    public boolean tryLock() {
        return jedisLock.tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return jedisLock.tryLockForAWhile(time, unit);
    }

    @Override
    public void unlock() {
        jedisLock.unlock();
    }

    @Override
    public Condition newCondition() {
        throw new IllegalStateException("Not implemented nor supported");
    }

    /**
     * Checks if the lock is currently in use
     * @return true if the lock is active
     */
    public boolean isLocked(){
        return jedisLock.isLocked();
    }

    @Override
    public void close() {
        unlock();
    }

}
