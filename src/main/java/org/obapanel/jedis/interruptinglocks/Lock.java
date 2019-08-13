package org.obapanel.jedis.interruptinglocks;


import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;


/**
 * java.util.concurrent.locks.Lock from a Redis Lock
 */
class Lock implements java.util.concurrent.locks.Lock, Closeable, AutoCloseable {


    private final JedisLock jedisLock;


    /**
     * Creates a java.util.concurrent.locks.Lock from a Redis Lock
     * @param jedisLock Redis Lock
     */
    Lock(JedisLock jedisLock){
        this.jedisLock = jedisLock;
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
    public void close() throws IOException {
        unlock();
    }

}
