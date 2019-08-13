package org.obapanel.jedis.interruptinglocks;


import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;


/**
 * java.util.concurrent.locks.Lock from a Redis Lock
 */
class Lock implements java.util.concurrent.locks.Lock, Closeable, AutoCloseable {


    private final JedisLock rLock;


    /**
     * Creates a java.util.concurrent.locks.Lock from a Redis Lock
     * @param rLock Redis Lock
     */
    Lock(JedisLock rLock){
        this.rLock = rLock;
    }




    @Override
    public void lock() {
        rLock.redisLock();
        while (!rLock.isLocked()) {
            try {
                Thread.sleep(1000);
                rLock.redisLock();
            } catch (InterruptedException e) {
                // NOOP
            }
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        rLock.redisLock();
        while (!rLock.isLocked()) {
            Thread.sleep(1000);
            rLock.isLocked();
        }
    }

    @Override
    public boolean tryLock() {
        return rLock.redisLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long tryLockTimeLimit = System.currentTimeMillis() + unit.toMillis(time);
        rLock.redisLock();
        while (!rLock.isLocked() && tryLockTimeLimit > System.currentTimeMillis()) {
            Thread.sleep(1000);
            rLock.redisLock();
        }
        return rLock.isLocked();
    }

    @Override
    public void unlock() {
        rLock.unlock();
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
        return rLock.isLocked();
    }

    @Override
    public void close() throws IOException {
        unlock();
    }

}
