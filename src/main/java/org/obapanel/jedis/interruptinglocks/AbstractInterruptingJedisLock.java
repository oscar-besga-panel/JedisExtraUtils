package org.obapanel.jedis.interruptinglocks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractInterruptingJedisLock implements IJedisLock {


    private static final Logger LOG = LoggerFactory.getLogger(AbstractInterruptingJedisLock.class);


    private JedisLock jedisLock;
    private Thread currentThread;
    private AtomicBoolean manualUnlock = new AtomicBoolean(false);

    AbstractInterruptingJedisLock(Jedis jedis, String name, long leaseTime, TimeUnit timeUnit) {
        jedisLock = new JedisLock(jedis,name,leaseTime, timeUnit);
    }

    public boolean isLocked() {
        return jedisLock.isLocked();
    }

    public boolean tryLock() {
        jedisLock.tryLock();
        if (isLocked()) {
            afterLock();
        }
        return isLocked();
    }

    public boolean tryLockForAWhile(long time, TimeUnit unit) throws InterruptedException {
        jedisLock.tryLockForAWhile(time, unit);
        if (isLocked()) {
            afterLock();
        }
        return isLocked();
    }


    public void lock() {
        jedisLock.lock();
        if (isLocked()) {
            afterLock();
        }
    }


    public void lockInterruptibly() throws InterruptedException {
        jedisLock.lockInterruptibly();
        if (isLocked()) {
            afterLock();
        }
    }






    @Override
    public String getName() {
        return jedisLock.getName();
    }

    @Override
    public void unlock() {
        jedisLock.unlock();
        if (!isLocked()) {
            afterUnLock();
        }
    }


    private void afterLock(){
        currentThread = Thread.currentThread();
        startInterruptingThread();
    }



    private void afterUnLock(){
        manualUnlock.set(true);
        stopInterruptingThread();
    }

    /**
     * Starts the thread that will interrupt the main one when time expires
     */
    abstract void startInterruptingThread();

    /**
     * Stops the interrupting thread
     */
    abstract void stopInterruptingThread();

    /**
     * Method that will wait the lease time of the lock
     *
     * After waiting,
     * if lock is locked, it will unlock it
     * if no manual unlock has happen it will interrupt the main thread
     */
    final void runInterruptThread() {
        try {
            long currentLeaseTime = jedisLock.getTimeUnit().toMillis( jedisLock.getLeaseTime() );
            long realTimeToSleep = jedisLock.getLeaseMoment() + currentLeaseTime - System.currentTimeMillis();
            LOG.debug("runInterruptThread realTimeToSleep {} leaseTime {} ", realTimeToSleep, currentLeaseTime);
            Thread.sleep(realTimeToSleep);
        } catch (InterruptedException e) {
            LOG.debug("runInterruptThread interrupted");
        }
        // If lock is locked, then unlock
        if (isLocked()) {
            LOG.debug("runInterruptThread lock is locked");
            jedisLock.unlock();
        }
        // If no manual unlock has happened, interrupt current thread
        if (!manualUnlock.get() && currentThread != null) {
            LOG.debug("runInterruptThread interrupt current thread");
            currentThread.interrupt();
        }
    }
}
