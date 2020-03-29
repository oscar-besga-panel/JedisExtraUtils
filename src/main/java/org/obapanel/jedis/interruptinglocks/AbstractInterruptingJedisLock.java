package org.obapanel.jedis.interruptinglocks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public abstract class AbstractInterruptingJedisLock implements IJedisLock {


    private static final Logger LOG = LoggerFactory.getLogger(AbstractInterruptingJedisLock.class);


    private JedisLock jedisLock;
    private Thread currentThread;
    private boolean forceTimeoutRedis;
    private long leaseTime;
    private TimeUnit timeUnit;
    private AtomicBoolean manualUnlock = new AtomicBoolean(false);
    private long leaseTimeDiscountMillis;
    private long recoverFromInterruptionMillis;



    AbstractInterruptingJedisLock(Jedis jedis, String name, long leaseTime, TimeUnit timeUnit) {
        this(jedis, name,leaseTime, timeUnit, false);
    }

    AbstractInterruptingJedisLock(Jedis jedis, String name, long leaseTime, TimeUnit timeUnit, boolean forceTimeoutRedis) {
        if (forceTimeoutRedis){
            jedisLock = new JedisLock(jedis, name, leaseTime, timeUnit);
            this.leaseTimeDiscountMillis = 10L;
            this.recoverFromInterruptionMillis = 15L;

        } else {
            jedisLock = new JedisLock(jedis, name);
            this.leaseTimeDiscountMillis = 0L;
            this.recoverFromInterruptionMillis = 10L;
        }
        this.forceTimeoutRedis = forceTimeoutRedis;
        this.leaseTime = leaseTime;
        this.timeUnit = timeUnit;
    }

    public boolean isLocked() {
        return jedisLock.isLocked();
    }

    public synchronized boolean tryLock() {
        boolean result = jedisLock.tryLock();
        if (result) {
            afterLock();
        }
        return result;
    }

    public synchronized boolean tryLockForAWhile(long time, TimeUnit unit) throws InterruptedException {
        boolean result = jedisLock.tryLockForAWhile(time, unit);
        if (result) {
            afterLock();
        }
        return result;
    }


    public synchronized void lock() {
        jedisLock.lock();
        afterLock();
    }


    public synchronized void lockInterruptibly() throws InterruptedException {
        jedisLock.lockInterruptibly();
        afterLock();
    }

    @Override
    public Long getLeaseTime() {
        return this.leaseTime;
    }

    @Override
    public TimeUnit getTimeUnit() {
        return this.timeUnit;
    }

    @Override
    public String getName() {
        return jedisLock.getName();
    }

    @Override
    public synchronized void unlock() {
        jedisLock.unlock();
        if (!isLocked()) {
            afterUnLock();
        }
    }


    public synchronized void underLock(Runnable task) {
        try(AbstractInterruptingJedisLock aijl = this) {
            aijl.lock();
            task.run();
        }
    }

    public synchronized <T> T underLock(Supplier<T> task) {
        try(AbstractInterruptingJedisLock aijl = this) {
            aijl.lock();
            return task.get();
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
            long currentLeaseTime = timeUnit.toMillis( leaseTime );
            long realTimeToSleep = jedisLock.getLeaseMoment() + currentLeaseTime - System.currentTimeMillis() - leaseTimeDiscountMillis;
            LOG.debug("runInterruptThread realTimeToSleep {} leaseTime {} forceTimeoutRedis ", realTimeToSleep, currentLeaseTime, forceTimeoutRedis);
            if (realTimeToSleep > 0) {
                Thread.sleep(realTimeToSleep);
            } else {
                LOG.error("runInterruptThread realTimeToSleep ERROR, sleepring 50");
                Thread.sleep(50);
            }
            interruptAndUnlock();
        } catch (InterruptedException e) {
            LOG.debug("runInterruptThread interrupted");
        }
    }

    private synchronized void interruptAndUnlock() {
        if (!manualUnlock.get() && currentThread != null) {
            LOG.debug("interruptAndUnlock interrupt current thread " + currentThread.getName());
            currentThread.interrupt();
        }
        try {
            Thread.sleep(recoverFromInterruptionMillis);
        } catch (InterruptedException e) {
            //NOOP
        }
        jedisLock.unlock();
    }
}
