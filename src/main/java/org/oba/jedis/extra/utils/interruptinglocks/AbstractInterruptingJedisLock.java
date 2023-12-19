package org.oba.jedis.extra.utils.interruptinglocks;

import org.oba.jedis.extra.utils.lock.IJedisLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Abstract class base to interrupting locks
 * It carries most of the code, and the thread generation is in the descendants
 */
public abstract class AbstractInterruptingJedisLock implements IJedisLock {


    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractInterruptingJedisLock.class);


    private final IJedisLock jedisLock;
    private Thread currentThread;
    private final boolean forceTimeoutRedis;
    private final long leaseTime;
    private final TimeUnit timeUnit;
    private final AtomicBoolean manualUnlock = new AtomicBoolean(false);
    private final long leaseTimeDiscountMillis;
    private final long recoverFromInterruptionMillis;


    /**
     * Base constructor
     * @param jedisPool Jedis connection pool
     * @param name Name of the lock
     * @param leaseTime time to lease the lock and wait to interrupt the main thread
     * @param timeUnit  unit of the lease time
     */
    AbstractInterruptingJedisLock(JedisPool jedisPool, String name, long leaseTime, TimeUnit timeUnit) {
        this(jedisPool, name,leaseTime, timeUnit, false);
    }

    /**
     * Base constructor
     * @param jedisPool Jedis connection pool
     * @param name Name of the lock
     * @param leaseTime time to lease the lock and wait to interrupt the main thread
     * @param forceTimeoutRedis If jedis lock should have a timeout or be released when the interrput occurs from java
     * @param timeUnit  unit of the lease time
     */
    AbstractInterruptingJedisLock(JedisPool jedisPool, String name, long leaseTime, TimeUnit timeUnit, boolean forceTimeoutRedis) {
        if (forceTimeoutRedis){
            jedisLock = new JedisLock(jedisPool, name, leaseTime, timeUnit);
            this.leaseTimeDiscountMillis = 10L;
            this.recoverFromInterruptionMillis = 15L;
        } else {
            jedisLock = new JedisLock(jedisPool, name);
            this.leaseTimeDiscountMillis = 0L;
            this.recoverFromInterruptionMillis = 10L;
        }
        this.forceTimeoutRedis = forceTimeoutRedis;
        this.leaseTime = leaseTime;
        this.timeUnit = timeUnit;
    }

    @Override
    public String getName() {
        return jedisLock.getName();
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisLock.getJedisPool();
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
    public long getLeaseMoment() {
        return jedisLock.getLeaseMoment();
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

    /**
     * Execute after getting a lock
     */
    private void afterLock(){
        currentThread = Thread.currentThread();
        startInterruptingThread();
    }

    /**
     * Execute after relesing a lock
     */
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
            LOGGER.debug("runInterruptThread realTimeToSleep {} leaseTime {} forceTimeoutRedis {}", realTimeToSleep, currentLeaseTime, forceTimeoutRedis);
            if (realTimeToSleep > 0) {
                Thread.sleep(realTimeToSleep);
            } else {
                LOGGER.error("runInterruptThread realTimeToSleep ERROR, sleepring 50");
                Thread.sleep(50);
            }
            interruptAndUnlock();
        } catch (InterruptedException e) {
            LOGGER.debug("runInterruptThread interrupted");
        }
    }

    /**
     * Interrupts the main thread and unlocks the redis lock in remote
     */
    private synchronized void interruptAndUnlock() {
        if (!manualUnlock.get() && currentThread != null) {
            LOGGER.debug("interruptAndUnlock interrupt current thread " + currentThread.getName());
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
