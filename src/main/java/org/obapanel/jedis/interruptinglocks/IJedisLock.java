package org.obapanel.jedis.interruptinglocks;


import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Interface for locks on redis
 */
public interface IJedisLock extends AutoCloseable {

    /**
     * Name of the lock. Can not be null
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
     * Can be interrupted
     * @throws InterruptedException if interrupted
     */
    void lockInterruptibly() throws InterruptedException;

    /**
     * Attempts to unlock the lock
     */
    void unlock();

    // Closing the resource is the same as unlocking the lock
    default void close() {
        unlock();
    }

    /**
     * Returns true if the lock is retained with this object
     * If the time has expired, it will return false
     * If the time has not expired or no lease time is set, it will check the lock value against redis
     * @return true if lock is retained here
     */
    boolean isLocked();


    /**
     * Will execute the task between locking of this lock
     * The steps are: obtain lock - execute task - free lock
     * This is into a try-catch so close is mandatory, even after an exception
     * This operation can be interrputed, and will wait to obtaint the lock
     * This method will use and consume the lock
     * @param task Task to execute
     */
    void underLock(Runnable task);


    /**
     * Will execute the task between locking and return the result
     * The steps are: obtain lock - execute task - free lock - return result
     * This is into a try-catch so close is mandatory, even after an exception
     * This operation can be interrputed, and will wait to obtaint the lock
     * @param task Task to execute with return type
     */
    <T> T underLock(Supplier<T> task);



}
