package org.oba.jedis.extra.utils.lock;


import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.oba.jedis.extra.utils.utils.Named;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Interface for locks on redis
 */
public interface IJedisLock extends AutoCloseable, Named, JedisPoolUser {

    /**
     * Lease time of the lock, null if none
     * If null, lock is locked until manually unlocked
p     * It gives the configured time, not the expire time if locked
     * @return leaseTime
     */
    Long getLeaseTime();

    /**
     * Timeunit of leased time, null if none
     * @return timeUnit
     */
    TimeUnit getTimeUnit();

    /**
     * Moment when the lock was captured in this object, -1 if no locked
     * @return leaseMoment
     */
    long getLeaseMoment();

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


    /**
     * Adds more lease time to the current lock
     * Only if the lock is currently locked by this object at the current time, if not it doesn't affect
     * Only applies if the lock was given a lease time to begin with
     * It doesn't affect the original lease time that is given to the lock in the constructor
     * @param addExpireTimeMillis Time to add to the lock situation
     * @return true if the lock has more time added
     */
    boolean addMoreExpireTimeToCurrentLock(Long addExpireTimeMillis);


    /**
     * Adds more lease time to the current lock. See method above
     * @param addExpireTime Time to add to the lock situation
     * @param timeUnit timeUnit of the time
     * @return true if the lock has more time added
     */
    default boolean addMoreExpireTimeToCurrentLock(Long addExpireTime, TimeUnit timeUnit) {
        return addMoreExpireTimeToCurrentLock(timeUnit.toMillis(addExpireTime));
    }


    /**
     * Returns the current time to live that has the current lock until if expires
     * Only if the lock is currently locked by this object at the current time, if not it doesn't affect
     * Only applies if the lock was given a lease time to begin with.
     * If not, it returns -1L
     * In milliseconds
     * @return time to live currently or -1L
     */
    long timeToLiveMillis();


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
