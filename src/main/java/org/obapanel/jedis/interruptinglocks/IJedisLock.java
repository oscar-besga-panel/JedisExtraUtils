package org.obapanel.jedis.interruptinglocks;

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
     * @return true if lock is retained here
     */
    boolean isLocked();
}
