package org.obapanel.jedis.interruptinglocks;

import java.util.concurrent.TimeUnit;

public interface IJedisLock {


    String getName();

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

    void lock();

    void lockInterruptibly() throws InterruptedException;

    void unlock();

    boolean isLocked();
}
