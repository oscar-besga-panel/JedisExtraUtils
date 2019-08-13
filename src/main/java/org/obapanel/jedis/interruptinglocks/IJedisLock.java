package org.obapanel.jedis.interruptinglocks;

public interface IJedisLock {


    String getName();

    boolean tryLock();

    void lock();

    void lockInterruptibly() throws InterruptedException;

    void unlock();

    boolean isLocked();
}
