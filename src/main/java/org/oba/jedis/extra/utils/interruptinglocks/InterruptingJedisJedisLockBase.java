package org.oba.jedis.extra.utils.interruptinglocks;

import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;

/**
 * This interrupting lock will create a background thread into the class
 * This is the preferred method for the interrupting locks
 */
public final class InterruptingJedisJedisLockBase extends AbstractInterruptingJedisLock {


    private final Thread interruptingThread;


    /**
     * Main constructor
     * @param jedisPool client connections pool to generate the lock
     * @param name Lock name
     */
    public InterruptingJedisJedisLockBase(JedisPool jedisPool, String name, long leaseTime, TimeUnit timeUnit) {
        super(jedisPool, name, leaseTime, timeUnit);
        interruptingThread = new Thread(this::runInterruptThread);
        interruptingThread.setDaemon(true);
        interruptingThread.setName(name + "_interruptingThread");
    }


    void startInterruptingThread() {
        if (isLocked()){
            interruptingThread.start();
        }
    }

    @Override
    void stopInterruptingThread() {
        interruptingThread.interrupt();
    }



}
