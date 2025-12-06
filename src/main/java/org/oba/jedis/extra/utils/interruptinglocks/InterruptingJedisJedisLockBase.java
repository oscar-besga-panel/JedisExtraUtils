package org.oba.jedis.extra.utils.interruptinglocks;

import redis.clients.jedis.JedisPooled;

import java.util.concurrent.TimeUnit;

/**
 * This interrupting lock will create a background thread into the class
 * This is the preferred method for the interrupting locks
 *
 * This class is not thread safe, do not share within multiple threads
 */
public final class InterruptingJedisJedisLockBase extends AbstractInterruptingJedisLock {


    private final Thread interruptingThread;


    /**
     * Main constructor
     * @param jedisPooled client connections pool to generate the lock
     * @param name Lock name
     */
    public InterruptingJedisJedisLockBase(JedisPooled jedisPooled, String name, long leaseTime, TimeUnit timeUnit) {
        super(jedisPooled, name, leaseTime, timeUnit);
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
