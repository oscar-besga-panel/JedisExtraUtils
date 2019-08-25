package org.obapanel.jedis.interruptinglocks;

import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

public final class InterruptingJedisJedisLockBase extends AbstractInterruptingJedisLock {


    private Thread interruptingThread;


    /**
     * Main constructor
     * @param jedis client to generate the lock
     * @param name Lock name
     */
    public InterruptingJedisJedisLockBase(Jedis jedis, String name, long leaseTime, TimeUnit timeUnit) {
        super(jedis, name, leaseTime, timeUnit);
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
