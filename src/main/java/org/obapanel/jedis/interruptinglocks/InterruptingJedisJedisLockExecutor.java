package org.obapanel.jedis.interruptinglocks;

import redis.clients.jedis.Jedis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * This class will use a executor to get a thread to wait for lease time to go zero
 *
 * CAUTION: the executor can prevent the interrupting operation to be launched,
 * be aware not to use an exhausted pool
 */
public class InterruptingJedisJedisLockExecutor extends AbstractInterruptingJedisLock {

    private ExecutorService executorService;
    private Future future;


    /**
     * Main constructor
     * @param jedis Client to generate the lock
     * @param name Lock name
     */
    public InterruptingJedisJedisLockExecutor(Jedis jedis, String name, long leaseTime, TimeUnit timeUnit, ExecutorService executorService) {
        super(jedis, name, leaseTime, timeUnit);
        this.executorService = executorService;
    }

    @Override
    void startInterruptingThread() {
        future = executorService.submit(this::runInterruptThread);
    }

    @Override
    void stopInterruptingThread() {
        future.cancel(true);
    }
}
