package org.oba.jedis.extra.utils.interruptinglocks;

import redis.clients.jedis.JedisPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * This class will use a executor to get a thread to wait for lease time to go zero
 *
 * This class is not thread safe, do not share within multiple threads
 *
 * CAUTION: the executor can prevent the interrupting operation to be launched,
 * be aware not to use an exhausted pool
 * For this reason, is recommended a cached thread pool, which will create bew threads on demand or will use old ones if avalible
 * For example, with Executors.newCachedThreadPool()
 */
public final class InterruptingJedisJedisLockExecutor extends AbstractInterruptingJedisLock {

    private final ExecutorService executorService;
    private Future<?> future;


    /**
     * Main constructor
     * @param jedisPool Client pool to generate the lock
     * @param name Lock name
     * @param leaseTime Time to lease the lock
     * @param timeUnit Unit of leaseTime
     * @param executorService Executor service that will provide locks
     */
    public InterruptingJedisJedisLockExecutor(JedisPool jedisPool, String name, long leaseTime, TimeUnit timeUnit, ExecutorService executorService) {
        super(jedisPool, name, leaseTime, timeUnit);
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

    /**
     * Current executor service
     * @return executorService
     */
    public ExecutorService getExecutorService(){
        return executorService;
    }
}
