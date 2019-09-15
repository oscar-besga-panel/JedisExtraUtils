package org.obapanel.jedis.interruptinglocks;

import redis.clients.jedis.Jedis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * This class will use a executor to get a thread to wait for lease time to go zero
 *
 * CAUTION: the executor can prevent the interrupting operation to be launched,
 * be aware not to use an exhausted pool
 * For this reason, is recommended a cached thread pool, which will create bew threads on demand or will use old ones if avalible
 * For example, with Executors.newCachedThreadPool()
 */
public class InterruptingJedisJedisLockExecutor extends AbstractInterruptingJedisLock {

    private ExecutorService executorService;
    private Future future;

    /**
     * Main constructor
     * Creates a cached thread pool, which will create bew threads on demand or will use old ones if avalible
     * @param jedis Jedis client
     * @param name Lock name
     * @param leaseTime Time to lease the lock
     * @param timeUnit Unit of leaseTime
     */
    public InterruptingJedisJedisLockExecutor(Jedis jedis, String name, long leaseTime, TimeUnit timeUnit) {
        super(jedis, name, leaseTime, timeUnit);
        this.executorService = Executors.newCachedThreadPool();
    }


    /**
     * Main constructor
     * @param jedis Client to generate the lock
     * @param name Lock name
     * @param leaseTime Time to lease the lock
     * @param timeUnit Unit of leaseTime
     * @param executorService Executor service that will provide locks
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

    public ExecutorService getExecutorService(){
        return executorService;
    }
}
