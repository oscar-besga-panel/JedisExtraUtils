package org.oba.jedis.extra.utils.test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A fixed thread pool executor service, with daemon threads, that
 * allows core thread timeout.
 * https://stackoverflow.com/questions/13883293/turning-an-executorservice-to-daemon-in-java
 * https://stackoverflow.com/questions/13883293/turning-an-executorservice-to-daemon-in-java/29453160#29453160
 */
public class FixedDaemonThreadPool implements ExecutorService {



    private ThreadPoolExecutor threadPoolExecutor;
    private LinkedBlockingQueue<Runnable> workQueue;


    /**
     * Creates an executor service with a fixed pool size, that will time
     * out after a certain period of inactivity.
     *
     * @param poolSize The core- and maximum pool size
     */
    public FixedDaemonThreadPool(int poolSize) {
        this(poolSize, Long.MAX_VALUE, TimeUnit.DAYS);
    }

    /**
     * Creates an executor service with a fixed pool size, that will time
     * out after a certain period of inactivity.
     *
     * @param poolSize The core- and maximum pool size
     * @param keepAliveTime The keep alive time
     * @param timeUnit The time unit
     */
    public FixedDaemonThreadPool(int poolSize, long keepAliveTime, TimeUnit timeUnit) {
        threadPoolExecutor = new ThreadPoolExecutor(poolSize, poolSize,
                keepAliveTime, timeUnit, new LinkedBlockingQueue<>());
        threadPoolExecutor.setThreadFactory(new DaemonThreadPoolThreadFactory());
        threadPoolExecutor.allowCoreThreadTimeOut(true);
    }

    @Override
    public void shutdown() {
        threadPoolExecutor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return threadPoolExecutor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return threadPoolExecutor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return threadPoolExecutor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return threadPoolExecutor.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return threadPoolExecutor.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return threadPoolExecutor.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return threadPoolExecutor.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return threadPoolExecutor.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return threadPoolExecutor.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return threadPoolExecutor.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return threadPoolExecutor.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        threadPoolExecutor.execute(command);
    }

}
