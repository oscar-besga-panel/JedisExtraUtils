package org.oba.jedis.extra.utils.rateLimiter.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.rateLimiter.BucketRateLimiter;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class FunctionalBucketRateLimiterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalBucketRateLimiterTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPool jedisPool;
    private String bucketName;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
        bucketName = "bucketName:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Test
    public void bucketBasicTest() throws InterruptedException {
        BucketRateLimiter bucketRateLimiter = new BucketRateLimiter(jedisPool, bucketName).
                create(1, BucketRateLimiter.Mode.INTERVAL, 500, TimeUnit.MILLISECONDS);
        boolean result1 = bucketRateLimiter.acquire();
        Thread.sleep(200);
        boolean result2 = bucketRateLimiter.acquire();
        Thread.sleep(400);
        boolean result3 = bucketRateLimiter.acquire();
        assertTrue(bucketRateLimiter.exists());
        assertTrue(result1);
        assertFalse(result2);
        assertTrue(result3);
    }

    @Test
    public void bucketAdvancedTest() throws InterruptedException {
        BucketRateLimiter bucketRateLimiter = new BucketRateLimiter(jedisPool, bucketName).
                create(2, BucketRateLimiter.Mode.INTERVAL, 100, TimeUnit.MILLISECONDS);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);
        Callable<Boolean> tryAcquire = () -> this.tryAcquire(bucketRateLimiter);
        List<ScheduledFuture<Boolean>> scheduledFutureList = new ArrayList<>();
        for(int i= 0; i < 100; i++) {
            long wait = 1000L + (i % 3);
            ScheduledFuture<Boolean> scheduledFuture = executorService.schedule(tryAcquire, wait, TimeUnit.MILLISECONDS);
            scheduledFutureList.add(scheduledFuture);
        }
        AtomicInteger count = new AtomicInteger(0);
        scheduledFutureList.forEach( sf -> {
            try {
                if (sf.get()) {
                    count.incrementAndGet();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals(1, count.get());

    }

    boolean tryAcquire(BucketRateLimiter bucketRateLimiter){
        boolean result = bucketRateLimiter.acquire();
        return result;
    }

}
