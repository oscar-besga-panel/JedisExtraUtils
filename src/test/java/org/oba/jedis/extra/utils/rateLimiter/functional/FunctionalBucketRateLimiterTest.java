package org.oba.jedis.extra.utils.rateLimiter.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.rateLimiter.BucketRateLimiter;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.test.WithJedisPoolDelete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
            WithJedisPoolDelete.doDelete(jedisPool, bucketName);
            jedisPool.close();
        }
    }

    @Test
    public void create0Test() {
        BucketRateLimiter bucketRateLimiter = new BucketRateLimiter(jedisPool, bucketName).
                create(10, BucketRateLimiter.Mode.INTERVAL, 1 , TimeUnit.SECONDS);
        assertTrue(bucketRateLimiter.exists());
        bucketRateLimiter.delete();
        assertFalse(bucketRateLimiter.exists());
        bucketRateLimiter.
                createIfNotExists(10, BucketRateLimiter.Mode.INTERVAL, 1 , TimeUnit.SECONDS);
        assertTrue(bucketRateLimiter.exists());
        bucketRateLimiter.
                createIfNotExists(20, BucketRateLimiter.Mode.INTERVAL, 2 , TimeUnit.SECONDS);
        assertTrue(bucketRateLimiter.exists());
        assertEquals("10", jedisPool.getResource().hget(bucketName, "capacity"));
    }

    @Test
    public void bucketBasic01Test() throws InterruptedException {
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
    public void bucketBasic03Test() throws InterruptedException {
        BucketRateLimiter bucketRateLimiter = new BucketRateLimiter(jedisPool, bucketName).
                create(1, BucketRateLimiter.Mode.GREEDY, 500, TimeUnit.MILLISECONDS);
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
    public void bucketAdvanced01Test() {
        BucketRateLimiter bucketRateLimiter = new BucketRateLimiter(jedisPool, bucketName).
                create(1, BucketRateLimiter.Mode.INTERVAL, 1000, TimeUnit.MILLISECONDS);

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(105);
        List<ScheduledFuture<Boolean>> scheduledFutureList = new ArrayList<>();
        for(int i= 0; i < 100; i++) {
            long wait = 500L + (i % 3)*1000 + ThreadLocalRandom.current().nextLong(5L,75L);
            ScheduledFuture<Boolean> scheduledFuture = executorService.schedule(() -> tryAcquire(wait, bucketRateLimiter),
                    wait, TimeUnit.MILLISECONDS);
            scheduledFutureList.add(scheduledFuture);
        }
        AtomicInteger count = new AtomicInteger(0);
        scheduledFutureList.forEach( sf -> countIfGet(count, sf));
        LOGGER.debug("bucketAdvanced01Test count {}", count.get());
        assertEquals(3, count.get());
    }

    @Test
    public void bucketAdvanced02Test() {
        
        BucketRateLimiter bucketRateLimiter = new BucketRateLimiter(jedisPool, bucketName).
                create(1, BucketRateLimiter.Mode.GREEDY, 1000, TimeUnit.MILLISECONDS);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(205);
        List<ScheduledFuture<Boolean>> scheduledFutureList = new ArrayList<>();
        for(int i= 0; i < 200; i++) {
            long wait = 500L + (i % 3)*1000 + ThreadLocalRandom.current().nextLong(5L,75L);
            ScheduledFuture<Boolean> scheduledFuture = executorService.schedule(() -> tryAcquire(wait, bucketRateLimiter),
                    wait, TimeUnit.MILLISECONDS);
            scheduledFutureList.add(scheduledFuture);
        }
        AtomicInteger count = new AtomicInteger(0);
        scheduledFutureList.forEach( sf -> countIfGet(count, sf));
        LOGGER.debug("bucketAdvanced02Test count {}", count.get());
        assertTrue(count.get() >= 3);
    }

    private static boolean tryAcquire(long currentWait, BucketRateLimiter bucketRateLimiter) {
        LOGGER.debug("tryAcquire currentWait {}", currentWait);
        boolean result = bucketRateLimiter.acquire();
        if (result) {
            LOGGER.debug("acquire Ok currentWait {}", currentWait);
        }
        return result;
    }

    private static void countIfGet(AtomicInteger count, ScheduledFuture<Boolean> sf) {
        try {
            if (sf.get()) {
                count.incrementAndGet();
            }
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Error while getting response",e);
            throw new RuntimeException(e);
        }
    }


}
