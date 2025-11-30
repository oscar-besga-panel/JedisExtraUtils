package org.oba.jedis.extra.utils.rateLimiter.functional;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.rateLimiter.ThrottlingRateLimiter;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;


public class FunctionalThrottlingRateLimiterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalThrottlingRateLimiterTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPooled jedisPooled;
    private String throttlingName;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPooled = jtfTest.createJedisPooled();
        throttlingName = "throttlingName:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        ThrottlingRateLimiter throttlingRateLimiter = new ThrottlingRateLimiter(jedisPooled, throttlingName);
        if (throttlingRateLimiter.exists()) {
            throttlingRateLimiter.delete();
            LOGGER.debug("deleted throttlingRateLimiter {}", throttlingName);
        }
        if (jedisPooled != null) {
            jedisPooled.del(throttlingName);
            jedisPooled.close();
        }
    }

    @Test
    public void create0Test() {
        ThrottlingRateLimiter throttlingRateLimiter = new ThrottlingRateLimiter(jedisPooled, throttlingName).
                create(1, TimeUnit.SECONDS);
        Assert.assertTrue(throttlingRateLimiter.exists());
        throttlingRateLimiter.delete();
        Assert.assertFalse(throttlingRateLimiter.exists());
        throttlingRateLimiter.
                createIfNotExists(1500);
        Assert.assertTrue(throttlingRateLimiter.exists());
        throttlingRateLimiter.
                createIfNotExists(2, TimeUnit.SECONDS);
        Assert.assertTrue(throttlingRateLimiter.exists());
        assertEquals("1500000", jedisPooled.hget(throttlingName, "allow_micros"));
    }

    @Test(timeout = 15000)
    public void throttlingBasicTest() throws InterruptedException {
        ThrottlingRateLimiter rateLimiter = new ThrottlingRateLimiter(jedisPooled, throttlingName).
                create(500, TimeUnit.MILLISECONDS);
        Thread.sleep(550);
        boolean result1 = rateLimiter.allow();
        Thread.sleep(200);
        boolean result2 = rateLimiter.allow();
        Thread.sleep(400);
        boolean result3 = rateLimiter.allow();
        assertTrue(rateLimiter.exists());
        assertTrue(result1);
        assertFalse(result2);
        assertTrue(result3);
        rateLimiter.delete();
        assertFalse(rateLimiter.exists());
    }


    @Test(timeout = 35000)
    public void throttlingAdvancedTest() {
        SortedMap<Integer, Map.Entry<Long,Boolean>> resultMap = new TreeMap<>();
        ThrottlingRateLimiter rateLimiter = new ThrottlingRateLimiter(jedisPooled, throttlingName).
                create(495, TimeUnit.MILLISECONDS);
        ExecutorService executor = Executors.newFixedThreadPool(50);
        List<Future<Boolean>> futureList = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        for(int i= 0; i < 50; i++) {
            final int num = i + 1;
            Future<Boolean> result = executor.submit(() ->
                    tryToAcquire(rateLimiter, num, countDownLatch, resultMap)
            );
            futureList.add(result);
        }
        countDownLatch.countDown();
        long result = futureList.stream().
                filter(this::futureIsTrue).
                count();
        LOGGER.debug("result is {}", result);
        resultMap.forEach( (entryNum, entryData) ->
            LOGGER.debug("Resultmap num {} wait {} value {} ", entryNum, entryData.getKey(), entryData.getValue())
        );

        assertTrue(rateLimiter.exists());
        //assertEquals(5, result);
        assertTrue( result >= 9 && result <= 11);
    }

    boolean tryToAcquire(ThrottlingRateLimiter rateLimiter, int num, CountDownLatch countDownLatch, SortedMap<Integer, Map.Entry<Long,Boolean>> resultMap) {
        try {
            long waitMillis = (Math.floorDiv(num, 5) * 500L) - ThreadLocalRandom.current().nextLong(5L,15L) + 500L;
            LOGGER.debug("Wait num {} waitMillis {}", num, waitMillis);
            countDownLatch.await();
            Thread.sleep(waitMillis);
            boolean allowed =  rateLimiter.allow();
            LOGGER.debug("Try to acquire num {} result {} ", num, allowed);

            Map.Entry<Long,Boolean> entry = new AbstractMap.SimpleEntry<>(waitMillis, allowed);
            resultMap.put(num, entry);
            return allowed;
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting num {} ", num, e);
            throw new RuntimeException(e);
        }

    }

    boolean futureIsTrue(Future<Boolean> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.debug("Error in future", e);
            return false;
        }
    }

}
