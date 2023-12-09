package org.oba.jedis.extra.utils.notificationLock.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.oba.jedis.extra.utils.notificationLock.NotificationLockFactory;
import org.oba.jedis.extra.utils.notificationLock.StreamMessageSystem;
import org.oba.jedis.extra.utils.rateLimiter.functional.FunctionalBucketRateLimiterTest;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.utils.SimpleEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class FunctionalStreamMessageSystemTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalBucketRateLimiterTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPool jedisPool;
    private String factoryName;
    private NotificationLockFactory mockNotificationLockFactory;
    private Semaphore semaphore;
    private List<SimpleEntry> messageList;
    private ExecutorService execurtor;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
        factoryName = "factoryName:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        semaphore = new Semaphore(0);
        messageList = new ArrayList<>();
        mockNotificationLockFactory = Mockito.mock(NotificationLockFactory.class);
        when(mockNotificationLockFactory.getName()).thenReturn(factoryName);
        when(mockNotificationLockFactory.getJedisPool()).thenReturn(jedisPool);
        Mockito.doAnswer( ioc -> {
            String channel = ioc.getArgument(0, String.class);
            String message = ioc.getArgument(1, String.class);
            onMessage(channel, message);
            return null;
        }).when(mockNotificationLockFactory).onMessage(anyString(), anyString());
        execurtor = Executors.newSingleThreadExecutor();
    }

    private void onMessage(String channel, String message) {
        LOGGER.debug("TEST onMessage channel {} message {}", channel, message);
        messageList.add(new SimpleEntry(channel, message));
        semaphore.release();
        execurtor.shutdown();
        execurtor.shutdownNow();
    }


    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        semaphore.release(1_000);
        messageList.clear();
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Test
    public void createBasic1Test() throws InterruptedException {
        String messageHello = "hello_" + System.currentTimeMillis();
        TestStreamMessageSystem streamMessageSystem = new TestStreamMessageSystem(); // This will not as it receibes what it sends
        streamMessageSystem.sendMessage(messageHello);
        Thread.sleep(50);
        boolean acquired = semaphore.tryAcquire(500, TimeUnit.MILLISECONDS);
        assertFalse(acquired);
        assertEquals(0, messageList.size());
    }

    @Test
    public void createBasic2Test() throws InterruptedException {
        String messageHello = "hello_" + System.currentTimeMillis();
        TestStreamMessageSystem streamMessageSystemReciever = new TestStreamMessageSystem(); // This will receive messages via mock
        TestStreamMessageSystem streamMessageSystemSender = new TestStreamMessageSystem(); // This will not as it receibes what it sends
        Thread.sleep(150);
        streamMessageSystemSender.sendMessage(messageHello);
        Thread.sleep(50);
        boolean acquired = semaphore.tryAcquire(1500, TimeUnit.MILLISECONDS);
        assertTrue(acquired);
        assertEquals(1, messageList.size());
        assertTrue(messageList.get(0).getKey().contains("factoryName"));
        assertEquals(messageHello, messageList.get(0).getValue());
    }

    @Test
    public void createBasic3Test() throws InterruptedException {
        String messageHello = "hello_" + System.currentTimeMillis();
        TestStreamMessageSystem streamMessageSystemReciever = new TestStreamMessageSystem(); // This will receive messages via mock
        TestStreamMessageSystem streamMessageSystemSender = new TestStreamMessageSystem(); // This will not as it receibes what it sends
        execurtor.submit(() -> {
            try {
                Thread.sleep(150);
                streamMessageSystemSender.sendMessage(messageHello);
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        boolean acquired = semaphore.tryAcquire(1500, TimeUnit.MILLISECONDS);
        assertTrue(acquired);
        assertEquals(1, messageList.size());
        assertTrue(messageList.get(0).getKey().contains("factoryName"));
        assertEquals(messageHello, messageList.get(0).getValue());
    }

    @Test
    public void createBasic4Test() throws InterruptedException {
        String messageHello = "hello_" + System.currentTimeMillis();
        TestStreamMessageSystem streamMessageSystemSender = new TestStreamMessageSystem(); // This will not as it receibes what it sends
        streamMessageSystemSender.sendMessage(messageHello);
        execurtor.submit(() -> {
            try {
                TestStreamMessageSystem streamMessageSystemReciever = new TestStreamMessageSystem(); // This will receive messages via mock
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        });
        boolean acquired = semaphore.tryAcquire(1500, TimeUnit.MILLISECONDS);
        assertFalse(acquired);
        assertEquals(0, messageList.size());
    }



    private class TestStreamMessageSystem extends StreamMessageSystem {

        TestStreamMessageSystem() {
            super(mockNotificationLockFactory);
        }

    }

}
