package org.oba.jedis.extra.utils.streamMessageSystem;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.oba.jedis.extra.utils.utils.SimpleEntry;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class, TransactionBase.class })
public class StreamMessageSystemTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamMessageSystemTest.class);

    private MockOfJedis mockOfJedis;
    private JedisPool jedisPool;
    private String factoryName;
    private Semaphore semaphore;
    private List<SimpleEntry> messageList;
    private ExecutorService execurtor;


    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        jedisPool = mockOfJedis.getJedisPool();
        factoryName = "factoryName:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        semaphore = new Semaphore(0);
        messageList = new ArrayList<>();
        execurtor = Executors.newSingleThreadExecutor();
    }

    private void executeOnMessage(String message) {
        LOGGER.debug("TEST onMessage message {}", message);
        messageList.add(new SimpleEntry(factoryName, message));
        semaphore.release();
        execurtor.shutdown();
        execurtor.shutdownNow();
    }


    @After
    public void after() throws IOException {
        if (!MockOfJedis.unitTestEnabled()) return;
        semaphore.release(1_000);
        messageList.clear();
        if (jedisPool != null) {
            jedisPool.close();
        }
        mockOfJedis.clearData();
    }


    @Test
    public void createBasic1Test() throws InterruptedException {
        String messageHello = "hello_" + System.currentTimeMillis();
        TestStreamMessageSystem streamMessageSystem = new TestStreamMessageSystem(); // This will not as it receibes what it sends
        streamMessageSystem.sendMessage(messageHello);
        Thread.sleep(150);
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
            super("TestStreamMessageSystem", jedisPool, StreamMessageSystemTest.this::executeOnMessage);
        }
    }

}
