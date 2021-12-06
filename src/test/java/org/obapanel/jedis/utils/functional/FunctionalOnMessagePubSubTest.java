package org.obapanel.jedis.utils.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.obapanel.jedis.utils.OnMessagePubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FunctionalOnMessagePubSubTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalJedisConnectionProxyTest.class);

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPool jedisPool;
    private String varName;
    private OnMessagePubSub onMessagePubSub;
    private final AtomicReference<String> refChannel = new AtomicReference<>();
    private final AtomicReference<String> refMessage = new AtomicReference<>();
    private final Object waiter = new Object();


    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
        varName = "OnMessagePubSub:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (onMessagePubSub != null && onMessagePubSub.isSubscribed()) {
            onMessagePubSub.unsubscribe();
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    void doOnMessage(String channel, String message) {
        try {
            LOGGER.info("doOnMessage channel {} message {}", channel, message);
            refChannel.set(channel);
            refMessage.set(message);
            synchronized (waiter) {
                waiter.notify();
            }
            LOGGER.info("doOnMessage exit channel {} message {}", channel, message);
        } catch (Exception e) {
            LOGGER.error("doOnMessage error", e);
        }
    }

    void doSubscribe() {
        try (Jedis jedis = jedisPool.getResource()) {
            LOGGER.info("messagePubSub subscribe {}", varName);
            onMessagePubSub = new OnMessagePubSub(this::doOnMessage);
            jedis.subscribe(onMessagePubSub, varName);
            LOGGER.info("messagePubSub subscribe exit {}", varName);
        } catch (Exception e) {
            LOGGER.error("messagePubSub subscribe error", e);
        }
    }

    void doSendMessage() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish(varName, "FunctionalOnMessagePubSubTest");
            LOGGER.info("doSendMessage channel {} message {} ", varName, "FunctionalOnMessagePubSubTest");
        } catch (Exception e) {
            LOGGER.error("doSendMessage error", e);
        }
    }

    @Test
    public void testONMessage() throws InterruptedException {
        long t = System.currentTimeMillis();
        LOGGER.info("testONMessage init");
        scheduler.schedule(this::doSubscribe, 5, TimeUnit.MILLISECONDS);
        scheduler.schedule(this::doSendMessage, 250, TimeUnit.MILLISECONDS);
        LOGGER.info("testONMessage wait");
        synchronized (waiter) {
            waiter.wait(2500);
        }
        LOGGER.info("testONMessage unwait");
        assertTrue(onMessagePubSub.isSubscribed());
        assertEquals(varName, refChannel.get());
        assertEquals("FunctionalOnMessagePubSubTest", refMessage.get());
        assertTrue( 2500 > (System.currentTimeMillis() - t) );
        Thread.sleep(10);
        if (onMessagePubSub != null) {
            onMessagePubSub.unsubscribe();
        }
        Thread.sleep(100); // Needed
        assertFalse(onMessagePubSub.isSubscribed());

    }

}
