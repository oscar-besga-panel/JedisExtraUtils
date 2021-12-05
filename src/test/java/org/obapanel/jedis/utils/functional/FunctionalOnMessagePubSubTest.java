package org.obapanel.jedis.utils.functional;

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
        onMessagePubSub = new OnMessagePubSub(this::doOnMessage);
    }

    void doOnMessage(String channel, String message) {
        LOGGER.info("FunctionalOnMessagePubSubTest doOnMessage channel {} message {}", channel, message);
        refChannel.set(channel);
        refMessage.set(message);
        waiter.notify();
    }

    void doSubscribe() {
        try (Jedis jedis = jedisPool.getResource()) {
            LOGGER.info("FunctionalOnMessagePubSubTest messagePubSub subscribe {}", varName);
            jedis.subscribe(onMessagePubSub, varName);
        }
    }

    void doSendMessage() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish(varName, "FunctionalOnMessagePubSubTest");
            LOGGER.info("FunctionalOnMessagePubSubTest doSendMessage channel {} message {} ", varName, "FunctionalOnMessagePubSubTest");
        }
    }


        @Test
        public void testONMessage() throws InterruptedException {
            long t = System.currentTimeMillis();
            LOGGER.info("testONMessage init");
            scheduler.schedule(this::doSubscribe, 5, TimeUnit.MILLISECONDS);
            scheduler.schedule(this::doSendMessage, 250, TimeUnit.MILLISECONDS);
            synchronized (waiter) {
                waiter.wait(5000);
            }
            assertEquals(varName, refChannel.get());
            assertEquals("FunctionalOnMessagePubSubTest", refMessage.get());
            assertTrue( 5000 > (System.currentTimeMillis() - t) );
        }


}
