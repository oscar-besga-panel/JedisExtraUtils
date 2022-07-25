package org.obapanel.jedis.utils.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.obapanel.jedis.utils.SimpleEntry;
import org.obapanel.jedis.utils.SimplePubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FunctionalSimplePubSubTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalSimplePubSubTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPool jedisPool;
    private String channelName;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
        channelName = "channel:" + this.getClass().getName() + ":" + System.currentTimeMillis() + "_";
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPool != null) {
            jedisPool.close();
        }
    }


    @Test
    public void onMessageTest() throws InterruptedException {
        final List<SimpleEntry> recolectedData = new ArrayList<>();
        final Semaphore semaphore = new Semaphore(0);
        SimplePubSub simplePubSub = new SimplePubSub( (c, m) -> {
            LOGGER.debug("onMessageTest simplePubSub c {} m {} >", c, m);
            recolectedData.add(new SimpleEntry(c,m));
            semaphore.release();
            LOGGER.debug("onMessageTest simplePubSub c {} m {} <", c, m);
        } );
        Thread t = new Thread(() -> this.doSubscribe(simplePubSub));
        t.setDaemon(true);
        t.setName("doSubscribe");
        t.start();
        Thread.sleep(250);
        try(Jedis jedis = jedisPool.getResource()) {
            LOGGER.debug("onMessageTest publish  {} message1{} >", channelName, channelName);
            jedis.publish(channelName, "message1" + channelName);
            LOGGER.debug("onMessageTest publish  {} message1{} <", channelName, channelName);
        }
        boolean acquired = semaphore.tryAcquire(750, TimeUnit.MILLISECONDS);
        assertTrue(acquired);
        assertEquals(1, recolectedData.size());
        assertEquals(channelName, recolectedData.get(0).getKey());
        assertEquals("message1" + channelName, recolectedData.get(0).getValue());
    }

    private void doSubscribe(SimplePubSub simplePubSub) {
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.subscribe(simplePubSub, channelName);
        }
    }

}
