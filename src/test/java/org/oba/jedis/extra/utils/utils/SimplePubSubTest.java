package org.oba.jedis.extra.utils.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.oba.jedis.extra.utils.utils.MockOfJedis.unitTestEnabled;

public class SimplePubSubTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimplePubSubTest.class);

    private MockOfJedis mockOfJedis;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
    }

    @After
    public void after() throws IOException {
        mockOfJedis.getJedisPooled().close();
        mockOfJedis.clearData();
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
        LOGGER.debug("onMessageTest publish  channel1 message1 >");
        mockOfJedis.getJedisPooled().publish("channel1", "message1");
        LOGGER.debug("onMessageTest publish  channel1 message1 <");
        boolean acquired = semaphore.tryAcquire(750, TimeUnit.MILLISECONDS);
        assertTrue(acquired);
        assertEquals(1, recolectedData.size());
        assertEquals("channel1", recolectedData.get(0).getKey());
        assertEquals("message1", recolectedData.get(0).getValue());
    }

    private void doSubscribe(SimplePubSub simplePubSub) {
        mockOfJedis.getJedisPooled().subscribe(simplePubSub, "channel1");
    }

}
