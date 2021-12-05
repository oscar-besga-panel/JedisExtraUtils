package org.obapanel.jedis.testing.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestBlpopWithThreads {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestBlpopWithThreads.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private final Object waiter = new Object();
    private String blpopName;
    private JedisPool jedisPool;
    private Jedis jedisOfBlpop;
    private final AtomicReference<String> pong = new AtomicReference<>();
    private final AtomicBoolean blpopEnter = new AtomicBoolean(false);
    private final AtomicBoolean blpopExit = new AtomicBoolean(false);
    private final AtomicReference<String> blpopResult = new AtomicReference<>();
    private final AtomicBoolean interrupted = new AtomicBoolean(false);

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        blpopName = "blpop:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        jedisPool = jtfTest.createJedisPool();
    }

    @After
    public void after() {
        if (jedisPool != null) jedisPool.close();
        if (scheduler != null) scheduler.shutdown();
    }

    private void doBlpop() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedisOfBlpop = jedis;
            LOGGER.info("blpop {}", blpopName);
            blpopExit.set(false);
            blpopEnter.set(true);
            List<String> result = jedis.blpop(blpopName, "0");
            blpopExit.set(true);
            LOGGER.info("blpopResult {}", result);
            if (result != null && result.size() == 2) {
                blpopResult.set(result.get(0) + ":" + result.get(1));
            }
            LOGGER.info("waiter notify");
            synchronized (waiter) {
                waiter.notify();
            }

        } catch (Exception e) {
            LOGGER.error("doBlpop Exception ", e);
        }
    }

    private void doOtherThings() {
        try (Jedis jedis = jedisPool.getResource()) {
            LOGGER.info("waiter wait");
            synchronized (waiter) {
                waiter.wait();
            }
            String pongReponse = jedis.ping();
            pong.set(pongReponse);
            LOGGER.info("pong {}", pong.get());
        } catch (InterruptedException e) {
            interrupted.set(true);
            LOGGER.info("Interrupted e " + e);
        } catch (Exception e) {
            LOGGER.error("Exception ", e);
        }
    }

    private void sendData() {
        try (Jedis jedis = jedisPool.getResource()) {
            LOGGER.info("rpush send");
            jedis.rpush(blpopName, "XXX");
            LOGGER.info("rpush sent");
        } catch (Exception e) {
            LOGGER.error("sendData Exception ", e);
        }
    }

    private void cancelJedisOfBlpop() {
        LOGGER.info("cancel jedis of blpop");
        if (jedisOfBlpop != null) {
            jedisOfBlpop.close();
            jedisOfBlpop.quit();
            LOGGER.info("cancelled jedis of blpop");
        } else {
            LOGGER.info("cancelled jedis of blpop because it was null");
        }
    }

    @Test
    public void testBlpopInterrupt() throws InterruptedException {
        LOGGER.info("testBlpopInterrupt init");
        ScheduledFuture doOtherThingsFuture = scheduler.schedule(this::doOtherThings, 10, TimeUnit.MILLISECONDS);
        ScheduledFuture doBlpopFuture = scheduler.schedule(this::doBlpop, 150, TimeUnit.MILLISECONDS);
        Thread.sleep(300);
        assertFalse(doOtherThingsFuture.isDone());
        assertFalse(doBlpopFuture.isDone());
        assertFalse(interrupted.get());
        assertNull(pong.get());
        assertNull(blpopResult.get());
        assertTrue(blpopEnter.get());
        assertFalse(blpopExit.get());

        LOGGER.info("testBlpopInterrupt cancel");
        doBlpopFuture.cancel(true);
        doOtherThingsFuture.cancel(true);
        Thread.sleep(500);

        // Those will returb true because are cancelled, but doBlpopFuture is waiting on blop command
        // assertTrue(doOtherThingsFuture.isDone());
        // assertFalse(doBlpopFuture.isDone());
        assertTrue(interrupted.get());
        assertNull(pong.get());
        assertNull(blpopResult.get());
        assertTrue(blpopEnter.get());
        assertFalse(blpopExit.get());

    }

    @Test
    public void testBlpopInterruptCancelling() throws InterruptedException {
        LOGGER.info("testBlpopInterrupt init");
        ScheduledFuture doOtherThingsFuture = scheduler.schedule(this::doOtherThings, 10, TimeUnit.MILLISECONDS);
        ScheduledFuture doBlpopFuture = scheduler.schedule(this::doBlpop, 150, TimeUnit.MILLISECONDS);
        ScheduledFuture doCancelJedisOfBlpop = scheduler.schedule(this::cancelJedisOfBlpop, 500, TimeUnit.MILLISECONDS);
        Thread.sleep(300);
        assertFalse(doOtherThingsFuture.isDone());
        assertFalse(doBlpopFuture.isDone());
        assertFalse(doCancelJedisOfBlpop.isDone());
        assertFalse(interrupted.get());
        assertNull(pong.get());
        assertNull(blpopResult.get());
        assertTrue(blpopEnter.get());
        assertFalse(blpopExit.get());

        LOGGER.info("testBlpopInterruptCancelling cancel");
        doBlpopFuture.cancel(true);
        //doOtherThingsFuture.cancel(true);
        Thread.sleep(5500);

        // Those will return true because are cancelled, but doBlpopFuture is waiting on blop command
        // assertTrue(doOtherThingsFuture.isDone());
        // assertFalse(doBlpopFuture.isDone());

        // Will not work as jedis.close and jedis.quit will not close the connection for blpop

//        assertTrue(doCancelJedisOfBlpop.isDone());
//        assertFalse(interrupted.get());
//        assertNull(blpopResult.get());
//        assertTrue(blpopEnter.get());
//        assertFalse(blpopExit.get());
//        assertEquals("PONG", pong.get());
    }

    @Test
    public void testBlpopSend() throws InterruptedException {
        LOGGER.info("testBlpopSend init");
        ScheduledFuture doOtherThingsFuture = scheduler.schedule(this::doOtherThings, 10, TimeUnit.MILLISECONDS);
        ScheduledFuture doBlpopFuture = scheduler.schedule(this::doBlpop, 150, TimeUnit.MILLISECONDS);
        ScheduledFuture sendDataFuture = scheduler.schedule(this::sendData, 500, TimeUnit.MILLISECONDS);
        Thread.sleep(300);
        assertFalse(doOtherThingsFuture.isDone());
        assertFalse(doBlpopFuture.isDone());
        assertFalse(sendDataFuture.isDone());
        assertFalse(interrupted.get());
        assertNull(pong.get());
        assertNull(blpopResult.get());
        assertTrue(blpopEnter.get());
        assertFalse(blpopExit.get());

        LOGGER.info("testBlpopSend cancel");
        doBlpopFuture.cancel(true);
        Thread.sleep(500);

        assertTrue(doOtherThingsFuture.isDone());
        assertTrue(doBlpopFuture.isDone());
        assertTrue(sendDataFuture.isDone());
        assertFalse(interrupted.get());
        assertEquals("PONG", pong.get());
        assertEquals(blpopName + ":XXX", blpopResult.get());
        assertTrue(blpopEnter.get());
        assertTrue(blpopExit.get());
    }

}
