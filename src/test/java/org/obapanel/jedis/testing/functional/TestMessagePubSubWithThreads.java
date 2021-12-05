package org.obapanel.jedis.testing.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.obapanel.jedis.utils.OnMessagePubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

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

public class TestMessagePubSubWithThreads {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestMessagePubSubWithThreads.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private final Object waiter = new Object();
    private OnMessagePubSub onMessagePubSub;
    private String messagePubSubName;
    private JedisPool jedisPool;
    private Jedis jedisOfMessagePubSub;
    private final AtomicReference<String> pong = new AtomicReference<>();
    private final AtomicBoolean messagePubSubEnter = new AtomicBoolean(false);
    private final AtomicBoolean messagePubSubExit = new AtomicBoolean(false);
    private final AtomicReference<String> messagePubSubResult = new AtomicReference<>();
    private final AtomicBoolean interrupted = new AtomicBoolean(false);

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        messagePubSubName = "messagePubSub:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        jedisPool = jtfTest.createJedisPool();
    }

    @After
    public void after() {
        if (jedisPool != null) jedisPool.close();
        if (scheduler != null) scheduler.shutdown();
    }

    private void doSubscribe() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedisOfMessagePubSub = jedis;
            LOGGER.info("doSubscribe messagePubSub subscribe {}", messagePubSubName);
            messagePubSubExit.set(false);
            messagePubSubEnter.set(true);
            onMessagePubSub = new OnMessagePubSub(this::doOnMessage);
            jedis.subscribe(onMessagePubSub, messagePubSubName);
            messagePubSubExit.set(true);
            LOGGER.info("doSubscribe exit ");
        } catch (Exception e) {
            LOGGER.error("doSubscribe Exception ", e);
        }
    }

    private void doOnMessage(String channel, String message) {
        LOGGER.info("doOnMessage {}:{}", channel, message);
        if (messagePubSubName.equals(channel) && messagePubSubName.equals(message)){
            messagePubSubResult.set(channel + ":" + message);
            LOGGER.info("waiter notify");
            synchronized (waiter) {
                waiter.notify();
            }
            if (jedisOfMessagePubSub != null && onMessagePubSub != null) {
                onMessagePubSub.unsubscribe();
            }
        }
    }

    private void doWaitUnitlMessageComesOrInterrupted() {
        try {
            LOGGER.info("waiter wait");
            synchronized (waiter) {
                waiter.wait();
            }
            try (Jedis jedis = jedisPool.getResource()) {
                String pongReponse = jedis.ping();
                pong.set(pongReponse);
                LOGGER.info("pong {}", pong.get());
            }
        } catch (InterruptedException e) {
            interrupted.set(true);
            LOGGER.info("Interrupted e " + e);
        } catch (Exception e) {
            LOGGER.error("Exception ", e);
        }
    }

    private void sendData() {
        try (Jedis jedis = jedisPool.getResource()) {
            LOGGER.info("message send");
            jedis.publish(messagePubSubName, messagePubSubName);
            LOGGER.info("message sent");
        } catch (Exception e) {
            LOGGER.error("sendData Exception ", e);
        }
    }

    private void cancelSubscription() {
        LOGGER.info("cancel subscription");
        if (jedisOfMessagePubSub != null && onMessagePubSub != null) {
            onMessagePubSub.unsubscribe();
//            jedisOfMessagePubSub.close();
//            jedisOfMessagePubSub.quit();
            LOGGER.info("cancelled subscription");
        } else {
            LOGGER.info("cancelled subscription because it was null");
        }
    }

    @Test
    public void testMessagePubSubInterrupt() throws InterruptedException {
        LOGGER.info("testBlpopInterrupt init");
        ScheduledFuture doOtherThingsFuture = scheduler.schedule(this::doWaitUnitlMessageComesOrInterrupted, 10, TimeUnit.MILLISECONDS);
        ScheduledFuture doSubscribeFuture = scheduler.schedule(this::doSubscribe, 150, TimeUnit.MILLISECONDS);
        Thread.sleep(300);
        assertFalse(doOtherThingsFuture.isDone());
        assertFalse(doSubscribeFuture.isDone());
        assertFalse(interrupted.get());
        assertNull(pong.get());
        assertNull(messagePubSubResult.get());
        assertTrue(messagePubSubEnter.get());
        assertFalse(messagePubSubExit.get());

        LOGGER.info("testBlpopInterrupt cancel");
        doSubscribeFuture.cancel(true);
        doOtherThingsFuture.cancel(true);
        Thread.sleep(500);

        // Those will returb true because are cancelled, but doBlpopFuture is waiting on blop command
        // assertTrue(doOtherThingsFuture.isDone());
        // assertFalse(doBlpopFuture.isDone());
        assertTrue(interrupted.get());
        assertNull(pong.get());
        assertNull(messagePubSubResult.get());
        assertTrue(messagePubSubEnter.get());
        assertFalse(messagePubSubExit.get());
        assertEquals(1, jedisPool.getNumActive());
    }

    @Test
    public void testMessagePubSubInterruptCancelling() throws InterruptedException {
        LOGGER.info("testBlpopInterrupt init");
        ScheduledFuture doOtherThingsFuture = scheduler.schedule(this::doWaitUnitlMessageComesOrInterrupted, 10, TimeUnit.MILLISECONDS);
        ScheduledFuture doSubscribeFuture = scheduler.schedule(this::doSubscribe, 150, TimeUnit.MILLISECONDS);
        ScheduledFuture doCancelSubscription = scheduler.schedule(this::cancelSubscription, 500, TimeUnit.MILLISECONDS);
        Thread.sleep(300);
        assertFalse(doOtherThingsFuture.isDone());
        assertFalse(doSubscribeFuture.isDone());
        assertFalse(doCancelSubscription.isDone());
        assertFalse(interrupted.get());
        assertNull(pong.get());
        assertNull(messagePubSubResult.get());
        assertTrue(messagePubSubEnter.get());
        assertFalse(messagePubSubExit.get());

        LOGGER.info("testBlpopInterruptCancelling cancel");
        doSubscribeFuture.cancel(true);
        //doOtherThingsFuture.cancel(true);
        Thread.sleep(2500);

        // Those will return true because are cancelled, but doBlpopFuture is waiting on blop command
        // assertTrue(doOtherThingsFuture.isDone());
//         assertFalse(doSubscribeFuture.isDone());

        // Will  work as message pub sub can be interrupted

        assertTrue(doCancelSubscription.isDone());
        assertFalse(interrupted.get());
        assertNull(messagePubSubResult.get());
        assertTrue(messagePubSubEnter.get());
        assertTrue(messagePubSubExit.get());
        assertNull(pong.get());
        assertEquals(0, jedisPool.getNumActive());

        doOtherThingsFuture.cancel(true);
        Thread.sleep(1500);
        assertTrue(interrupted.get());
    }

    @Test
    public void testMessagePubSubSend() throws InterruptedException {
        LOGGER.info("testMessagePubSubSend init");
        ScheduledFuture doOtherThingsFuture = scheduler.schedule(this::doWaitUnitlMessageComesOrInterrupted, 10, TimeUnit.MILLISECONDS);
        ScheduledFuture doSubscribeFuture = scheduler.schedule(this::doSubscribe, 150, TimeUnit.MILLISECONDS);
        ScheduledFuture sendDataFuture = scheduler.schedule(this::sendData, 500, TimeUnit.MILLISECONDS);
        Thread.sleep(300);
        assertFalse(doOtherThingsFuture.isDone());
        assertFalse(doSubscribeFuture.isDone());
        assertFalse(sendDataFuture.isDone());
        assertFalse(interrupted.get());
        assertNull(pong.get());
        assertNull(messagePubSubResult.get());
        assertTrue(messagePubSubEnter.get());
        assertFalse(messagePubSubExit.get());

        LOGGER.info("testMessagePubSubSend cancel");
        doSubscribeFuture.cancel(true);
        Thread.sleep(500);

        assertTrue(doOtherThingsFuture.isDone());
        assertTrue(doSubscribeFuture.isDone());
        assertTrue(sendDataFuture.isDone());
        assertFalse(interrupted.get());
        assertEquals("PONG", pong.get());
        assertEquals(messagePubSubName + ":" + messagePubSubName, messagePubSubResult.get());
        assertTrue(messagePubSubEnter.get());
        assertTrue(messagePubSubExit.get());
        assertEquals(0, jedisPool.getNumActive());
    }

}
