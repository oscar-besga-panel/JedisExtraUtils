package org.obapanel.jedis.interruptinglocks;

import org.obapanel.jedis.utils.OnMessagePubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;


public class JedisLockWithNotification extends JedisLock {


    private static final Logger LOGGER = LoggerFactory.getLogger(JedisLockWithNotification.class);

    public static final String CHANNEL_NAME = "JedisLockWithNotification";

    private final Object waiter = new Object();
    private long waitNotificationTime = 30L;
    private TimeUnit waitNotificationUnit = TimeUnit.SECONDS;

    public JedisLockWithNotification(JedisPool jedisPool, String name) {
        super(jedisPool, name);
    }

    public JedisLockWithNotification(JedisPool jedisPool, String name, Long leaseTime, TimeUnit timeUnit) {
        super(jedisPool, name, leaseTime, timeUnit);
        setWaitNotificationTime(leaseTime / 2, timeUnit);
    }

    JedisLockWithNotification withWaitNotificationTime(long time, TimeUnit unit){
        setWaitNotificationTime(time, unit);
        return  this;
    }

    private void setWaitNotificationTime(long time, TimeUnit unit) {
        this.waitNotificationTime = time;
        this.waitNotificationUnit = unit;
    }

    protected void doSleepBetweenAttempts() throws InterruptedException {
        //long currentWaitNotificationMilis = waitNotificationUnit.toMillis(waitNotificationTime);
        //redisWaitNotification(currentWaitNotificationMilis);
        redisWaitNotification(0L);
    }

    protected void doSleepBetweenAttempts(long timeInMilis) throws InterruptedException {
        redisWaitNotification(timeInMilis - 1);
    }


    private void redisWaitNotification(long currentWaitNotificationMilis) throws InterruptedException {
        OnMessagePubSub onMessagePubSub = new OnMessagePubSub(this::redisWaitNotificationOnMessage);
        try {
            createBackgroundDaemon(() ->
                    redisWaitNotificationSubscribeBackground(onMessagePubSub)
            );
            synchronized (waiter) {
                if (currentWaitNotificationMilis == 0) {
                    waiter.wait();
                } else {
                    waiter.wait(currentWaitNotificationMilis);
                }
            }
        } finally {
            onMessagePubSub.unsubscribe();
        }
    }

    private void createBackgroundDaemon(Runnable subscribeAction) {
        Thread jedisSubscribeThread = new Thread(subscribeAction);
        jedisSubscribeThread.setDaemon(true);
        jedisSubscribeThread.setName("jedisSubscribeThread_" + CHANNEL_NAME + "_"  + System.currentTimeMillis());
        jedisSubscribeThread.start();
    }

    /**
     * The background thread executing this in mandatory, because subscribe must use
     * @param onMensaggePubSub PubSub
     */
    private void redisWaitNotificationSubscribeBackground(OnMessagePubSub onMensaggePubSub) {
        JedisPool jedisPool = getJedisPool();
        try(Jedis jedis = jedisPool.getResource()){
            jedis.subscribe(onMensaggePubSub, CHANNEL_NAME);
        }
        LOGGER.debug("end jedisSubscribeThread {}", Thread.currentThread().getName());
    }

    private void redisWaitNotificationOnMessage(String channel, String message) {
        String name = getName();
        if (channel.equals(CHANNEL_NAME) && message.equals(name)){
            LOGGER.debug("redisWaitNotificationOnMessage {} {}", channel, message);
            synchronized (waiter) {
                waiter.notify();
            }
        }
    }

    @Override
    public synchronized void unlock() {
        super.unlock();
        redisNotificationUnlock();
    }

    private void redisNotificationUnlock() {
        JedisPool jedisPool = getJedisPool();
        String name = getName();
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish(CHANNEL_NAME, name);
            LOGGER.debug("redisNotificationUnlock publish {} {}", CHANNEL_NAME, name);
        }
    }

}
