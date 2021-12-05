package org.obapanel.jedis.interruptinglocks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class JedisLockWithNotification extends JedisLock {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisLockWithNotification.class);

    public static final String BLPOP_PREFIX_NAME = "JedisLockWithNotification:";


    private static final AtomicLong LOCKS_NUM = new AtomicLong();

    private final long lockNum = LOCKS_NUM.incrementAndGet();

    private final Object waiter = new Object();
    private long waitNotificationTime = 30_000L;
    private TimeUnit waitNotificationUnit = TimeUnit.MILLISECONDS;
    private final String blpopName;
    private Jedis jedisBlopConnection;
    private Thread jedisSubscribeThread;

    public JedisLockWithNotification(JedisPool jedisPool, String name) {
        super(jedisPool, name);
        this.blpopName = BLPOP_PREFIX_NAME + name;
    }

    public JedisLockWithNotification(JedisPool jedisPool, String name, Long leaseTime, TimeUnit timeUnit) {
        super(jedisPool, name, leaseTime, timeUnit);
        this.blpopName = BLPOP_PREFIX_NAME + name;
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
        redisWaitNotification();
    }

    protected void doSleepBetweenAttempts(long timeInMilis) throws InterruptedException {
        redisWaitNotification(timeInMilis - 1);
    }


    private void redisWaitNotification() throws InterruptedException {
        long currentWaitNotificationMilis = waitNotificationUnit.toMillis(waitNotificationTime);
        redisWaitNotification(currentWaitNotificationMilis);
    }

    private void redisWaitNotification(long currentWaitNotificationMilis) throws InterruptedException {
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> redisWaitNotification");
        createBackgroundDaemon(() ->
                redisBlopBackground(currentWaitNotificationMilis)
        );
        synchronized (waiter) {
            LOGGER.debug("redisWaitNotification wait > {} {} >", blpopName, currentWaitNotificationMilis);
            waiter.wait(currentWaitNotificationMilis);
            LOGGER.debug("redisWaitNotification wait < {} {} <", blpopName, currentWaitNotificationMilis);
        }
    }

    private void createBackgroundDaemon(Runnable subscribeAction) {
        if (jedisSubscribeThread == null) {
            jedisSubscribeThread = new Thread(subscribeAction);
            jedisSubscribeThread.setDaemon(true);
            jedisSubscribeThread.setName("jedisBlpopThread_" + lockNum + "_" + blpopName + "_" + System.currentTimeMillis());
            jedisSubscribeThread.start();
        }
    }

    private void redisBlopBackground(long currentWaitNotificationMilis) {
        int seconds = Long.valueOf(TimeUnit.MILLISECONDS.toSeconds(currentWaitNotificationMilis)).intValue();
        if (seconds == 0) {
            seconds = 1;
        }
        JedisPool jedisPool = getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            jedisBlopConnection = jedis;
            LOGGER.debug("redisWaitNotification  blpop > {} {} {} >", blpopName, seconds, lockNum);
            jedis.blpop(seconds, blpopName);
            LOGGER.debug("redisWaitNotification blpop < {} {} {} >", blpopName, seconds, lockNum);
        } catch (Exception e) {
            LOGGER.debug("redisWaitNotification exception e",e);
        } finally {
            jedisBlopConnection = null;
            synchronized (waiter) {
                waiter.notify();
                LOGGER.debug("redisWaitNotification notify <> {} {} <>", blpopName, seconds);
            }
        }
    }

    @Override
    public synchronized void unlock() {
        super.unlock();
        redisNotificationUnlock();
    }

    private void redisNotificationUnlock() {
        String message = Long.toString(System.currentTimeMillis());
        JedisPool jedisPool = getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.rpush(blpopName, message);
            LOGGER.debug("redisNotificationUnlock rpush {} {}", blpopName, message);
        }
    }

    protected void finalize() {
        boolean jedisBlopConnectionStatus = jedisBlopConnection == null;
        boolean isThreadNull = jedisSubscribeThread == null;
        boolean isThreadAlive = !isThreadNull && jedisSubscribeThread.isAlive();
        String threadSatus = !isThreadNull ? jedisSubscribeThread.getState().toString() : "";
        LOGGER.debug("finalize JedisLockWithNotification jedisBlopConnectionStatus {} {} {} {}", jedisBlopConnectionStatus, isThreadNull, isThreadAlive, threadSatus);
    }

}
