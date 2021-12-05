package org.obapanel.jedis.interruptinglocks;

import org.obapanel.jedis.utils.OnMensaggePubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;

import static org.obapanel.jedis.utils.OnMensaggePubSub.subcribeOnMessage;

public class JedisLockWithNotification extends JedisLock {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisLockWithNotification.class);

    public static final String CHANNEL_NAME = "JedisLockWithNotification";
    public static final String BLPOP_PREFIX_NAME = "JedisLockWithNotification_";

    private final String blpopName;
    private final Object waiter = new Object();
    private long waitNotificationTime = 30L;
    private TimeUnit waitNotificationUnit = TimeUnit.SECONDS;

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
        long currentWaitNotificationTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(timeInMilis);
        redisWaitNotification(currentWaitNotificationTimeSeconds);
    }

    private void redisWaitNotification() throws InterruptedException {
        long currentWaitNotificationTimeSeconds = waitNotificationUnit.toSeconds(waitNotificationTime);
        redisWaitNotification(currentWaitNotificationTimeSeconds);
    }

    private void redisWaitNotification(long currentWaitNotificationTimeSeconds) throws InterruptedException {
        if (currentWaitNotificationTimeSeconds == 0){
            currentWaitNotificationTimeSeconds = 1;
        }
        int icurrentWaitNotificationTimeSeconds = Long.valueOf(currentWaitNotificationTimeSeconds).intValue();
        JedisPool jedisPool = getJedisPool();
        try(Jedis jedis = jedisPool.getResource()) {
            LOGGER.debug("redisWaitNotification blpop {} {} >", blpopName, icurrentWaitNotificationTimeSeconds);
            jedis.blpop(icurrentWaitNotificationTimeSeconds, blpopName);
            LOGGER.debug("redisWaitNotification blpop {} {} <", blpopName, icurrentWaitNotificationTimeSeconds);
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


}
