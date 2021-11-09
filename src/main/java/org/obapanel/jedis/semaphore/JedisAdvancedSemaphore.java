package org.obapanel.jedis.semaphore;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.params.SetParams;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This semaphore works, but it is deprecated because its usage is very tricky
 * The acquire method waits por a pub/sub message if permits not obtained.
 * But waiting for a message forces the code to do some strange decisions
 * This is because
 * - We must use a JedisPubSub to listen to the message on the channel
 * - The JedisPubSub subscribe action must be placed into a new background thread
 *   that will be blocked (waiting for events and executing the code) until unsubscribe
 * - Also the JedisPubSub must use other jedis connection than the one used to check the
 *   semaphore. As the release method publish messages, an error will arise if the same connection
 *   is used to publish and subscribe
 *
 * So for a semaphore with messages we must use two connection and a new background thread; and manage them.
 * For this cause, I prefer the JedisSemaphore implementation with polling.
 * But this is here for anyone to use or refactor
 */
@Deprecated
public class JedisAdvancedSemaphore {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisAdvancedSemaphore.class);

    public static final String SEMAPHORE_LUA_SCRIPT = "" +
            "local permits = redis.call('get', KEYS[1]); " + "\n" +
            "if (permits ~= false and tonumber(permits) >= tonumber(ARGV[1])) then " + "\n" +
            "    redis.call('decrby', KEYS[1], ARGV[1]); " + "\n" +
            "    return 'true'; " + "\n" +
            "else " + "\n" +
            "    return 'false'; "+ "\n" +
            "end ";

    private static final String JEDIS_SEMAPHORE_CHANNEL_PREFIX = "JedisSemaphoreChannel:";
    private final JedisPool jedisPool;
    private final Jedis jedisForPubSub;
    private final String name;
    private final String channelName;
    private JedisSemaphorePubSub pubSub;
    private Object lockForPermits;
    private Thread pubSubThread;
    private Executor pubSubExecutor;


    public JedisAdvancedSemaphore(JedisPool jedisPool, String name) {
        this(jedisPool, name, 1);
    }


    public JedisAdvancedSemaphore(JedisPool jedisPool, String name, int initialPermits) {
        this.jedisPool = jedisPool;
        this.jedisForPubSub = jedisPool.getResource();
        this.name = name;
        this.channelName = JEDIS_SEMAPHORE_CHANNEL_PREFIX + name;
        init(initialPermits);
    }

    private void init(int initialPermits){
        if (initialPermits < 0) {
            initialPermits = 0;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(name, String.valueOf(initialPermits), new SetParams().nx());
        }
    }

    public JedisAdvancedSemaphore withExecutor(Executor executor){
        this.pubSubExecutor = executor;
        return this;
    }

    private synchronized void lazySubscribeToChannel() {
        LOGGER.debug("lazySubscribeToChannel");
        if (pubSub == null && lockForPermits == null) {
            lockForPermits = new ReentrantLock();
            if (pubSubExecutor != null) {
                pubSubExecutor.execute(getSubscribeToChannelRunnable());
            } else {
                pubSubThread = new Thread(getSubscribeToChannelRunnable());
                pubSubThread.setDaemon(true);
                pubSubThread.setName("JedisSemaphore.susbscribeThread." + name);
                pubSubThread.start();
            }
        }
    }

    private synchronized Runnable getSubscribeToChannelRunnable() {
        return () -> {
            try {
                pubSub = new JedisSemaphorePubSub();
                jedisForPubSub.subscribe(pubSub, channelName);
            } catch (Exception e) {
                LOGGER.error("subscribe error ", e);
                e.printStackTrace();
            }
        };
    }

    public void acquire() throws InterruptedException {
        acquire(1);
    }

    public void acquire(int permits) throws InterruptedException {
        boolean acquired = redisAcquire(permits);
        while(!acquired) {
            lazySubscribeToChannel();
            synchronized (lockForPermits) {
                lockForPermits.wait();
            }
            acquired = redisAcquire(permits);
        }
    }


    public void release() {
        release(1);
    }

    public void release(int permits) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.incrBy(name, permits);
            jedis.publish(channelName, name);
            LOGGER.debug("release channel {} message {}", channelName, name);
        }
    }

    public boolean tryAcquire() {
        return redisAcquire(1);
    }

    public boolean tryAcquire(int permits) {
        return redisAcquire(permits);
    }

    public boolean tryAcquire(int permits, long timeOut, TimeUnit timeUnit) throws InterruptedException {
        long timeMax = System.currentTimeMillis() + timeUnit.toMillis(timeOut);
        boolean acquired = redisAcquire(permits);
        boolean exired = System.currentTimeMillis() > timeMax;
        while(!acquired && !exired) {
            lazySubscribeToChannel();
            synchronized (lockForPermits) {
                lockForPermits.wait(250);
            }
            exired = System.currentTimeMillis() > timeMax;
            if (!exired) {
                acquired = redisAcquire(permits);
            }
        }
        return acquired;
    }

    private boolean redisAcquire(int permits){
        try (Jedis jedis = jedisPool.getResource()) {
            Object oresult = jedis.eval(SEMAPHORE_LUA_SCRIPT, Arrays.asList(name), Arrays.asList(String.valueOf(permits)));
            String result = (String) oresult;
            return Boolean.parseBoolean(result);
        }
    }

    public int availablePermits() {
        try (Jedis jedis = jedisPool.getResource()) {
            String permits = jedis.get(name);
            if (permits == null || permits.isEmpty()) {
                return -1;
            } else {
                return Integer.parseInt(permits);
            }
        }
    }

    private synchronized void unlockOnMessage(String messageChannelName, String messageName) {
        LOGGER.debug("unlockOnMessage channel {} message {}", messageChannelName, messageName);
        if (channelName.equals(messageChannelName) && name.equals(messageName) && lockForPermits != null) {
            synchronized (lockForPermits) {
                lockForPermits.notify();
            }
        }
    }

    public void invalidate() {
        pubSub.unsubscribe(channelName);
        pubSub = null;
        jedisForPubSub.close();
    }

    public void destroy(){
        invalidate();
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(name);
        }
    }


    class JedisSemaphorePubSub extends JedisPubSub {
        public void onMessage(String channel, String message) {
            unlockOnMessage(channel, message);
        }
    }
}

