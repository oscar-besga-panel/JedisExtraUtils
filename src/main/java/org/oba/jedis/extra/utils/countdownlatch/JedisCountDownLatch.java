package org.oba.jedis.extra.utils.countdownlatch;

import org.oba.jedis.extra.utils.utils.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.util.concurrent.TimeUnit;

/**
 * Jedis implemetation of a CountDownLatch
 *
 * It makes some threads, in one or many different processes, wait until its counter reaches zero.
 * Then, all threads can resume the execution.
 * All the CountDownLatch on different threads and processes must share the same name to synchronize.
 *
 * The first created CountDownLatch assings the intial count to the shared value
 *
 * In this implementation, a thread that is waiting checks periodically the value of the counter in redis (polling)
 *
 *
 */
public class JedisCountDownLatch implements Named {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisCountDownLatch.class);
    private static final Long LONG_NULL_VALUE = -1L;


    private final JedisPool jedisPool;
    private final String name;
    private int waitTimeMilis = 150;


    /**
     * Creates a new shared CountDownLatch
     * @param jedisPool Jedis connection pool
     * @param name Shared name
     * @param count Initial count
     */
    public JedisCountDownLatch(JedisPool jedisPool, String name, long count) {
        this.jedisPool = jedisPool;
        this.name = name;
        init(count);
    }


    /**
     * Sets the waiting time between queries on Redis while waiting
     * @param waitTimeMilis time to wait in miliseconds
     * @return this
     */
    public JedisCountDownLatch withWaitingTimeMilis(int waitTimeMilis){
        this.waitTimeMilis = waitTimeMilis;
        return this;
    }

    /**
     * Checks if count is more than zero and creates the shared value if doesn't exists
     * @param count Initial count
     */
    private void init(long count){
        if (count <= 0) {
            throw new IllegalArgumentException("initial count on countdownlatch must be always more than zero");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(name, String.valueOf(count), new SetParams().nx());
        }
    }

    public String getName() {
        return name;
    }

    /**
     * Wait until interrupted or shared value reaches zero
     * @throws InterruptedException if interrupted
     */
    public void await() throws InterruptedException {
        while(!isCountZero()) {
            Thread.sleep(waitTimeMilis);
        }
    }

    /**
     * Wait until interrupted or shared value reaches zero or time passes
     * @param timeout Maximim time to wait
     * @param unit wait time unit
     * @return true if counter has reached zero, false if maximum wait time reached
     * @throws InterruptedException if interrupted
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        long timeStampToWait = System.currentTimeMillis() + unit.toMillis(timeout);
        boolean reachedZero = isCountZero();
        while(timeStampToWait > System.currentTimeMillis() && !reachedZero) {
            Thread.sleep(waitTimeMilis);
            reachedZero = isCountZero();
        }
        return reachedZero;
    }


    /**
     * Decreases by one unit the share value
     * @return the current value, after operation
     */
    public long countDown() {
        try (Jedis jedis = jedisPool.getResource()) {
            long value = jedis.decr(name);
            LOGGER.debug("countDown name {} value {}", name, value);
            return value;
        }
    }

    private boolean isCountZero() {
        return getCount() <= 0;
    }

    /**
     * Get the current shared value, or -1 if it doen't exists
     * @return current value
     */
    public long getCount() {
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(name);
            LOGGER.debug("getCount name {} value {}", name, value);
            if (value != null && !value.isEmpty()) {
                return Long.parseLong(value);
            } else {
                return LONG_NULL_VALUE;
            }
        }
    }

    /**
     * CAUTION !!
     * THIS METHOD DELETES THE REMOTE VALUE DESTROYING THIS COUNTDOWNLATCH AND OHTERS
     * USE AT YOUR OWN RISK WHEN ALL POSSIBLE OPERATIONS ARE FINISHED
     */
    public void destroy() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(name);
        }
    }


}
