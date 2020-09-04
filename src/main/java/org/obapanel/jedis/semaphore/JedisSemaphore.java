package org.obapanel.jedis.semaphore;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/*
http://tutorials.jenkov.com/java-util-concurrent/semaphore.html
https://github.com/redisson/redisson/blob/master/redisson/src/main/java/org/redisson/RedissonSemaphore.java
https://www.javadoc.io/static/org.redisson/redisson/3.11.6/index.html?org/redisson/RedissonReadWriteLock.html
https://github.com/redisson/redisson/wiki/8.-distributed-locks-and-synchronizers#86-semaphore

https://gist.github.com/FredrikWendt/3343861
https://www.alibabacloud.com/help/doc-detail/26368.htm
 */



/**
 * A redis/jedis implementation of a Java semaphore that is distributed over various processes/threads
 * It works like a semaphore, but the semaphore counter is stored in redis and operated by all the
 * threads/processes that use the same name (on the same server of course )
 *
 * A semaphore is identified and shared by its name
 *
 * The first semaphore that is created in redis assings the intial permits to the redis shared value.
 * Only the first assing value
 * Next semaphores initial value isn't used on redis.
 *
 * When waiting to retrieve permits if not avalible at the moment, the semaphore will
 * check against redis every so ofter - polling
 * No messages will be involved, the semaphore must check redis periodically until the
 * permits are avalible or interrupted.
 *
 *
 *
 */
public class JedisSemaphore {

    private static final Logger LOG = LoggerFactory.getLogger(JedisSemaphore.class);

    // Thanks to redisson semaphore for guidance
    // https://github.com/redisson/redisson/blob/master/redisson/src/main/java/org/redisson/RedissonSemaphore.java
    // https://www.javadoc.io/static/org.redisson/redisson/3.11.6/index.html?org/redisson/RedissonReadWriteLock.html
    public static final String SEMAPHORE_LUA_SCRIPT = "" +
            "local permits = redis.call('get', KEYS[1]); " + "\n" +
            "if (permits ~= false and tonumber(permits) >= tonumber(ARGV[1])) then " + "\n" +
            "    redis.call('decrby', KEYS[1], ARGV[1]); " + "\n" +
            "    return 'true'; " + "\n" +
            "else " + "\n" +
            "    return 'false'; "+ "\n" +
            "end ";

    private final Jedis jedis;
    private final String name;
    private long waitingMilis = 150;

    /**
     * Creates a semaphore with one initial permit
     * @param jedis Jedis connection
     * @param name Name of the semaphore
     */
    public JedisSemaphore(Jedis jedis, String name) {
        this(jedis, name, 1);
    }

    /**
     * Creates a semaphore with one permit
     * @param jedis Jedis connection
     * @param name Name of the semaphore
     * @param initialPermits Initial permits of the semaphore
     */
    public JedisSemaphore(Jedis jedis, String name, int initialPermits) {
        this.jedis = jedis;
        this.name = name;
        init(initialPermits);
    }

    /**
     * Wait time between polling attemts
     * @param waitingMilis time in milis
     * @return this
     */
    public JedisSemaphore withWaitingMilis(long waitingMilis){
        this.waitingMilis = waitingMilis;
        return this;
    }

    /**
     * Init the semaphore if is the first
     * @param initialPermits initial permits
     */
    private void init(int initialPermits){
        if (initialPermits < 0) {
            initialPermits = 0;
        }
        jedis.set(name, String.valueOf(initialPermits), new SetParams().nx());
    }

    /**
     * Returns the sempahore name
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Acquires one permit, waiting (and polling) if not avalible
     * @throws InterruptedException can be interrupted
     */
    public void acquire() throws InterruptedException {
        acquire(1);
    }

    /**
     * Acquires N permits, waiting (and polling) if not avalible
     * @param permits permits to acquire
     * @throws InterruptedException can be interrupted
     */
    public void acquire(int permits) throws InterruptedException {
        boolean acquired = redisAcquire(permits);
        while(!acquired) {
            Thread.sleep(waitingMilis);
            acquired = redisAcquire(permits);
        }
    }

    /**
     * Tries to acquire one permit, but doesn't wait
     * @return true if permit acquired, false otherwise
     */
    public boolean tryAcquire() {
        return redisAcquire(1);
    }

    /**
     * Tries to acquire N permits, but doesn't wait
     * @param permits permits to acquire
     * @return true if permits acquired, false otherwise
     */
    public boolean tryAcquire(int permits) {
        return redisAcquire(permits);
    }

    /**
     * Tries to acquire N permits, waiting a limited time
     * It tries polling to acquire until timeout is reached,
     * if not acquire returns with false
     * @param permits permits to acquire
     * @param timeOut timeout to wait
     * @param timeUnit unit of timeout
     * @return true if permits acquired, false otherwise
     */
    public boolean tryAcquire(int permits, long timeOut, TimeUnit timeUnit) throws InterruptedException {
        long timeMax = System.currentTimeMillis() + timeUnit.toMillis(timeOut);
        boolean acquired = redisAcquire(permits);
        boolean exired = System.currentTimeMillis() > timeMax;
        while(!acquired && !exired) {
            Thread.sleep(waitingMilis);
            exired = System.currentTimeMillis() > timeMax;
            if (!exired) {
                acquired = redisAcquire(permits);
            }
        }
        return acquired;
    }

    /**
     * Internal method to acquire permits executing Lua script
     * @param permits permits to obain
     * @return true if permits obtained
     */
    private boolean redisAcquire(int permits){
        Object oresult = jedis.eval(SEMAPHORE_LUA_SCRIPT, Arrays.asList(name), Arrays.asList(String.valueOf(permits)));
        String result = (String) oresult;
        return Boolean.parseBoolean(result);
    }


    /**
     * Releases one permit
     */
    public void release() {
        release(1);
    }

    /**
     * Releases N permits
     * @param permits permits to release
     */
    public void release(int permits) {
        jedis.incrBy(name,permits);
    }

    /**
     * Return the current avalible permits on this semaphore
     * @return number of permits
     */
    public int availablePermits() {
        String permits = jedis.get(name);
        if (permits == null || permits.isEmpty()) {
            return -1;
        } else {
            return Integer.parseInt(permits);
        }
    }

    /**
     * CAUTION !!
     * THIS METHOD DELETES THE REMOTE VALUE DESTROYING THIS SEMAPHORE AND OHTERS
     * USE AT YOUR OWN RISK WHEN ALL POSSIBLE OPERATIONS ARE FINISHED
     */
    public void destroy(){
        jedis.del(name);
    }


}