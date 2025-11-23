package org.oba.jedis.extra.utils.semaphore;


import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.oba.jedis.extra.utils.utils.Named;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha12;
import org.oba.jedis.extra.utils.utils.ScriptHolder;
import org.oba.jedis.extra.utils.utils.ScriptHolder2;
import org.oba.jedis.extra.utils.utils.UniversalReader;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

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
 * Thanks to redisson semaphore for guidance for lua script
 * https://github.com/redisson/redisson/blob/master/redisson/src/main/java/org/redisson/RedissonSemaphore.java
 * https://www.javadoc.io/static/org.redisson/redisson/3.11.6/index.html?org/redisson/RedissonReadWriteLock.html
 *
 *
 */
public class JedisSemaphore2 implements Named {

    public static final String SCRIPT_NAME = "semaphore.lua";
    public static final String FILE_PATH = "./src/main/resources/semaphore.lua";

    private final JedisPooled jedisPooled;
    private final String name;
    private final ScriptEvalSha12 script;
    private long waitingMilis = 150;

    /**
     * Creates a semaphore with one initial permit
     * @param jedisPooled Jedis connection pool
     * @param name Name of the semaphore
     */
    public JedisSemaphore2(JedisPooled jedisPooled, String name) {
        this(jedisPooled, name, 1);
    }

    /**
     * Creates a semaphore with one permit
     * @param jedisPooled Jedis connection pool
     * @param name Name of the semaphore
     * @param initialPermits Initial permits of the semaphore
     */
    public JedisSemaphore2(JedisPooled jedisPooled, String name, int initialPermits) {
        this.jedisPooled = jedisPooled;
        this.name = name;
        this.script = new ScriptEvalSha12(jedisPooled, new UniversalReader().
                withResoruce(SCRIPT_NAME).
                withFile(FILE_PATH));
        init(initialPermits);
    }

    /**
     * Creates a semaphore with one permit
     * @param scriptHolder holder with the script 'semaphore.lua'.
     *                     The pool of the holder will be used with the semaphore
     * @param name Name of the semaphore
     */
    public JedisSemaphore2(ScriptHolder2 scriptHolder, String name) {
        this(scriptHolder, name, 1);
    }

    /**
     * Creates a semaphore with permits
     * @param scriptHolder holder with the script 'semaphore.lua'.
     *                     The pool of the holder will be used with the semaphore
     * @param name Name of the semaphore
     * @param initialPermits Initial permits of the semaphore
     */
    public JedisSemaphore2(ScriptHolder2 scriptHolder, String name, int initialPermits) {
        this.jedisPooled = scriptHolder.getJedisPooled();
        this.name = name;
        this.script = scriptHolder.getScript(SCRIPT_NAME);
        init(initialPermits);
    }

    /**
     * Wait time between polling attemts
     * @param waitingMilis time in milis
     * @return this
     */
    public JedisSemaphore2 withWaitingMilis(long waitingMilis){
        this.waitingMilis = waitingMilis;
        return this;
    }

    /**
     * Init the semaphore if is the first
     * @param initialPermits initial permits
     */
    private void init(int initialPermits){
        if (initialPermits < 0) {
            throw new IllegalArgumentException("initial permit on semaphore must be always equal or more than zero");
        }
        jedisPooled.set(name, String.valueOf(initialPermits), new SetParams().nx());
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
    private boolean redisAcquire(int permits) {
        if (permits <= 0){
            throw new IllegalArgumentException("permits to acquire on semaphore must be always more than zero");
        }
        Object oresult = script.evalSha(Collections.singletonList(name),
                Collections.singletonList(String.valueOf(permits)));
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
        if (permits <= 0) {
            throw new IllegalArgumentException("permit to release on semaphore must be always more than zero");
        }
       jedisPooled.incrBy(name, permits);
    }

    /**
     * Return the current avalible permits on this semaphore
     * If value doesn't exists, it returns -1
     * @return number of permits
     */
    public int availablePermits() {
        String permits = jedisPooled.get(name);
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
    public void destroy() {
        jedisPooled.del(name);
    }

}