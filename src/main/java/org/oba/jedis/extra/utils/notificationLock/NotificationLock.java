package org.oba.jedis.extra.utils.notificationLock;

import org.oba.jedis.extra.utils.lock.IJedisLock;
import org.oba.jedis.extra.utils.utils.NamedMessageListener;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.UniversalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class NotificationLock implements IJedisLock, NamedMessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationLock.class);

    public static final String SCRIPT_NAME = "semaphore.lua";
    public static final String FILE_PATH = "./src/main/resources/semaphore.lua";

    public static final String CLIENT_RESPONSE_OK = "OK";
    private static volatile long lastTokenCurrentTimeMilis;

    private final JedisPool jedisPool;
    private final String name;
    private final String uniqueToken;
    private final ScriptEvalSha1 script;
    private final StreamMessageSystem streamMessageSystem;
    private final Semaphore semaphore;

    NotificationLock(JedisPool jedisPool, String name) {
        this.jedisPool = jedisPool;
        this.name = name;
        this.uniqueToken = generateUniqueToken(name);
        this.script = new ScriptEvalSha1(jedisPool, new UniversalReader().
                withResoruce(SCRIPT_NAME).
                withFile(FILE_PATH));
        this.streamMessageSystem = new StreamMessageSystem(this, jedisPool);
        this.semaphore = new Semaphore(0);
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() {
        unlock();
    }

    @Override
    public boolean isLocked() {
        return false;
    }

    @Override
    public void onMessage(String message) {
        if (name.equals(message) && semaphore.hasQueuedThreads()) {
            semaphore.release();
        }
    }

    @Override
    public Long getLeaseTime() {
        return 0L;
    }

    @Override
    public TimeUnit getTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    @Override
    public long getLeaseMoment() {
        return 0;
    }

    public synchronized boolean tryLock() {
        return redisLock();
    }

    @Override
    public boolean tryLockForAWhile(long time, TimeUnit unit) throws InterruptedException {
        long effectiveTime = unit.toMillis(time);
        long ts = System.currentTimeMillis();
        boolean acquired = true;
        boolean locked = redisLock();
        while (!locked && acquired && effectiveTime > 0) {
            acquired = semaphore.tryAcquire(time, unit);
            if (acquired) {
                locked = redisLock();
            }
            long timePassed = System.currentTimeMillis() - ts;
            effectiveTime = effectiveTime - timePassed;
            ts = System.currentTimeMillis();
        }
        return locked && acquired && effectiveTime > 0;
    }

    public synchronized void lock() {
        boolean locked = redisLock();
        while (!locked) {
            try {
                semaphore.acquire();
                locked = redisLock();
            } catch (InterruptedException ie) {
                LOGGER.debug("interrupted", ie);
            }
        }
    }


    public synchronized void lockInterruptibly() throws InterruptedException {
        boolean locked = redisLock();
        while (!locked) {
            semaphore.acquire();
            locked = redisLock();
        }
    }

    public synchronized void unlock() {
        redisUnlock();
    }


    public synchronized void underLock(Runnable task)  {
        try (NotificationLock nl = this) {
            nl.lock();
            task.run();
        }
    }

    public synchronized <T> T underLock(Supplier<T> task) {
        try (NotificationLock nl = this){
            nl.lock();
            return task.get();
        }
    }

    /**
     * If a leaseTime is set, it checks the leasetime and the timelimit
     * Then it checks if remote redis has te same value as the lock
     * If not, returns false
     * @return true if the lock is remotely held
     */
    private synchronized boolean redisCheckLock() {
        return withJedisPoolGet(this::redisCheckLockUnderPool);
    }

    /**
     * If a leaseTime is set, it checks the leasetime and the timelimit
     * Then it checks if remote redis has te same value as the lock
     * If not, returns false
     * @return true if the lock is remotely held
     */
    private synchronized boolean redisCheckLockUnderPool(Jedis jedis) {
        String currentValueRedis = jedis.get(name);
        boolean check = uniqueToken.equals(currentValueRedis);
        LOGGER.debug("checkLock >" + Thread.currentThread().getName() + "check value {} currentValueRedis {} check {}",
                uniqueToken, currentValueRedis, check);
        return check;
    }


    /**
     * Attempts to get the lock.
     * It will try one time and return
     * The leaseMoment and timeLimit are set if lock is obtained
     * @return true if lock obtained, false otherwise
     */
    private synchronized boolean redisLock() {
        return withJedisPoolGet(this::redisLockUnderPool);
    }

    private synchronized boolean redisLockUnderPool(Jedis jedis) {
        LOGGER.debug("redisLockUnderPool");
        SetParams setParams = new SetParams().nx();
        Transaction t = jedis.multi();
        Response<String> responseClientStatusCodeReply = t.set(name, uniqueToken,setParams);
        Response<String> responseCurrentValueRedis = t.get(name);
        t.exec();
        String clientStatusCodeReply = responseClientStatusCodeReply.get();
        String currentValueRedis = responseCurrentValueRedis.get();
        LOGGER.debug("redisLockUnderPool clientStatusCodeReply {} currentValueRedis {} uniqueToken {}",
                clientStatusCodeReply, currentValueRedis, uniqueToken);
        return CLIENT_RESPONSE_OK.equalsIgnoreCase(clientStatusCodeReply) && uniqueToken.equals(currentValueRedis);
    }

    /**
     * Attempts to unlock the lock
     */
    private synchronized void redisUnlock() {
        if (!redisCheckLock()) return;
        List<String> keys = Collections.singletonList(name);
        List<String> values = Collections.singletonList(uniqueToken);
        Object response = script.evalSha(keys, values);
        LOGGER.debug("redisUnlock response {}", response);
        int num = 0;
        if (response != null) {
            LOGGER.debug("response {}", response);
            num = Integer.parseInt(response.toString());
        }
        if (num > 0) {
            streamMessageSystem.sendMessage(this.getName());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NotificationLock that = (NotificationLock) o;
        return Objects.equals(name, that.name) && Objects.equals(uniqueToken, that.uniqueToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, uniqueToken);
    }

    /**
     * Creates an unique token for a lock
     * @param name
     * @return
     */
    public synchronized static String generateUniqueToken(String name){
        long currentTimeMillis = System.currentTimeMillis();
        while(currentTimeMillis == lastTokenCurrentTimeMilis){
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                //NOOP
            }
            currentTimeMillis = System.currentTimeMillis();
        }
        lastTokenCurrentTimeMilis = currentTimeMillis;
        return name + "_" + System.currentTimeMillis() + "_" + ThreadLocalRandom.current().nextInt(1_000_000);
    }


}
