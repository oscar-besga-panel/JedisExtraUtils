package org.oba.jedis.extra.utils.notificationLock;

import org.oba.jedis.extra.utils.utils.Named;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.UniversalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

public class NotificationLock implements Named, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationLock.class);

    public static final String SCRIPT_NAME = "semaphore.lua";
    public static final String FILE_PATH = "./src/main/resources/semaphore.lua";

    public static final String CLIENT_RESPONSE_OK = "OK";

    private final NotificationLockFactory factory;
    private final String lockName;
    private final String uniqueToken;
    private final ScriptEvalSha1 script;
    private final Semaphore semaphore;

    NotificationLock(NotificationLockFactory factory, String lockName, String uniqueToken) {
        this.factory = factory;
        this.lockName = lockName;
        this.uniqueToken = uniqueToken;
        this.script = new ScriptEvalSha1(factory.getJedisPool(), new UniversalReader().
                withResoruce(SCRIPT_NAME).
                withFile(FILE_PATH));
        this.semaphore = new Semaphore(0);
    }

    @Override
    public String getName() {
        return lockName;
    }

    @Override
    public void close() {
        unlock();
    }

    public synchronized boolean tryLock() {
        return redisLock();
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


    void awake() {
        semaphore.release();
    }



    /**
     * If a leaseTime is set, it checks the leasetime and the timelimit
     * Then it checks if remote redis has te same value as the lock
     * If not, returns false
     * @return true if the lock is remotely held
     */
    private synchronized boolean redisCheckLock() {
        return factory.withJedisPoolGet(this::redisCheckLockUnderPool);
    }

    /**
     * If a leaseTime is set, it checks the leasetime and the timelimit
     * Then it checks if remote redis has te same value as the lock
     * If not, returns false
     * @return true if the lock is remotely held
     */
    private synchronized boolean redisCheckLockUnderPool(Jedis jedis) {
        boolean check = false;
        String currentValueRedis = jedis.get(lockName);
        LOGGER.debug("checkLock >" + Thread.currentThread().getName() + "check value {} currentValueRedis {}", uniqueToken, currentValueRedis);
        check = uniqueToken.equals(currentValueRedis);
        return check;
    }


    /**
     * Attempts to get the lock.
     * It will try one time and return
     * The leaseMoment and timeLimit are set if lock is obtained
     * @return true if lock obtained, false otherwise
     */
    private synchronized boolean redisLock() {
        return factory.withJedisPoolGet(this::redisLockUnderPool);
    }

    private synchronized boolean redisLockUnderPool(Jedis jedis) {
        LOGGER.debug("redisLockUnderPool");
        SetParams setParams = new SetParams().nx();
        Transaction t = jedis.multi();
        Response<String> responseClientStatusCodeReply = t.set(lockName, uniqueToken,setParams);
        Response<String> responseCurrentValueRedis = t.get(lockName);
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
        List<String> keys = Collections.singletonList(lockName);
        List<String> values = Collections.singletonList(uniqueToken);
        Object response = script.evalSha(keys, values);
        LOGGER.debug("redisUnlock response {}", response);
        int num = 0;
        if (response != null) {
            LOGGER.debug("response {}", response);
            num = Integer.parseInt(response.toString());
        }
        if (num > 0) {
            factory.messageOnUnlock(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NotificationLock that = (NotificationLock) o;
        return Objects.equals(factory, that.factory) && Objects.equals(lockName, that.lockName) && Objects.equals(uniqueToken, that.uniqueToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(factory, lockName, uniqueToken);
    }

}
