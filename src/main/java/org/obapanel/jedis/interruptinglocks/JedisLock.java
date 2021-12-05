package org.obapanel.jedis.interruptinglocks;

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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Class that will perform a lock based on Redis locks
 * It is also closeable to be used in try-with-resources
 *
 * I do not recommend reuse a locked-and-unlocked JedisLock
 * Should be thread-safe, I do not recomend using between thread s
 *
 * https://redis.io/topics/distlock
 *
 */
public class JedisLock implements IJedisLock {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisLock.class);

    public static final String CLIENT_RESPONSE_OK = "OK";

    public static final String UNLOCK_LUA_SCRIPT = "" +
            "if redis.call(\"get\",KEYS[1]) == ARGV[1] then\n" +
            "    return redis.call(\"del\",KEYS[1])\n" +
            "else\n" +
            "    return 0\n" +
            "end";

    private final Long leaseTime;
    private final TimeUnit timeUnit;
    private final String name;
    private final String value;
    private final JedisPool jedisPool;

    private long leaseMoment = -1L;
    private long timeLimit = -1L;

    private long waitCylce = 300L;

    private static long lastCurrentTimeMilis = 0L;

    /**
     * Creates a Redis lock with a name
     * This constructor makes the lock with no time limitations
     * @param jedisPool Jedis is Java Redis connection and operations pool
     * @param name Unique name of the lock, shared with all distributed lock
     */
    public JedisLock(JedisPool jedisPool, String name){
        this(jedisPool, name, null, null);
    }




    /**
     * Creates a Redis lock with a name
     * @param jedisPool Jedis is Java Redis connection and operations pool
     * @param name Unique name of the lock, shared with all distributed lock
     * @param leaseTime Amount of time in unit that the lock should live
     * @param timeUnit Unit of leaseTime
     */
    public JedisLock(JedisPool jedisPool, String name, Long leaseTime, TimeUnit timeUnit) {
        if (jedisPool == null) throw new IllegalArgumentException("JedisPool can not be null");
        if (name == null || name.trim().isEmpty()) throw new IllegalArgumentException("Name can not be null nor empty nor whitespace");
        this.jedisPool = jedisPool;
        this.name = name;
        this.leaseTime = leaseTime;
        this.timeUnit = timeUnit;
        this.value = getUniqueValue(name);
    }

    private synchronized static String getUniqueValue(String name){
        long currentTimeMillis = System.currentTimeMillis();
        while(currentTimeMillis == lastCurrentTimeMilis){
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                //NOOP
            }
            currentTimeMillis = System.currentTimeMillis();
        }
        lastCurrentTimeMilis = currentTimeMillis;
        return name + "_" + System.currentTimeMillis() + "_" + ThreadLocalRandom.current().nextInt(1_000_000);
    }

    public void setWaitCylce(int time, TimeUnit timeUnit){
        this.waitCylce = timeUnit.toMillis(time);
    }

    @Override
    public Long getLeaseTime() {
        return leaseTime;
    }

    @Override
    public TimeUnit getTimeUnit() {
        return timeUnit;
    }


    @Override
    public String getName() {
        return name;
    }

    /**
     * JedisPool object used to lock
     * @return jedisPool
     */
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    @Override
    public long getLeaseMoment() {
        return leaseMoment;
    }

    /**
     * System time until lock will be not valid, -1 if no locked
     * @return timeLimit
     */
    public long getTimeLimit() {
        return timeLimit;
    }

    // VisibleForTesting
    private String getValue() {
        return value;
    }

    @Override
    public synchronized boolean tryLock() {
        return redisLock();
    }



    @Override
    public synchronized boolean tryLockForAWhile(long time, TimeUnit unit) throws InterruptedException {
        long timeInMilis = unit.toMillis(time);
        long tryLockTimeLimit = System.currentTimeMillis() + timeInMilis;
        boolean locked = redisLock();
        while (!locked && tryLockTimeLimit > System.currentTimeMillis()) {
            doSleepBetweenAttempts(timeInMilis);
            locked = redisLock();
        }
        return locked;
    }


    @Override
    public synchronized void lock() {
        boolean locked = redisLock();
        while (!locked) {
            try {
                doSleepBetweenAttempts();
                locked = redisLock();
            } catch (InterruptedException ie) {
                LOGGER.debug("interrupted", ie);
            }
        }
    }


    @Override
    public synchronized void lockInterruptibly() throws InterruptedException {
        boolean locked = redisLock();
        while (!locked) {
            doSleepBetweenAttempts();
            locked = redisLock();
        }
    }

    protected void doSleepBetweenAttempts() throws InterruptedException {
        Thread.sleep(waitCylce);
    }

    protected void doSleepBetweenAttempts(long timeInMilis) throws InterruptedException {
        long waitNow = waitCylce;
        if (waitCylce > timeInMilis) {
            waitNow = timeInMilis - 1;
        }
        Thread.sleep(waitNow);
    }

    @Override
    public boolean isLocked(){
        return redisCheckLock();
    }

    @Override
    public synchronized void unlock() {
        redisUnlock();
    }

    public synchronized void underLock(Runnable task)  {
        try (JedisLock jl = this) {
            jl.lock();
            task.run();
        }
    }

    public synchronized <T> T underLock(Supplier<T> task) {
        try (JedisLock jl = this){
            jl.lock();
            return task.get();
        }
    }


    /**
     * Attempts to get the lock.
     * It will try one time and return
     * The leaseMoment and timeLimit are set if lock is obtained
     * @return true if lock obtained, false otherwise
     */
    private synchronized boolean redisLock() {
        try (Jedis jedis = jedisPool.getResource()) {
            return redisLockUnderPool(jedis);
        }
    }

    private synchronized boolean redisLockUnderPool(Jedis jedis) {
        SetParams setParams = new SetParams().nx();
        if (leaseTime != null) {
            setParams.px(timeUnit.toMillis(leaseTime));
        }
        Transaction t = jedis.multi();
        Response<String> responseClientStatusCodeReply = t.set(name,value,setParams);
        Response<String> responseCurrentValueRedis = t.get(name);
        t.exec();
        String clientStatusCodeReply = responseClientStatusCodeReply.get();
        String currentValueRedis = responseCurrentValueRedis.get();
        boolean locked = CLIENT_RESPONSE_OK.equalsIgnoreCase(clientStatusCodeReply) && value.equals(currentValueRedis);
        if (locked) {
            setLockMoment();
        }
        return  locked;
    }

    private void setLockMoment() {
        leaseMoment = System.currentTimeMillis();
        if (leaseTime != null){
            this.timeLimit = System.currentTimeMillis() +  timeUnit.toMillis(leaseTime);
        }
    }

    /**
     * Attempts to unlock the lock
     */
    private synchronized void redisUnlock() {
        try (Jedis jedis = jedisPool.getResource()){
            if (!redisCheckLock()) return;
            List<String> keys = Collections.singletonList(name);
            List<String> values = Collections.singletonList(value);
            Object response = jedis.eval(UNLOCK_LUA_SCRIPT, keys, values);
            int num = 0;
            if (response != null) {
                LOGGER.debug("response {}", response);
                num = Integer.parseInt(response.toString());
            }
            if (num > 0) {
                resetLockMoment();
            }
        }
    }

    /**
     * If a leaseTime is set, it checks the leasetime and the timelimit
     * Then it checks if remote redis has te same value as the lock
     * If not, returns false
     * @return true if the lock is remotely held
     */
    private boolean redisCheckLock() {
        try (Jedis jedis = jedisPool.getResource()) {
            return redisCheckLockUnderPool(jedis);
        }
    }

    /**
     * If a leaseTime is set, it checks the leasetime and the timelimit
     * Then it checks if remote redis has te same value as the lock
     * If not, returns false
     * @return true if the lock is remotely held
     */
    private boolean redisCheckLockUnderPool(Jedis jedis) {
        boolean check = false;
        LOGGER.info("checkLock >" + Thread.currentThread().getName() + "check time {}", timeLimit - System.currentTimeMillis());
        if ((leaseTime == null) || (timeLimit > System.currentTimeMillis())) {
            String currentValueRedis = jedis.get(name);
            LOGGER.debug("checkLock >" + Thread.currentThread().getName() + "check value {} currentValueRedis {}", value, currentValueRedis);
            check = value.equals(currentValueRedis);
        }
        return check;
    }

    /**
     * Resets the internal timers
     */
    private void resetLockMoment() {
        leaseMoment = -1L;
        timeLimit = -1L;
    }

    /**
     * Creates a java.util.concurrent.Lock instance of this lock
     * The new instance is binded to this object
     *
     * A JedisLock with leaseTinme can not be a concurrent lock, an exception will be thrown if you try
     *
     * @return Lock of JedisLock
     */
    public Lock asConcurrentLock(){
        if (leaseTime != null) throw new IllegalStateException("A JedisLock with leaseTime can not be a concurrent lock");
        return new Lock(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JedisLock jedisLock = (JedisLock) o;
        return name.equals(jedisLock.name) &&
                value.equals(jedisLock.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

}






