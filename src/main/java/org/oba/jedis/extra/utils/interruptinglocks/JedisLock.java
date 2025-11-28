package org.oba.jedis.extra.utils.interruptinglocks;

import org.oba.jedis.extra.utils.lock.IJedisLock;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.TimeLimit;
import org.oba.jedis.extra.utils.utils.UniversalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.oba.jedis.extra.utils.lock.UniqueTokenValueGenerator.generateUniqueTokenValue;

/**
 * Class that will perform a lock based on Redis locks
 * It is also closeable to be used in try-with-resources
 *
 * I do not recommend reuse a locked-and-unlocked JedisLock
 * It is not thread-safe, I do not recommend using between threads
 * Creating a new one is very, very cheap, as data depends on Redis only
 *
 * https://redis.io/topics/distlock
 *
 */
public class JedisLock implements IJedisLock {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisLock.class);

    public static final String CLIENT_RESPONSE_OK = "OK";

    public static final String SCRIPT_NAME = "lockUnlock.lua";
    public static final String FILE_PATH = "./src/main/resources/lockUnlock.lua";


    private final Long leaseTime;
    private final TimeUnit timeUnit;
    private final String name;
    private final String uniqueToken;
    private final JedisPooled jedisPooled;
    private final ScriptEvalSha1 script;

    private long leaseMoment = -1L;
    private long timeLimit = -1L;

    private long waitCylce = 300L;


    /**
     * Creates a Redis lock with a name
     * This constructor makes the lock with no time limitations
     * @param jedisPooled Jedis is Java Redis connection and operations pool
     * @param name Unique name of the lock, shared with all distributed lock
     */
    public JedisLock(JedisPooled jedisPooled, String name){
        this(jedisPooled, name, null, null);
    }

    /**
     * Creates a Redis lock with a name
     * @param jedisPooled Jedis is Java Redis connection and operations pool
     * @param name Unique name of the lock, shared with all distributed lock
     * @param leaseTime Amount of time in unit that the lock should live
     * @param timeUnit Unit of leaseTime
     */
    public JedisLock(JedisPooled jedisPooled, String name, Long leaseTime, TimeUnit timeUnit) {
        if (jedisPooled == null) throw new IllegalArgumentException("JedisPool can not be null");
        if (name == null || name.trim().isEmpty()) throw new IllegalArgumentException("Name can not be null nor empty nor whitespace");
        this.jedisPooled = jedisPooled;
        this.name = name;
        if (leaseTime != null && leaseTime > 0) {
            this.leaseTime = leaseTime;
            this.timeUnit = timeUnit;
        } else {
            this.leaseTime = null;
            this.timeUnit = null;
        }
        this.uniqueToken = generateUniqueTokenValue(name);
        this.script = new ScriptEvalSha1(jedisPooled, new UniversalReader().
                withResoruce(SCRIPT_NAME).
                withFile(FILE_PATH));
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

    @Override
    public long getLeaseMoment() {
        return leaseMoment;
    }

    String getUniqueToken(){
        return uniqueToken;
    }

    @Override
    public boolean tryLock() {
        return redisLock();
    }

    @Override
    public boolean tryLockForAWhile(long time, TimeUnit unit) throws InterruptedException {
        final TimeLimit timeLimit = new TimeLimit(time,unit);
        boolean locked = redisLock();
        while (!locked && timeLimit.checkInLimit()) {
            Thread.sleep(waitCylce);
            locked = redisLock();
        }
        return locked;
    }

    @Override
    public void lock() {
        boolean locked = redisLock();
        while (!locked) {
            try {
                Thread.sleep(waitCylce);
                locked = redisLock();
            } catch (InterruptedException ie) {
                LOGGER.debug("interrupted", ie);
            }
        }
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {
        boolean locked = redisLock();
        while (!locked) {
            Thread.sleep(waitCylce);
            locked = redisLock();
        }
    }

    @Override
    public boolean isLocked(){
        return redisCheckLock();
    }

    @Override
    public void unlock() {
        redisUnlock();
    }

    @Override
    public void underLock(Runnable task)  {
        try (JedisLock jl = this) {
            jl.lock();
            task.run();
        }
    }

    @Override
    public <T> T underLock(Supplier<T> task) {
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
    private boolean redisLock() {
        SetParams setParams = new SetParams().nx();
        if (leaseTime != null) {
            setParams.px(timeUnit.toMillis(leaseTime));
        }
        AbstractTransaction t = jedisPooled.multi();
        Response<String> responseClientStatusCodeReply = t.set(name, uniqueToken,setParams);
        Response<String> responseCurrentValueRedis = t.get(name);
        t.exec();
        String clientStatusCodeReply = responseClientStatusCodeReply.get();
        String currentValueRedis = responseCurrentValueRedis.get();
        boolean locked = CLIENT_RESPONSE_OK.equalsIgnoreCase(clientStatusCodeReply) && uniqueToken.equals(currentValueRedis);
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
    private void redisUnlock() {
        if (!redisCheckLock()) return;
        List<String> keys = Collections.singletonList(name);
        List<String> values = Collections.singletonList(uniqueToken);
        Object response = script.evalSha(keys, values);
        int num = 0;
        if (response != null) {
            LOGGER.debug("response {}", response);
            num = Integer.parseInt(response.toString());
        }
        if (num > 0) {
            resetLockMoment();
        }
    }

    /**
     * If a leaseTime is set, it checks the leasetime and the timelimit
     * Then it checks if remote redis has te same value as the lock
     * If not, returns false
     * @return true if the lock is remotely held
     */
    private boolean redisCheckLock() {
        boolean check = false;
        LOGGER.info("checkLock >" + Thread.currentThread().getName() + "check time {}", timeLimit - System.currentTimeMillis());
        if ((leaseTime == null) || (timeLimit > System.currentTimeMillis())) {
            String currentValueRedis = jedisPooled.get(name);
            LOGGER.debug("checkLock >" + Thread.currentThread().getName() + "check value {} currentValueRedis {}", uniqueToken, currentValueRedis);
            check = uniqueToken.equals(currentValueRedis);
        }
        if (!check) {
            resetLockMoment();
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
    public LockFromRedis asConcurrentLock(){
        if (leaseTime != null) {
            throw new IllegalStateException("A JedisLock with leaseTime can not be a concurrent lock");
        }
        return new LockFromRedis(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JedisLock jedisLock = (JedisLock) o;
        return name.equals(jedisLock.name) &&
                uniqueToken.equals(jedisLock.uniqueToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, uniqueToken);
    }

}