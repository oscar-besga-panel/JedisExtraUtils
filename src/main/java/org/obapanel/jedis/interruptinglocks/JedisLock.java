package org.obapanel.jedis.interruptinglocks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class that will perform a lock based on Redis locks
 * It is also closeable to be used in try-with-resources
 *
 * https://redis.io/topics/distlock
 *
 */
public class JedisLock implements Closeable, AutoCloseable, IJedisLock {

    private static final Logger log = LoggerFactory.getLogger(JedisLock.class);

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
    private final Jedis jedis;

    private AtomicBoolean isLocked = new AtomicBoolean(false);
    private long leaseMoment = -1L;
    private long timeLimit = -1L;

    private long waitCylce = 300L;

    private static long lastCurrentTimeMilis = 0L;

    /**
     * Creates a Redis lock with a name
     * This constructor makes the lock with no time limitations
     * @param jedis Jedis is Java Redis connection and operartions
     * @param name Unique name of the lock, shared with all distributed lock
     */
    public JedisLock(Jedis jedis, String name){
        this(jedis, name, null, null);
    }


    /**
     * Creates a Redis lock with a name
     * @param jedis Jedis is Java Redis connection and operartions
     * @param name Unique name of the lock, shared with all distributed lock
     * @param leaseTime Amount of time in unit that the lock should live
     * @param timeUnit Unit of leaseTime
     */
    public JedisLock(Jedis jedis, String name, Long leaseTime, TimeUnit timeUnit){
        this.jedis = jedis;
        this.name = name;
        this.value = getUniqueValue(name);
        this.leaseTime = leaseTime;
        this.timeUnit = timeUnit;
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
     * Jedis object used to lock
     * @return jedis
     */
    public Jedis getJedis() {
        return jedis;
    }

    /**
     * Moment when the lock was captured in this object, -1 if no locked
     * @return leaseMoment
     */
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

    /**
     * Returns the current value in Redis
     * A string whem is locked, null if not
     * For testing
     * @return Strig of redis
     */
    private String getValue(){
        return value;
    }

    /**
     * Attempts to get the lock.
     * It will try one time and return
     * The leaseMoment and timeLimit are set if lock is obtained
     * @return true if lock obtained, false otherwise
     */
    private synchronized boolean redisLock() {
        checkTimeoutAndUnlock();
        SetParams setParams = new SetParams().nx();
        if (leaseTime != null) {
            setParams.px(timeUnit.toMillis(leaseTime));
        }
        String clientStatusCodeReply = jedis.set(name,value,setParams);
        String currentValueRedis = jedis.get(name);
        boolean itWorked = CLIENT_RESPONSE_OK.equalsIgnoreCase(clientStatusCodeReply) && value.equals(currentValueRedis);
        if (itWorked) {
            setLockState();
        }
        return isLocked.get();
    }

    private synchronized void setLockState() {
        isLocked.set(true);
        leaseMoment = System.currentTimeMillis();
        if (leaseTime != null){
            this.timeLimit = System.currentTimeMillis() +  timeUnit.toMillis(leaseTime);
        }
    }


    @Override
    public synchronized boolean tryLock() {
        return redisLock();
    }



    @Override
    public synchronized boolean tryLockForAWhile(long time, TimeUnit unit) throws InterruptedException {
        long tryLockTimeLimit = System.currentTimeMillis() + unit.toMillis(time);
        tryLock();
        while (!isLocked() && tryLockTimeLimit > System.currentTimeMillis()) {
            Thread.sleep(waitCylce);
            tryLock();
        }
        return isLocked();
    }


    @Override
    public synchronized void lock() {
        redisLock();
        while (!isLocked.get()){
            try {
                lockInterruptibly();
            }catch (InterruptedException ie){
                log.debug("interrupted",ie);
            }
        }
    }


    @Override
    public synchronized void lockInterruptibly() throws InterruptedException {
        redisLock();
        while (!isLocked.get()){
            Thread.sleep(waitCylce);
            redisLock();
        }
    }

    /**
     * Attempts to unlock the lock
     * @return true if unlocked
     */
    private synchronized boolean redisUnlock() {
        checkTimeoutAndUnlock();
        List<String> keys = Arrays.asList(name);
        List<String> values = Arrays.asList(value);
        Object response = jedis.eval(UNLOCK_LUA_SCRIPT, keys, values);
        int num = 0;
        if (response != null) {
            num = Integer.parseInt(response.toString());
        }
        if ( num > 0 ) {
            setUnlockState();
        }
        return !isLocked.get();
    }

    @Override
    public synchronized void unlock() {
        redisUnlock();
    }

    /**
     * Checks if locked, and if time limit has passed
     * If passed, the state of the object is unlocked
     * Doesn't class to redis, as we expect the locking value to be expired
     */
    private void checkTimeoutAndUnlock() {
        if (isLocked.get() && leaseTime != null && System.currentTimeMillis() > timeLimit) {
            setUnlockState();
        }
    }

    /**
     * Set the internal flags to unlock state
     */
    private void setUnlockState() {
        leaseMoment = -1L;
        timeLimit = -1L;
        isLocked.set(false);
    }


    @Override
    public boolean isLocked(){
        checkTimeoutAndUnlock();
        return isLocked.get();
    }


    @Override
    public void close() throws IOException {
        redisUnlock();
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

}
