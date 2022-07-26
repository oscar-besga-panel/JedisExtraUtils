package org.obapanel.jedis.interruptinglocks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.interruptinglocks.MockOfJedis.unitTestEnabled;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class, TransactionBase.class })
public class InterruptingLocksTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(InterruptingLocksTest.class);


    private String lockName;
    private final List<InterruptingJedisJedisLockBase> interruptingLockBaseList = new ArrayList<>();

    private MockOfJedis mockOfJedis;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        lockName = "lock:" + this.getClass().getName() + ":lockT" + System.currentTimeMillis();
    }

    @After
    public void after() {
        if (mockOfJedis!= null) {
            mockOfJedis.getJedisPool().close();
            mockOfJedis.clearData();
        }
        interruptingLockBaseList.stream().
                filter(Objects::nonNull).
                forEach(il -> {
                    if (il.isLocked()) {
                        LOGGER.error("A lock named {} is locked !", il.getName());
                    }
                    il.unlock();
                });
    }

    @Test
    public void testInterruptingLock() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        JedisPool jedisPool = mockOfJedis.getJedisPool();
        InterruptingJedisJedisLockBase jedisLock1 = new InterruptingJedisJedisLockBase(jedisPool, lockName, 5L , TimeUnit.SECONDS);
        assertEquals(lockName, jedisLock1.getName());
        assertEquals(Long.valueOf(5L) , Long.valueOf(jedisLock1.getLeaseTime()));
        assertEquals(TimeUnit.SECONDS , jedisLock1.getTimeUnit());
        assertEquals(Long.valueOf(-1L) , Long.valueOf(jedisLock1.getLeaseMoment()));
        long t = System.currentTimeMillis();
        boolean lock = jedisLock1.tryLock();
        if (lock) {
            assertTrue(t <= jedisLock1.getLeaseMoment());
        }
        jedisLock1.unlock();
    }


        @Test
    public void testTryLock() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        JedisPool jedisPool = mockOfJedis.getJedisPool();
        InterruptingJedisJedisLockBase jedisLock1 = new InterruptingJedisJedisLockBase(jedisPool, lockName, 5L , TimeUnit.SECONDS);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        InterruptingJedisJedisLockBase jedisLock2 = new InterruptingJedisJedisLockBase(jedisPool, lockName, 5L , TimeUnit.SECONDS);
        boolean result2 = jedisLock2.tryLock();
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        jedisLock1.unlock();
    }

    @Test
    public void testTryLockForAWhile() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        JedisPool jedisPool = mockOfJedis.getJedisPool();
        InterruptingJedisJedisLockBase jedisLock1 = new InterruptingJedisJedisLockBase(jedisPool,lockName, 5L , TimeUnit.SECONDS);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        InterruptingJedisJedisLockBase jedisLock2 = new InterruptingJedisJedisLockBase(jedisPool,lockName, 5L , TimeUnit.SECONDS);
        boolean result2 = jedisLock2.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        jedisLock1.unlock();
    }

}
