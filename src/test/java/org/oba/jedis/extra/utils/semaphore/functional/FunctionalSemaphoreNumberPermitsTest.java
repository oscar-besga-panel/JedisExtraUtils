package org.oba.jedis.extra.utils.semaphore.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.semaphore.JedisSemaphore;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class FunctionalSemaphoreNumberPermitsTest {

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPool jedisPool;
    private String semaphoreName;


    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
        semaphoreName = "semaphore:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPool != null) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.del(semaphoreName);
            }
            jedisPool.close();
        }
    }



    @Test
    public void testNumOfPermits(){
        JedisSemaphore jedisSemaphore = new JedisSemaphore(jedisPool,   semaphoreName,3);
        assertEquals(3, jedisSemaphore.availablePermits());
        assertFalse( jedisSemaphore.tryAcquire(5));
        assertEquals(3, jedisSemaphore.availablePermits());
        assertTrue( jedisSemaphore.tryAcquire(1));
        assertEquals(2, jedisSemaphore.availablePermits());
        assertTrue( jedisSemaphore.tryAcquire(2));
        assertEquals(0, jedisSemaphore.availablePermits());
        assertFalse( jedisSemaphore.tryAcquire(1));
        assertEquals(0, jedisSemaphore.availablePermits());
        jedisSemaphore.release(2);
        assertEquals(2, jedisSemaphore.availablePermits());
        assertTrue( jedisSemaphore.tryAcquire(1));
        assertEquals(1, jedisSemaphore.availablePermits());
        assertTrue( jedisSemaphore.tryAcquire());
        assertEquals(0, jedisSemaphore.availablePermits());
    }

}
