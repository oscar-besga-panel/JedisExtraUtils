package org.obapanel.jedis.semaphore;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.semaphore.MockOfJedis.integrationTestEnabled;

public class JedisSemaphoreNumberPermitsTest {

    private MockOfJedis mockOfJedis;
    private String semaphoreName;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(integrationTestEnabled());
        if (!integrationTestEnabled()) return;
        semaphoreName = "semaphore:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        mockOfJedis = new MockOfJedis();
    }

    @After
    public void after() {
        if (mockOfJedis != null) mockOfJedis.clearData();
    }



    @Test
    public void testNumOfPermits(){
        JedisAdvancedSemaphore jedisAdvancedSemaphore = new JedisAdvancedSemaphore(mockOfJedis.getJedisPool(),semaphoreName,3);
        assertEquals(3, jedisAdvancedSemaphore.availablePermits());
        assertFalse( jedisAdvancedSemaphore.tryAcquire(5));
        assertEquals(3, jedisAdvancedSemaphore.availablePermits());
        assertTrue( jedisAdvancedSemaphore.tryAcquire(1));
        assertEquals(2, jedisAdvancedSemaphore.availablePermits());
        assertTrue( jedisAdvancedSemaphore.tryAcquire(2));
        assertEquals(0, jedisAdvancedSemaphore.availablePermits());
        assertFalse( jedisAdvancedSemaphore.tryAcquire(1));
        assertEquals(0, jedisAdvancedSemaphore.availablePermits());
        jedisAdvancedSemaphore.release(2);
        assertEquals(2, jedisAdvancedSemaphore.availablePermits());
        assertTrue( jedisAdvancedSemaphore.tryAcquire(1));
        assertEquals(1, jedisAdvancedSemaphore.availablePermits());
    }
}
