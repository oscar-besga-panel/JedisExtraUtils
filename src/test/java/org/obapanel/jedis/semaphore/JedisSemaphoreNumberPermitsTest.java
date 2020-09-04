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
        JedisSemaphore jedisSemaphore = new JedisSemaphore(mockOfJedis.getJedis(),semaphoreName,3);
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

    @Test(expected = IllegalArgumentException.class)
    public void testNumOfPermitsErrorAcquire(){
        JedisSemaphore jedisSemaphore = new JedisSemaphore(mockOfJedis.getJedis(),semaphoreName,0);
        jedisSemaphore.tryAcquire(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNumOfPermitsErrorRelease(){
        JedisSemaphore jedisSemaphore = new JedisSemaphore(mockOfJedis.getJedis(),semaphoreName,0);
        jedisSemaphore.release(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNumOfPermitsErrorOnCreation(){
        JedisSemaphore jedisSemaphore = new JedisSemaphore(mockOfJedis.getJedis(),semaphoreName,-1);
    }
}
