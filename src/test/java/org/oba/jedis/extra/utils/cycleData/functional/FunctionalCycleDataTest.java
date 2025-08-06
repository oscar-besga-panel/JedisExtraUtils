package org.oba.jedis.extra.utils.cycleData.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.cycle.CycleData;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.test.WithJedisPoolDelete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

public class FunctionalCycleDataTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalCycleDataTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPool jedisPool;
    private String cycleDataName;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
        cycleDataName = "cycleDataName:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPool != null) {
            WithJedisPoolDelete.doDelete(jedisPool, cycleDataName);
            jedisPool.close();
        }
    }

    @Test
    public void cycleDataBasicTest() throws InterruptedException {
        CycleData cycleData = new CycleData(jedisPool, cycleDataName).
                create("A","B","C");
        List<String> results = new ArrayList<>();
        for(int i = 0; i < 10; i++) {
            String result = cycleData.next();
            results.add(result);
        }
        List<String> compare = Arrays.asList("A","B","C","A","B","C","A","B","C","A");
        for(int i = 0; i < 10; i++) {
            assertEquals(compare.get(i), results.get(i));
        }
    }

    @Test
    public void createBasicTest() throws InterruptedException {
        CycleData cycleData = new CycleData(jedisPool, cycleDataName).
                createIfNotExists("A","B","C");
        assertTrue(cycleData.exists());
        cycleData.delete();
        assertFalse(cycleData.exists());
        cycleData.createIfNotExists("C","B","A");
        assertTrue(cycleData.exists());
        cycleData.createIfNotExists("C","B","A");
        assertTrue(cycleData.exists());
    }

    @Test
    public void iteratorTest() {
        CycleData cycleData = new CycleData(jedisPool, cycleDataName).
                createIfNotExists("A","B","C");
        int num = 0;
        // Tried with one million and it works
        while(cycleData.hasNext() && (num++ < 1_000)){
            assertNotNull(cycleData.next());
        }
        assertNotNull(cycleData.next());
    }

}
