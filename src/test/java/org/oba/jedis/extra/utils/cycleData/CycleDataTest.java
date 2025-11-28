package org.oba.jedis.extra.utils.cycleData;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.cycle.CycleData;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

public class CycleDataTest {

    private MockOfJedis mockOfJedis;
    private JedisPooled jedisPooled;
    private String cycleDataName;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        mockOfJedis.setDoWithEvalSha((r,k,p) -> cycleDataExecution(k,p));
        jedisPooled = mockOfJedis.getJedisPooled();
        cycleDataName = "cycleDataName:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }

    @After
    public void after() throws IOException {
        jedisPooled.close();
        mockOfJedis.clearData();
    }

    String cycleDataExecution(List<String> keys, List<String> values) {
        Map<String, String> cycle = mockOfJedis.getData(keys.get(0));
        int current = Integer.parseInt(cycle.get("current"));
        int size = cycle.size() - 1;
        String result = cycle.get(Integer.toString(current));
        current++;
        if (current >= size) {
            current = 0;
        }
        cycle.put("current",Integer.toString(current));
        return result;
    }

    @Test
    public void cycleDataBasicTest() throws InterruptedException {
        CycleData cycleData = new CycleData(jedisPooled, cycleDataName).
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
        CycleData cycleData = new CycleData(jedisPooled, cycleDataName).
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
        CycleData cycleData = new CycleData(jedisPooled, cycleDataName).
                createIfNotExists("A","B","C");
        int num = 0;
        // Tried with one million and it works
        while(cycleData.hasNext() && (num++ < 1_000)){
            assertNotNull(cycleData.next());
        }
        assertNotNull(cycleData.next());
    }

}
