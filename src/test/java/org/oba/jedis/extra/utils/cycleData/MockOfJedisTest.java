package org.oba.jedis.extra.utils.cycleData;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MockOfJedisTest {

    private MockOfJedis mockOfJedis;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
    }

    @After
    public void tearDown() {
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
    }

    @Test
    public void testExists() {
        boolean exists1 = mockOfJedis.getJedisPooled().exists("test");
        Map<String,String> m = new HashMap<>();
        m.put("a","1");
        mockOfJedis.getJedisPooled().hset("test", m);
        boolean exists2 = mockOfJedis.getJedisPooled().exists("test");
        assertFalse(exists1);
        assertTrue(exists2);
    }

    @Test
    public void testDelete() {
        boolean exists1 = mockOfJedis.getJedisPooled().exists("test");
        Map<String,String> m = new HashMap<>();
        m.put("a","1");
        mockOfJedis.getJedisPooled().hset("test", m);
        boolean exists2 = mockOfJedis.getJedisPooled().exists("test");
        mockOfJedis.getJedisPooled().del("test");
        boolean exists3 = mockOfJedis.getJedisPooled().exists("test");
        assertFalse(exists1);
        assertTrue(exists2);
        assertFalse(exists3);
    }

    @Test
    public void testHset() {
        Map<String,String> m = new HashMap<>();
        m.put("a","1");
        mockOfJedis.getJedisPooled().hset("test", m);
        mockOfJedis.getJedisPooled().hset("test", "b" , "2");
        assertEquals(2, mockOfJedis.getData("test").size());
        assertEquals("1", mockOfJedis.getData("test").get("a"));
        assertEquals("2", mockOfJedis.getData("test").get("b"));
    }


    @Test
    public void testScriptLoad() {
        String sha = mockOfJedis.getJedisPooled().scriptLoad("script");
        assertEquals(ScriptEvalSha1.sha1("script"), sha);
    }

    @Test
    public void testEvalSha() {
        long l = ThreadLocalRandom.current().nextLong();
        mockOfJedis.setDoWithEvalSha( (t,u,v) -> {
            assertEquals("sha1", t);
            assertEquals(2, u.size());
            assertEquals("a", u.get(0));
            assertEquals("b", u.get(1));
            assertEquals(2, v.size());
            assertEquals("1", v.get(0));
            assertEquals("2" + l, v.get(1));
            return Boolean.TRUE;
        });
        Object result = mockOfJedis.getJedisPooled().evalsha("sha1", Arrays.asList("a","b"), Arrays.asList("1","2" + l));
        assertEquals(Boolean.TRUE, result);
    }





}
