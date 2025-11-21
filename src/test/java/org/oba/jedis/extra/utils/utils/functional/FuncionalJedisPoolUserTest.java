package org.oba.jedis.extra.utils.utils.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class FuncionalJedisPoolUserTest implements JedisPoolUser {

    private static final Logger LOGGER = LoggerFactory.getLogger(FuncionalJedisPoolUserTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPool jedisPool;
    private Jedis jedis;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
        jedis = jedisPool.getResource();
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedis != null) {
            jedis.close();
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
    }


    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    @Test
    public void testJedisDo() {
        withResource(jedis -> {
            jedis.set("a","1");
            jedis.set("b","2");
            jedis.set("c","3");
        });
        assertTrue(jedis.exists("a"));
        assertTrue(jedis.exists("b"));
        assertTrue(jedis.exists("c"));
    }

    @Test
    public void testJedisGet() {
        boolean result = withResourceGet(jedis -> {
            jedis.set("a","1");
            jedis.set("b","2");
            jedis.set("c","3");
            return jedis.exists("a") && jedis.exists("b") && jedis.exists("c");
        });
        assertTrue(result);
        assertTrue(jedis.exists("a"));
        assertTrue(jedis.exists("b"));
        assertTrue(jedis.exists("c"));
    }

//
//    void withResource(Consumer<Jedis> consumer) {
//        try (Jedis jedis = getJedisPool().getResource()) {
//            consumer.accept(jedis);
//        }
//    }
//
//    <K> K withResource(Function<Jedis, K> function) {
//        try (Jedis jedis = getJedisPool().getResource()) {
//            return function.apply(jedis);
//        }
//    }
//
//    @Test
//    public void testJedisDo2() {
//        withResource(jedis -> {
//            jedis.set("a","1");
//            jedis.set("b","2");
//            jedis.set("c","3");
//        });
//        assertTrue(jedis.exists("a"));
//        assertTrue(jedis.exists("b"));
//        assertTrue(jedis.exists("c"));
//    }
//
//    @Test
//    public void testJedisGet2() {
//        boolean result = withResource(jedis -> {
//            jedis.set("a","1");
//            jedis.set("b","2");
//            jedis.set("c","3");
//            return jedis.exists("a") && jedis.exists("b") && jedis.exists("c");
//        });
//        assertTrue(result);
//        assertTrue(jedis.exists("a"));
//        assertTrue(jedis.exists("b"));
//        assertTrue(jedis.exists("c"));
//    }


}
