package org.oba.jedis.extra.utils.test;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.UnifiedJedis;

import java.io.IOException;

public class TestJedisTestFactory {

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
    }

    @Test
    public void testConnection() {
        if (jtfTest.functionalTestEnabled()) {
            jtfTest.testConnection();
        }
    }

}
