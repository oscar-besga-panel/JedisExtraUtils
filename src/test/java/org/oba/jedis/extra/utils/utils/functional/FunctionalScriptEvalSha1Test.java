package org.oba.jedis.extra.utils.utils.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.UniversalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class FunctionalScriptEvalSha1Test {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalScriptEvalSha1Test.class);

    private final List<String> EMPTY_LIST = Collections.emptyList();

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPooled jedisPooled;

    private UniversalReader luaScript;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPooled = jtfTest.createJedisPooled();
        luaScript = new UniversalReader().
                withResoruce("functionalScriptEvalSha1Test.lua").
                withFile("./src/test/resources/functionalScriptEvalSha1Test.lua").
                withValue("return redis.call('time')");
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPooled != null) {
            jedisPooled.close();
        }
    }


    @Test
    public void integrationTest() throws InterruptedException {
        ScriptEvalSha1 script = new ScriptEvalSha1(jedisPooled, luaScript);
        boolean loaded = script.load();
        Object result1 = script.evalSha(EMPTY_LIST, EMPTY_LIST);
        Thread.sleep(100);
        Object result2 = script.evalSha(EMPTY_LIST, EMPTY_LIST);
        assertTrue(loaded);
        assertNotNull(result1);
        assertNotNull(result2);
        assertEquals(ScriptEvalSha1.sha1(luaScript.read()), script.getSha1Digest());
        LOGGER.debug("script");
        LOGGER.debug(luaScript.read());
        LOGGER.debug("Loaded {} result1 {} result2 {} sha1 {}", loaded, result1, result2, script.getSha1Digest());
    }




}
