package org.oba.jedis.extra.utils.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.collections.JedisList;
import org.oba.jedis.extra.utils.cycle.CycleData;
import org.oba.jedis.extra.utils.interruptinglocks.JedisLock;
import org.oba.jedis.extra.utils.rateLimiter.BucketRateLimiter;
import org.oba.jedis.extra.utils.semaphore.JedisSemaphore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.oba.jedis.extra.utils.utils.MockOfJedis.unitTestEnabled;

public class ScriptHolderTest {

    public static final String SCRIPT_NAME = "scriptHolderTest.lua";
    public static final String FILE_PATH = "./src/test/resources/scriptHolderTest.lua";

    private MockOfJedis mockOfJedis;

    private ScriptHolder scriptHolder;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        scriptHolder = new ScriptHolder(mockOfJedis.getJedisPooled());
    }

    @After
    public void after() throws IOException {
        mockOfJedis.getJedisPooled().close();
        mockOfJedis.clearData();
    }

    @Test
    public void generateHolderForJedisExtraUtilsTest() {
        ScriptHolder holder = ScriptHolder.generateHolderForJedisExtraUtils(mockOfJedis.getJedisPooled());
        assertNotNull(holder.getScript(BucketRateLimiter.SCRIPT_NAME));
        assertNotNull(holder.getScript(CycleData.SCRIPT_NAME));
        assertNotNull(holder.getScript(JedisList.SCRIPT_NAME_INDEX_OF));
        assertNotNull(holder.getScript(JedisList.SCRIPT_NAME_LAST_INDEX_OF));
        assertNotNull(holder.getScript(JedisLock.SCRIPT_NAME));
        assertNotNull(holder.getScript(JedisSemaphore.SCRIPT_NAME));
    }

    @Test
    public void addTest() {
        ScriptEvalSha1 scriptEvalSha1 = new ScriptEvalSha1(mockOfJedis.getJedisPooled(),
                new UniversalReader().withResoruce(SCRIPT_NAME).withFile(FILE_PATH));
        scriptHolder.addScript(SCRIPT_NAME, scriptEvalSha1);
        assertNotNull(scriptHolder.getScript(SCRIPT_NAME));
        Object result = scriptHolder.getScript(SCRIPT_NAME).
                evalSha(Collections.singletonList("k"), Collections.singletonList("v"));
        List<String> lsResult = Arrays.asList(((String) result).split(","));
        assertTrue(lsResult.containsAll(Arrays.asList("k", "v")));
    }

    @Test
    public void addScriptWithResourceAndFile1Test() {
        scriptHolder.addScriptWithResourceAndFile(SCRIPT_NAME, FILE_PATH);
        assertNotNull(scriptHolder.getScript(SCRIPT_NAME));
        Object result = scriptHolder.getScript(SCRIPT_NAME).
                evalSha(Collections.singletonList("k"), Collections.singletonList("v"));
        List<String> lsResult = Arrays.asList(((String) result).split(","));
        assertTrue(lsResult.containsAll(Arrays.asList("k", "v")));
    }


    @Test
    public void addScriptWithResourceAndFile2Test() {
        scriptHolder.addScriptWithResourceAndFile(SCRIPT_NAME, SCRIPT_NAME, FILE_PATH);
        assertNotNull(scriptHolder.getScript(SCRIPT_NAME));
        Object result = scriptHolder.getScript(SCRIPT_NAME).
                evalSha(Collections.singletonList("k"), Collections.singletonList("v"));
        List<String> lsResult = Arrays.asList(((String) result).split(","));
        assertTrue(lsResult.containsAll(Arrays.asList("k", "v")));
    }

}
