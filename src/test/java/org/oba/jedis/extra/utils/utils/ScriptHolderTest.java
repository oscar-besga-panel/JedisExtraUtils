package org.oba.jedis.extra.utils.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        scriptHolder = new ScriptHolder(mockOfJedis.getJedisPool());
    }

    @After
    public void after() throws IOException {
        mockOfJedis.getJedisPool().close();
        mockOfJedis.clearData();
    }

    @Test
    public void addTest() {
        ScriptEvalSha1 scriptEvalSha1 = new ScriptEvalSha1(mockOfJedis.getJedisPool(),
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
