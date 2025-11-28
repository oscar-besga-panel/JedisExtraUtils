package org.oba.jedis.extra.utils.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.oba.jedis.extra.utils.utils.MockOfJedis.unitTestEnabled;

public class ScriptEvalSha1Test {

    private final String script = String.format("ECHO 'Hello %d'", ThreadLocalRandom.current().nextInt(9));

    private MockOfJedis mockOfJedis;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
    }

    @After
    public void after() throws IOException {
        mockOfJedis.getJedisPooled().close();
        mockOfJedis.clearData();
    }


    ScriptEvalSha1 createNewScript() {
        return new ScriptEvalSha1(mockOfJedis.getJedisPooled(), new UniversalReader().withValue(script));
    }


    @Test
    public void loadTest() {
        ScriptEvalSha1 scriptEvalSha1 = createNewScript();
        boolean result = scriptEvalSha1.load();
        assertTrue(result);
        assertNotNull(scriptEvalSha1.getSha1Digest());
    }

    @Test
    public void evalSha1Test() {
        ScriptEvalSha1 scriptEvalSha1 = createNewScript();
        scriptEvalSha1.load();
        Object result = scriptEvalSha1.evalSha(Collections.singletonList("K"), Collections.singletonList("V"));
        assertTrue(result.toString().contains(scriptEvalSha1.getSha1Digest()));
        assertTrue(result.toString().contains("V"));
        assertTrue(result.toString().contains("K"));
    }

}
