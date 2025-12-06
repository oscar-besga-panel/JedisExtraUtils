package org.oba.jedis.extra.utils.utils.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.collections.JedisList;
import org.oba.jedis.extra.utils.cycle.CycleData;
import org.oba.jedis.extra.utils.interruptinglocks.JedisLock;
import org.oba.jedis.extra.utils.rateLimiter.BucketRateLimiter;
import org.oba.jedis.extra.utils.semaphore.JedisSemaphore;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.ScriptHolder;
import org.oba.jedis.extra.utils.utils.UniversalReader;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

public class FunctionalScriptHolderTest {

    public static final String SCRIPT_NAME = "scriptHolderTest.lua";
    public static final String FILE_PATH = "./src/test/resources/scriptHolderTest.lua";

    private final JedisTestFactory jtfTest = JedisTestFactory.get();


    private JedisPooled jedisPooled;

    private ScriptHolder scriptHolder;


    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPooled = jtfTest.createJedisPooled();
        scriptHolder = new ScriptHolder(jedisPooled);
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPooled != null) {
            jedisPooled.close();
        }
    }

    @Test
    public void generateHolderForJedisExtraUtilsTest() {
        ScriptHolder holder = ScriptHolder.generateHolderForJedisExtraUtils(jedisPooled);
        assertNotNull(holder.getScript(BucketRateLimiter.SCRIPT_NAME));
        assertNotNull(holder.getScript(CycleData.SCRIPT_NAME));
        assertNotNull(holder.getScript(JedisList.SCRIPT_NAME_INDEX_OF));
        assertNotNull(holder.getScript(JedisList.SCRIPT_NAME_LAST_INDEX_OF));
        assertNotNull(holder.getScript(JedisLock.SCRIPT_NAME));
        assertNotNull(holder.getScript(JedisSemaphore.SCRIPT_NAME));
    }

    @Test
    public void addTest() {
        ScriptEvalSha1 scriptEvalSha1 = new ScriptEvalSha1(jedisPooled,
                new UniversalReader().withResoruce(SCRIPT_NAME).withFile(FILE_PATH));
        scriptHolder.addScript(SCRIPT_NAME, scriptEvalSha1);
        assertNotNull(scriptHolder.getScript(SCRIPT_NAME));
        Object result = scriptHolder.getScript(SCRIPT_NAME).
                evalSha(Collections.singletonList("k"), Collections.singletonList("v"));
        List<String> lsResult = (List<String>) result;
        assertTrue( isNumeric(lsResult.get(0)));
        assertTrue( isNumeric(lsResult.get(1)));
    }

    @Test
    public void addScriptWithResourceAndFile1Test() {
        scriptHolder.addScriptWithResourceAndFile(SCRIPT_NAME, FILE_PATH);
        assertNotNull(scriptHolder.getScript(SCRIPT_NAME));
        Object result = scriptHolder.getScript(SCRIPT_NAME).
                evalSha(Collections.singletonList("k"), Collections.singletonList("v"));
        List<String> lsResult = (List<String>) result;
        assertTrue( isNumeric(lsResult.get(0)));
        assertTrue( isNumeric(lsResult.get(1)));
    }


    @Test
    public void addScriptWithResourceAndFile2Test() {
        scriptHolder.addScriptWithResourceAndFile(SCRIPT_NAME, SCRIPT_NAME, FILE_PATH);
        assertNotNull(scriptHolder.getScript(SCRIPT_NAME));
        Object result = scriptHolder.getScript(SCRIPT_NAME).
                evalSha(Collections.singletonList("k"), Collections.singletonList("v"));
        List<String> lsResult = (List<String>) result;
        assertTrue( isNumeric(lsResult.get(0)));
        assertTrue( isNumeric(lsResult.get(1)));
    }


    // Thanks https://www.baeldung.com/java-check-string-number
    private static final Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");

    private static boolean isNumeric(String strNum) {
        return strNum != null && pattern.matcher(strNum).matches();
    }

}
