package org.oba.jedis.extra.utils.utils;

import org.oba.jedis.extra.utils.collections.JedisList;
import org.oba.jedis.extra.utils.cycle.CycleData;
import org.oba.jedis.extra.utils.interruptinglocks.JedisLock;
import org.oba.jedis.extra.utils.rateLimiter.BucketRateLimiter;
import org.oba.jedis.extra.utils.semaphore.JedisSemaphore;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPooled;

import java.util.HashMap;
import java.util.Map;

public class ScriptHolder2 {

    private final JedisPooled jedisPooled;
    private final Map<String, ScriptEvalSha12> scriptMap = new HashMap<>();


    public static ScriptHolder2 generateHolderForJedisExtraUtils(JedisPooled jedisPooled) {
        ScriptHolder2 scriptHolder = new ScriptHolder2(jedisPooled);
        scriptHolder.addScriptWithResourceAndFile(BucketRateLimiter.SCRIPT_NAME, BucketRateLimiter.FILE_PATH);
        scriptHolder.addScriptWithResourceAndFile(CycleData.SCRIPT_NAME, CycleData.FILE_PATH);
        scriptHolder.addScriptWithResourceAndFile(JedisList.SCRIPT_NAME_INDEX_OF, JedisList.FILE_PATH_INDEX_OF);
        scriptHolder.addScriptWithResourceAndFile(JedisList.SCRIPT_NAME_LAST_INDEX_OF, JedisList.FILE_PATH_LAST_INDEX_OF);
        scriptHolder.addScriptWithResourceAndFile(JedisLock.SCRIPT_NAME, JedisLock.FILE_PATH);
        scriptHolder.addScriptWithResourceAndFile(JedisSemaphore.SCRIPT_NAME, JedisSemaphore.FILE_PATH);
        return scriptHolder;
    }


    public ScriptHolder2(JedisPooled jedisPooled) {
        this.jedisPooled = jedisPooled;
    }

    public void addScript(String key, ScriptEvalSha12 script) {
        script.load();
        scriptMap.put(key, script);
    }

    public void addScriptWithResourceAndFile(String resource, String file) {
        this.addScriptWithResourceAndFile(resource, resource, file);
    }

    public void addScriptWithResourceAndFile(String key, String resource, String file) {
        UniversalReader reader = new UniversalReader().
                withResoruce(resource).
                withFile(file);
        ScriptEvalSha12 script = new ScriptEvalSha12(jedisPooled, reader, true);
        scriptMap.put(key, script);
    }

    public ScriptEvalSha12 getScript(String key) {
        return scriptMap.computeIfAbsent(key, k -> {
            throw new IllegalStateException("No script for key " + key);
        });
    }

    public JedisPooled getJedisPooled() {
        return jedisPooled;
    }

}
