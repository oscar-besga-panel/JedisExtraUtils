package org.oba.jedis.extra.utils.utils;

import org.oba.jedis.extra.utils.collections.JedisList;
import org.oba.jedis.extra.utils.cycle.CycleData;
import org.oba.jedis.extra.utils.interruptinglocks.JedisLock;
import org.oba.jedis.extra.utils.rateLimiter.BucketRateLimiter;
import org.oba.jedis.extra.utils.semaphore.JedisSemaphore;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

public class ScriptHolder implements JedisPoolUser {

    private final JedisPool jedisPool;
    private final Map<String, ScriptEvalSha1> scriptMap = new HashMap<>();


    public static ScriptHolder generateHolderForJedisExtraUtils(JedisPool jedisPool) {
        ScriptHolder scriptHolder = new ScriptHolder(jedisPool);
        scriptHolder.addScriptWithResourceAndFile(BucketRateLimiter.SCRIPT_NAME, BucketRateLimiter.FILE_PATH);
        scriptHolder.addScriptWithResourceAndFile(CycleData.SCRIPT_NAME, CycleData.FILE_PATH);
        scriptHolder.addScriptWithResourceAndFile(JedisList.SCRIPT_NAME_INDEX_OF, JedisList.FILE_PATH_INDEX_OF);
        scriptHolder.addScriptWithResourceAndFile(JedisList.SCRIPT_NAME_LAST_INDEX_OF, JedisList.FILE_PATH_LAST_INDEX_OF);
        scriptHolder.addScriptWithResourceAndFile(JedisLock.SCRIPT_NAME, JedisLock.FILE_PATH);
        scriptHolder.addScriptWithResourceAndFile(JedisSemaphore.SCRIPT_NAME, JedisSemaphore.FILE_PATH);
        return scriptHolder;
    }


    public ScriptHolder(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void addScript(String key, ScriptEvalSha1 script) {
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
        ScriptEvalSha1 script = new ScriptEvalSha1(jedisPool, reader, true);
        scriptMap.put(key, script);
    }

    public ScriptEvalSha1 getScript(String key) {
        return scriptMap.computeIfAbsent(key, k -> {
            throw new IllegalStateException("No script for key " + key);
        });
    }

}
