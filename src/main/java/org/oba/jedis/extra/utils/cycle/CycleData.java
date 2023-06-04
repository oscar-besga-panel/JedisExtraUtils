package org.oba.jedis.extra.utils.cycle;

import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.oba.jedis.extra.utils.utils.Named;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.UniversalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class will have a list of elements and calls to next element in a cycle
 * Each call will recover the next element, and when the last element is recovered
 *   the first element will be next; in a loop cycle
 *
 * It implements iterator, but take in account
 * - hasNext is always true as it cycles
 * - removing elements is not possible (exception will be thrown)
 * - using forEachRemaining will lead to a infinite loop, as it cycles , it has no end
 */
public class CycleData implements JedisPoolUser, Named, Iterator<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CycleData.class);

    public static final String SCRIPT_NAME = "cycleData.lua";
    public static final String FILE_PATH = "./src/main/resources/cycleData.lua";

    public static final String CURRENT = "current";
    public static final String CURRENT_VALUE = "0"; // Use 0 based index

    private final JedisPool jedisPool;
    private final String name;
    private final ScriptEvalSha1 script;

    public CycleData(JedisPool jedisPool, String name) {
        this.jedisPool = jedisPool;
        this.name = name;
        this.script = new ScriptEvalSha1(jedisPool, new UniversalReader().
                withResoruce(SCRIPT_NAME).
                withFile(FILE_PATH));
    }

    public CycleData createIfNotExists(String... data) {
        if (!exists()) {
            return create(data);
        } else {
            return this;
        }
    }

    public CycleData create(String... data) {
        withJedisPoolDo(jedis -> createWithJedis(jedis, data));
        return this;
    }

    private void createWithJedis(Jedis jedis, String... data) {
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put(CURRENT, CURRENT_VALUE);
        for (int i = 0; i < data.length; i++) {
            dataMap.put(Integer.toString(i), data[i]);
        }
        jedis.hset(name, dataMap);
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public String getName() {
        return name;
    }

    public boolean exists() {
        return withJedisPoolGet( jedis -> jedis.exists(name));
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public String next() {
        if (exists()) {
            Object result = script.evalSha(Collections.singletonList(name), Collections.emptyList());
            LOGGER.debug("Current result is {}", result);
            if (result instanceof String) {
                return (String) result;
            } else {
                throw new IllegalStateException("getNext result not good " + result);
            }
        } else {
            return null;
        }
    }

    public void delete() {
        withJedisPoolDo( jedis ->
                jedis.del(name)
        );
    }

}
