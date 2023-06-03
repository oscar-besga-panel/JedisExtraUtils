package org.oba.jedis.extra.utils.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Evals a script to its SHA-1 representation in redis
 * So it can be called without uploading all of the script code to redis every time
 *
 * It checks it against a local SHA-1 hash in Java
 * As seen in http://oliviertech.com/es/java/generate-SHA1-hash-from-a-String/
 *
 * Also, see how to debug im
 *    * https://redis.io/docs/manual/programmability/lua-debugging/
 *    * https://redis.com/blog/5-6-7-methods-for-tracing-and-debugging-redis-lua-scripts/
 *
 *
 */
public class ScriptEvalSha1 implements JedisPoolUser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScriptEvalSha1.class);
    public static final String SHA_1 = "SHA-1";

    private final JedisPool jedisPool;
    private final UniversalReader scriptSource;
    private String sha1Digest;

    public ScriptEvalSha1(JedisPool jedisPool, UniversalReader scriptSource) {
        this.jedisPool = jedisPool;
        this.scriptSource = scriptSource;
    }

    public String getSha1Digest() {
        return sha1Digest;
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public boolean load() {
        if (sha1Digest == null) {
            String scriptToLoad = scriptSource.read();
            if (scriptToLoad == null || scriptToLoad.isBlank()) {
                throw new IllegalArgumentException("Script to load cannot be null nor empty");
            }
            sha1Digest = withJedisPoolGet(jedis -> jedis.scriptLoad(scriptToLoad));
            LOGGER.debug("SHA1 load from script {}", sha1Digest);
            if (sha1Digest == null || sha1Digest.isBlank()) {
                LOGGER.error("SHA1 from reddit is null or empty !");
            } else if (!sha1Digest.equals(sha1(scriptToLoad))) {
                LOGGER.error("SHA1 from reddit and local doesn't match !");
            }
        }
        return sha1Digest != null;
    }

    public Object evalSha(List<String> keys, List<String> params) {
        if (sha1Digest == null) {
            load();
        }
        LOGGER.debug("SHA1 eval {}", sha1Digest);
        return withJedisPoolGet(jedis -> jedis.evalsha(sha1Digest, keys, params));
    }

    /**
     * As seen in http://oliviertech.com/es/java/generate-SHA1-hash-from-a-String/
     * @param script
     * @return
     */
    public static String sha1(String script) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(SHA_1);
            messageDigest.reset();
            messageDigest.update(script.getBytes(StandardCharsets.UTF_8));
            return String.format("%040x", new BigInteger(1, messageDigest.digest()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

}
