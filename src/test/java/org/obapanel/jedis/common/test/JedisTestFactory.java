package org.obapanel.jedis.common.test;

import org.obapanel.jedis.utils.JedisSentinelPoolAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.SetParams;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class JedisTestFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisTestFactory.class);

    private static final String ABC = "abcdefhijklmnopqrstuvwxyz";

    public static final String TEST_PROPERTIES_FILE = "jedis.test.properties";
    public static final String TEST_PROPERTIES_PATH = "./src/test/resources/" + TEST_PROPERTIES_FILE;

    private static final String PREFIX = "jedis.test.";

    private static final int DEFAULT_FUNCTIONAL_TEST_CYCLES = 0;
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 6379;
    private static final String DEFAULT_PASS = "";
    private static final boolean DEFAULT_ENABLE_SENTINEL = false;
    private static final String DEFAULT_SENTINEL_HOSTS = "";
    private static final String DEFAULT_SENTINEL_MASTER = "";
    private static final String DEFAULT_SEMTIMEL_PASS = "";

    public static JedisTestFactory get() {
        return JedisTestFactoryHolder.instance;
    }

    private static class JedisTestFactoryHolder {
        static JedisTestFactory instance = new JedisTestFactory();
    }

    private int functionalTestCycles = DEFAULT_FUNCTIONAL_TEST_CYCLES;
    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private String pass = DEFAULT_PASS;
    private boolean enableSentinel = DEFAULT_ENABLE_SENTINEL;
    private String sentinelHosts = DEFAULT_SENTINEL_HOSTS;
    private String sentinelMaster = DEFAULT_SENTINEL_MASTER;
    private String sentinelPass = DEFAULT_SEMTIMEL_PASS;


    private boolean testConnectionOk = true;

    public JedisTestFactory() {
        load();
    }

    private void load() {
        Properties properties = readFromFile();
        if (properties == null) {
            properties = readFromClasspath();
        }
        if (properties != null) {
            assignProperties(properties);
        }
        try {
            if (functionalTestEnabled()) {
                testConnection();
                testPoolConnection();
            }
        } catch (Exception e) {
            testConnectionOk = false;
            LOGGER.error("ERROR testing jedis connnections ",e);
        }
    }

    private Properties readFromFile() {
        return readFrom("file", () -> new FileInputStream(TEST_PROPERTIES_PATH));
    }

    private Properties readFromClasspath() {
        return readFrom("classpath", () -> JedisTestFactory.class.getClassLoader().getResourceAsStream(TEST_PROPERTIES_FILE));
    }

    private Properties readFrom(String fromWhat, IoSupplier inputStreamSupplier) {
        try (InputStream input = inputStreamSupplier.get()) {
            if (input == null) {
                throw new IllegalStateException("Input is null, not loaded from " + fromWhat);
            }
            Properties properties = new Properties();
            properties.load(input);
            LOGGER.debug("Ok loading from {} ", fromWhat);
            return properties;
        } catch (Exception e) {
            LOGGER.debug("Error loading from {} ", fromWhat, e);
            return null;
        }
    }

    private void assignProperties(Properties properties) {
        functionalTestCycles = Integer.parseInt(properties.getProperty(PREFIX + "functionalTestCycles",
                String.valueOf(DEFAULT_FUNCTIONAL_TEST_CYCLES)));
        host = properties.getProperty(PREFIX + "host", DEFAULT_HOST);
        port = Integer.parseInt(properties.getProperty(PREFIX + "port", String.valueOf(DEFAULT_PORT)));
        pass = properties.getProperty(PREFIX + "pass", DEFAULT_PASS);
        enableSentinel = "true".equalsIgnoreCase(properties.getProperty(PREFIX + "enableSentinel",
                String.valueOf(DEFAULT_ENABLE_SENTINEL)));
        sentinelHosts = properties.getProperty(PREFIX + "sentinel.hosts", DEFAULT_SENTINEL_HOSTS);
        sentinelMaster = properties.getProperty(PREFIX + "sentinel.master", DEFAULT_SENTINEL_MASTER);
        sentinelPass = properties.getProperty(PREFIX + "sentinel.pass", DEFAULT_SEMTIMEL_PASS);
    }


    public boolean functionalTestEnabled(){
        return testConnectionOk && functionalTestCycles > 0;
    }

    public int getFunctionalTestCycles() {
        return functionalTestCycles;
    }

    public Jedis createJedisClient(){
        HostAndPort hostAndPort = new HostAndPort(host, port);
        Jedis jedis = new Jedis(hostAndPort);
        if (pass != null && !pass.trim().isEmpty()) {
            jedis.auth(pass);
        }
        return jedis;
    }


    public JedisPool createJedisPool() {
        if (enableSentinel) {
            return createJedisPoolSentinel();
        } else {
            return createJedisPoolClassic();
        }
    }

    public JedisPool createJedisPoolClassic() {
        JedisPoolConfig jedisPoolConfig = createJedisPoolConfig();
        if (pass != null && !pass.trim().isEmpty()) {
            return new JedisPool(jedisPoolConfig, host, port, Protocol.DEFAULT_TIMEOUT, pass);
        } else {
            return new JedisPool(jedisPoolConfig, host, port);
        }
    }

    public JedisPool createJedisPoolSentinel() {
        JedisPoolConfig jedisPoolConfig = createJedisPoolConfig();
        Set<String> sentinels = new HashSet<>();
        Collections.addAll(sentinels, sentinelHosts.split(","));
        JedisSentinelPool jedisSentinelPool;
        if (sentinelPass != null || !sentinelPass.trim().isEmpty()) {
            jedisSentinelPool = new JedisSentinelPool(sentinelMaster, sentinels, jedisPoolConfig,sentinelPass);
        } else {
            jedisSentinelPool = new JedisSentinelPool(sentinelMaster, sentinels, jedisPoolConfig);
        }

        return JedisSentinelPoolAdapter.poolFromSentinel(jedisSentinelPool);
    }

    private JedisPoolConfig createJedisPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(24); // original 128
        jedisPoolConfig.setMaxIdle(24); // original 128
        jedisPoolConfig.setMinIdle(4); // original 16
        // High performance
//        jedisPoolConfig.setMaxTotal(128);
//        jedisPoolConfig.setMaxIdle(128);
//        jedisPoolConfig.setMinIdle(16);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPoolConfig.setTestWhileIdle(true);
        jedisPoolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(30).toMillis());
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(10).toMillis());
        jedisPoolConfig.setNumTestsPerEvictionRun(1);
        jedisPoolConfig.setBlockWhenExhausted(true);
        return jedisPoolConfig;
    }


    public void testConnection() {
        try (Jedis jedis = createJedisClient()) {
            testConnection(jedis);
        }
    }
    public void testConnection(Jedis jedis){
        String val = "test:" + System.currentTimeMillis();
        jedis.set(val,val,new SetParams().px(5000));
        String check = jedis.get(val);
        jedis.del(val);
        if (!val.equalsIgnoreCase(check))
            throw new IllegalStateException("Jedis connection not ok");
        if (!jedis.ping().equalsIgnoreCase("PONG"))
            throw new IllegalStateException("Jedis connection not pong");
    }

    public void testPoolConnection() {
        try (JedisPool jedisPool = createJedisPool()){
            testPoolConnection(jedisPool);
        }
    }

    public void testPoolConnection(JedisPool jedisPool){
        try (Jedis jedis = jedisPool.getResource() ) {
            String val = "test:" + System.currentTimeMillis();
            jedis.set(val, val, new SetParams().px(5000));
            String check = jedis.get(val);
            jedis.del(val);
            if (!val.equalsIgnoreCase(check))
                throw new IllegalStateException("Jedis connection not ok");
            if (!jedis.ping().equalsIgnoreCase("PONG"))
                throw new IllegalStateException("Jedis connection not pong");
        }
    }

    public List<String> randomSizedListOfChars() {
        int size = ThreadLocalRandom.current().nextInt(5, ABC.length());
        List<String> result = new ArrayList<>(size);
        for(int i=0; i < size ; i++) {
            result.add(String.valueOf(ABC.toCharArray()[i]));
        }
        return result;
    }



    interface IoSupplier {
        InputStream get() throws IOException;
    }

    public static void main(String[] args) {
        JedisTestFactory jedisTestFactory = JedisTestFactory.get();
        jedisTestFactory.testConnection();
        jedisTestFactory.testPoolConnection();

    }



}
