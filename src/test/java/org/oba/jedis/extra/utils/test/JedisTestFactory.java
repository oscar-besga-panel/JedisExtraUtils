package org.oba.jedis.extra.utils.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.SetParams;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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


    public JedisPooled createJedisPooled() {
        if (enableSentinel) {
            return createJedisPooledSentinel();
        } else {
            return createJedisPooledClassic();
        }
    }

    public JedisPooled createJedisPooledClassic() {
        JedisPoolConfig jedisPoolConfig = createJedisPoolConfig();
        if (pass != null && !pass.trim().isEmpty()) {
            return new JedisPooled(host, port, "", pass);
        } else {
            return new JedisPooled(host, port);
        }
    }


    public JedisPooled createJedisPooledSentinel() {
        throw new UnsupportedOperationException("no sentinel available for JedisPooled");
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
        try (JedisPooled jedisPooled = createJedisPooled()){
            testPoolConnection(jedisPooled);
        }
    }

    public void testPoolConnection(JedisPooled jedisPooled){
        String val = "test:" + System.currentTimeMillis();
        jedisPooled.set(val, val, new SetParams().px(5000));
        String check = jedisPooled.get(val);
        jedisPooled.del(val);
        if (!val.equalsIgnoreCase(check))
            throw new IllegalStateException("Jedis connection not ok");
        if (!jedisPooled.ping().equalsIgnoreCase("PONG"))
            throw new IllegalStateException("Jedis connection not pong");
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

    /**
     * Main method
     * Run only if avalible connnection on jedis.test.properties file
     * @param args arguments
     */
    public static void main(String[] args) {
        LOGGER.debug("main ini >>>> ");
        JedisTestFactory jedisTestFactory = JedisTestFactory.get();
        jedisTestFactory.testConnection();
        jedisTestFactory.testPoolConnection();
        LOGGER.debug("main fin <<<< ");
    }



}
