package org.obapanel.jedis.common.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
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


    public static JedisTestFactory get() {
        return JedisTestFactoryHolder.instance;
    }

    private static class JedisTestFactoryHolder {
        static JedisTestFactory instance = new JedisTestFactory();
    }

    private int functionalTestCycles = 0;
    private String host = "127.0.0.1";
    private int port = 6379;
    private String pass = "";

    private boolean testConnectionOk = true;

    public JedisTestFactory() {
        load();
    }

    private void load() {
        Properties properties = readFromFile();
        if (properties == null) {
            properties = readFromClasspath();
        }
        if (properties!=null) {
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
        functionalTestCycles = Integer.parseInt(properties.getProperty(PREFIX + "functionalTestCycles"));
        host = properties.getProperty(PREFIX + "host");
        port = Integer.parseInt(properties.getProperty(PREFIX + "port"));
        pass = properties.getProperty(PREFIX + "pass");
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
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(24); // original 128
        jedisPoolConfig.setMaxIdle(24); // original 128
        jedisPoolConfig.setMinIdle(4); // original 16
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPoolConfig.setTestWhileIdle(true);
        jedisPoolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        jedisPoolConfig.setNumTestsPerEvictionRun(3);
        jedisPoolConfig.setBlockWhenExhausted(true);
        if (pass != null && !pass.trim().isEmpty()) {
            return new JedisPool(jedisPoolConfig, host, port, Protocol.DEFAULT_TIMEOUT, pass);
        } else {
            return new JedisPool(jedisPoolConfig, host, port);
        }
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
