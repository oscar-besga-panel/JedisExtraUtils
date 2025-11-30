package org.oba.jedis.extra.utils.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.oba.jedis.extra.utils.interruptinglocks.InterruptingJedisJedisLockBase;
import org.oba.jedis.extra.utils.lock.IJedisLock;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;

public class FunctionalInterruptedWritingFileTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalInterruptedWritingFileTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPooled jedisPooled;
    private String lockName;
    private final List<IJedisLock> lockList = new ArrayList<>();
    private final AtomicBoolean otherError = new AtomicBoolean(false);
    private int line = 0;


    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPooled = jtfTest.createJedisPooled();
        lockName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();

    }

    @After
    public void after() {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPooled != null) {
            jedisPooled.del(lockName);
            jedisPooled.close();
        }
    }


    //@Ignore
    @Test(timeout = 35000)
    public void testIfInterruptedFor5SecondsLock() throws InterruptedException, IOException {
        for (int i = 0; i < jtfTest.getFunctionalTestCycles(); i ++) {
            line = 0;
            File tempFile = folder.newFile(getClass().getName() + "." + System.currentTimeMillis() + ".txt");
            LOGGER.info("Temp file is " + tempFile.getAbsolutePath());
            otherError.set(false);
            LOGGER.info("_\n");
            LOGGER.info("FUNCTIONAL_TEST_CYCLES " + i);
            Thread t1 = new Thread(new WriteTest(250, tempFile));
            t1.setName("T1_i"+i);
            Thread t2 = new Thread(new WriteTest(150, tempFile));
            t2.setName("T2_i"+i);
            Thread t3 = new Thread(new WriteTest(300, tempFile));
            t3.setName("T3_i"+i);
            List<Thread> threadList = Arrays.asList(t1,t2,t3);
            Collections.shuffle(threadList);
            threadList.forEach(Thread::start);
            for(Thread tt: threadList) tt.join();
            //pressAnyKeyToContinue();
            assertFalse(otherError.get());
            assertFalse(lockList.stream().anyMatch(il -> il != null && il.isLocked()));
        }
    }

    private void pressAnyKeyToContinue() {
        try {
            System.out.println("Press Enter key to continue...");
            System.in.read();
        } catch(Exception e) {
            // NOOPE
        }
    }

    private class WriteTest implements Runnable {

        private final long milis;
        private final File tempFile;
        private IJedisLock jedisLock;

        WriteTest(long milis, File tempFile){
            this.milis = milis;
            this.tempFile = tempFile;
        }

        @Override
        public void run() {
            try {
                jedisLock = new InterruptingJedisJedisLockBase(jedisPooled, lockName, milis, TimeUnit.MILLISECONDS);
                lockList.add(jedisLock);
                jedisLock.lock();
                JedisTestFactoryLocks.checkLock(jedisLock);
                writeTest();
            } catch (InterruptedException ie) {
                LOGGER.info("Closed channel by interrupt exception InterruptedException", ie);
            } catch (java.nio.channels.ClosedByInterruptException cbie) {
                LOGGER.info("Closed channel by interrupt exception ClosedByInterruptException", cbie);
                Thread.interrupted();  // We clean the state
            } catch (Exception e){
                LOGGER.error("Error ", e);
                otherError.set(true);
            } finally {
                jedisLock.unlock();
                lockList.remove(jedisLock);
            }
        }

        private void writeTest() throws IOException, InterruptedException {
            LOGGER.info("Writing with thread " + Thread.currentThread().getName());
            while(true) {
                line++;
                String text = "#" + line + " " + Thread.currentThread().getName();
                Files.write(tempFile.toPath(), Collections.singletonList(text), StandardOpenOption.APPEND);
                if (Thread.interrupted()) {
                    throw new InterruptedException("writeTest interrupted");
                }
            }
        }
    }

}
