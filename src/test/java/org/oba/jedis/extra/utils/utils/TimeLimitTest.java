package org.oba.jedis.extra.utils.utils;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TimeLimitTest {

    @Test(expected = IllegalArgumentException.class)
    public void newError1Test() {
        TimeLimit timeLimit = new TimeLimit(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void newError2Test() {
        TimeLimit timeLimit = new TimeLimit(-1, TimeUnit.SECONDS);
    }

    @Test
    public void checkInLimitTest() {
        TimeLimit timeLimit = new TimeLimit(1, TimeUnit.SECONDS);
        boolean check1 = timeLimit.checkInLimit();
        doWait(550);
        boolean check2 = timeLimit.checkInLimit();
        doWait(550);
        boolean check3 = timeLimit.checkInLimit();
        assertTrue(check1);
        assertTrue(check2);
        assertFalse(check3);
    }

    @Test
    public void checkTimeRestTest() {
        TimeLimit timeLimit = new TimeLimit(1, TimeUnit.SECONDS);
        long t1 = timeLimit.checkTimeRest();
        doWait(550);
        long t2 = timeLimit.checkTimeRest();
        doWait(550);
        long t3 = timeLimit.checkTimeRest();
        assertTrue(t1 >= 998);
        assertTrue(t2 >= 398);
        assertEquals(0L, t3);
    }

    public static void doWait(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }

}
