package org.obapanel.jedis.utils;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class OnMessagePubSubTest {


    @Test
    public void onMessagePubSubTest() {
        final AtomicInteger num = new AtomicInteger(0);
        OnMessagePubSub onMessagePubSub = new OnMessagePubSub((c,m)->{
            num.set(Integer.parseInt(c) + Integer.parseInt(m));
        });
        onMessagePubSub.onMessage("2", "3");
        assertEquals(5, num.get());
    }

}
