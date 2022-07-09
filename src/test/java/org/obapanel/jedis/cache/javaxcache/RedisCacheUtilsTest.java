package org.obapanel.jedis.cache.javaxcache;


import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertNotNull;

public class RedisCacheUtilsTest {

    @Test
    public void unwrapTest() {
        ArrayList<String> arrayList = new ArrayList<>();
        List list = RedisCacheUtils.unwrap( List.class, arrayList);
        assertNotNull(list);
    }

    @Test(expected = IllegalArgumentException.class)
    public void unwrapTestWithErrors() {
        ArrayList<String> arrayList = new ArrayList<>();
        RedisCacheUtils.unwrap( Set.class, arrayList);
    }


}
