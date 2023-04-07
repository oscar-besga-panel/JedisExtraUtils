package org.oba.jedis.extra.utils.utils;

import java.util.Map;

/**
 * This class produces an unmodifiable map of the elements inside the object,
 * in a key, value fashion
 * @param <K> Type of the elements of the keys of the map
 * @param <V> Type of the elements of the values of the map and the class
 */
public interface Mapeable<K,V> {

    /**
     * This method returns ALL values of the class as an unmodifiable map
     * @return map with key-values
     */
    Map<K,V> asMap();

}
