package org.obapanel.jedis.utils;

import java.util.List;

/**
 * This class produces an unmodifiable list of the elements inside the object
 * @param <K> Type of the elements of the class and the list
 */
public interface Listable<K> {

    /**
     * This method returns ALL of the values of the class as a unmodificable list
     * The list is unmodificable
     * @return list with values
     */
    List<K> asList();


}
