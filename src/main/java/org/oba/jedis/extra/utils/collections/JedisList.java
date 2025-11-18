package org.oba.jedis.extra.utils.collections;

import org.oba.jedis.extra.utils.lock.UniqueTokenValueGenerator;
import org.oba.jedis.extra.utils.utils.Named;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.UniversalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.args.ListPosition;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A Jedis-based implementation of a List interface backed on Redis list on server
 * The goal is to comply with the List interface specification by using Redis/Jedis directly and
 * getting data for server in every operation with no local data in the class (or mininum as possible).
 *
 * Of course, it is noted that a Redis list can change with the JedisList instance or by other instances
 * or by other commands/clients of Redis. This is assumed, and a caveat on using this class.
 * In case of iterators, it can cause malfunctions.
 * But the security of non interference of the Redis list data can not be given by this class; it must be
 * enforced by the environment or by a distributable lock or not assumed.
 *
 * Every process using the same name against the same Jedis server will share the underlying data in the Redis
 * server, and access to the same elements in the Redis list
 *
 * There are a few Redis-exclusive operations that will help to operate with Redis backed lists,
 * and you can retrieve the data to a pure Java list anytime.
 * But helpers on Redis lists is not the goal of this class, rather than have a list object backed by Redis
 *
 * Scripts from stackoverflow for indexof
 * https://stackoverflow.com/questions/8899111/get-the-index-of-an-item-by-value-in-a-redis-list
 */
public final class JedisList implements List<String>, Named {


    private static final Logger LOGGER = LoggerFactory.getLogger(JedisList.class);

    public static final String SCRIPT_NAME_INDEX_OF = "list.indexOf.lua";
    public static final String FILE_PATH_INDEX_OF = "./src/main/resources/list.indexOf.lua";

    public static final String SCRIPT_NAME_LAST_INDEX_OF = "list.lastIndexOf.lua";
    public static final String FILE_PATH_LAST_INDEX_OF = "./src/main/resources/list.lastIndexOf.lua";

    private static final String TO_DELETE = "TO_DELETE";

    private final JedisPool jedisPool;
    private final String name;
    private final ScriptEvalSha1 scriptIndexOf;
    private final ScriptEvalSha1 scriptLastIndexOf;

    /**
     * Creates a new list in jedis with given name, or references an existing one
     * This constructor doesn't make any change on Redis server
     * So really, creating here a list does not generate new data on Redis; the list on the server will
     *   exists when data is inserted
     * @param jedisPool Jedis pool connection
     * @param name Name of list on server
     */
    public JedisList(JedisPool jedisPool, String name){
        this.jedisPool = jedisPool;
        this.name = name;
        this.scriptIndexOf = new ScriptEvalSha1(jedisPool, new UniversalReader().
                withResoruce(SCRIPT_NAME_INDEX_OF).
                withFile(FILE_PATH_INDEX_OF));
        this.scriptLastIndexOf = new ScriptEvalSha1(jedisPool, new UniversalReader().
                withResoruce(SCRIPT_NAME_LAST_INDEX_OF).
                withFile(FILE_PATH_LAST_INDEX_OF));

    }

    /**
     * Creates a new list in jedis with given name, or references an existing one
     * If the list doesn't exists, the 'from' data is stored,
     * if the list already exists, the 'from' data is added to the list
     * @param jedisPool Jedis pool connection
     * @param name Name of list on server
     * @param from Data to add to the list
     */
    public JedisList(JedisPool jedisPool, String name, Collection<String> from){
        this(jedisPool, name);
        this.addAll(from);
    }

    /**
     * Name of the redis list
     * @return redis name
     */
    public String getName() {
        return name;
    }

    /**
     * If list exist in Redis namespace
     * @return true if there is a reference in redis namespace, false otherwise
     */
    public boolean exists() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(name);
        }
    }

    /**
     * Checks if list exist in Redis namespace
     * @throws IllegalStateException if there is not a reference in redis namespace
     */
    public void checkExists() {
        if (!exists()) {
            throw new IllegalStateException("Current list  " + name + " not found in redis server");
        }
    }

    /**
     * Returns a list in java memory with the data of the list on redis
     * It copies the redis data in java process
     * (currect implementation is an ArrayList, this may change)
     * @return list of data
     */
    public List<String> asList(){
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lrange(name, 0, -1);
        }
    }

    /**
     * Like subList, but it creates a sublist in Redis namespace
     * @param newListName New name of redis list
     * @param fromIndex index from to copy data
     * @param toIndex index to copy data
     * @return a new Jedis list referencing the sublist in redis server
     */
    public JedisList jedisSubList(String newListName, int fromIndex, int toIndex) {
        List<String> subList = subList(fromIndex, toIndex);
        return new JedisList(jedisPool, newListName, subList);
    }

    /**
     * Checks an index
     * @param index num to check
     * @throws IndexOutOfBoundsException if index is less than zero or more_or_equal to list size
     */
    public void checkIndex(int index) {
        int size = size();
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Current index out of bounds, value: " + index + " and list size " + size + " (size 0 is Jedis non existen list)");
        }
    }

    @Override
    public int size() {
        try (Jedis jedis = jedisPool.getResource()) {
            long len = jedis.llen(name);
            return Long.valueOf(len).intValue();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return indexOf(o) != -1;
    }

    @Override
    public Iterator<String> iterator() {
        return new JedisListIterator();
    }

    @Override
    public Object[] toArray() {
        return asList().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return asList().toArray(a);
    }

    @Override
    public boolean add(String s) {
        try (Jedis jedis = jedisPool.getResource()) {
            long result = jedis.rpush(name, s);
            return result > 0;
        }
    }

    @Override
    public boolean remove(Object o) {
        try (Jedis jedis = jedisPool.getResource()) {
            long result = jedis.lrem(name, 1L, (String) o);
            return result > 0;
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        boolean contains = true;
        for(Object o: c) {
            contains = contains && contains(o);
        }
        return contains;
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        try (Jedis jedis = jedisPool.getResource()) {
            String[] toAdd = c.toArray(new String[0]);
            long result = jedis.rpush(name, toAdd);
            return result > 0;
        }
    }

    @Override
    public boolean addAll(int index, Collection<? extends String> c) {
        int pos = 0;
        for(String s: c){
            add(index + pos, s);
            pos++;
        }
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean changed = false;
        for(Object o: c) {
            boolean lastValueChanged = remove(o);
            changed = changed || lastValueChanged;
        }
        return changed;
    }

    //TODO check mock
    @Override
    public boolean retainAll(Collection<?> c) {
        boolean changed = false;
        ListIterator<String> iterator = this.listIterator();
        while(iterator.hasNext()) {
            String data = iterator.next();
            if (!c.contains(data)){
                iterator.remove();
                changed = true;
            }
        }
        return changed;
    }

    @Override
    public void clear() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(name);
        }
    }

    @Override
    public String get(int index) {
        try (Jedis jedis = jedisPool.getResource()) {
            checkIndex(index);
            return jedis.lindex(name, index);
        }
    }

    @Override
    public String set(int index, String element) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction tjedis = jedis.multi();
            Response<String> futureReplaced = tjedis.lindex(name, index);
            tjedis.lset(name, index, element);
            tjedis.exec();
            return futureReplaced.get();
        }
    }

    @Override
    public void add(int index, String element) {
        try (Jedis jedis = jedisPool.getResource()) {
            String pivot = get(index);
            jedis.linsert(name, ListPosition.BEFORE, pivot, element);
        }
    }

    @Override
    public String remove(int index) {
        try (Jedis jedis = jedisPool.getResource()) {
            /* https://stackoverflow.com/questions/31580535/remove-element-at-specific-index-from-redis-list */
            checkIndex(index);
            String toDeleteTempName = UniqueTokenValueGenerator.generateUniqueTokenValue(name);
            Transaction jedisMulti = jedis.multi();
            Response<String> futureDeleted = jedisMulti.lindex(name, index);
            jedisMulti.lset(name, index, toDeleteTempName);
            jedisMulti.lrem(name, 1, toDeleteTempName);
            jedisMulti.exec();
            return futureDeleted.get();
        }
    }

    @Override
    public int indexOf(Object o) {
        String element = (String) o;
        Object result = scriptIndexOf.evalSha(Collections.singletonList(name), Collections.singletonList(element));
        LOGGER.debug("indexOf result {}", result);
        return ((Long) result).intValue();
    }

    @Override
    public int lastIndexOf(Object o) {
        String element = (String) o;
        Object result = scriptLastIndexOf.evalSha(Collections.singletonList(name), Collections.singletonList(element));
        LOGGER.debug("lastIndexOf result {}", result);
        return ((Long) result).intValue();
    }

    @Override
    public ListIterator<String> listIterator() {
        checkExists();
        return new JedisListIterator();
    }

    @Override
    public ListIterator<String> listIterator(int index) {
        checkExists();
        checkIndex(index);
        return new JedisListIterator().withInitialIndex(index);
    }

    @Override
    public List<String> subList(int fromIndex, int toIndex) {
        try (Jedis jedis = jedisPool.getResource()) {
            int effectiveIndex = toIndex - 1;
            checkIndex(fromIndex);
            checkIndex(effectiveIndex);
            return jedis.lrange(name, fromIndex, effectiveIndex);
        }
    }


    /**
     * Implementation of ListIterator and Iterator for JedisList
     */
    private class JedisListIterator implements ListIterator<String> {

        private int currentIndex = 0;
        private int lastReturned = -1;

        /**
         * Set an initial index to the iterator
         * @param initalIndex Initial index to put, will be checked
         * @return this
         */
        private JedisListIterator withInitialIndex(int initalIndex) {
            JedisList.this.checkIndex(initalIndex);
            this.currentIndex = initalIndex;
            this.lastReturned = currentIndex - 1;
            return this;
        }

        @Override
        public boolean hasNext() {
            return currentIndex < size();
        }

        @Override
        public String next() {
            String value = null;
            if (hasNext()) {
                value = get(currentIndex);
                lastReturned = currentIndex;
                currentIndex++;
            }
            return value;
        }

        @Override
        public boolean hasPrevious() {
            return currentIndex > 0;
        }

        @Override
        public String previous() {
            if (hasPrevious()) {
                currentIndex--;
                lastReturned = currentIndex;
                return get(currentIndex);
            } else {
                return null;
            }
        }

        @Override
        public int nextIndex() {
            return currentIndex + 1;
        }

        @Override
        public int previousIndex() {
            return currentIndex - 1;
        }

        @Override
        public void remove() {
            JedisList.this.remove(lastReturned);
            int currentSize = size();
            if (currentSize == 0) {
                currentIndex = 0;
                lastReturned = -1;
            } else if (currentIndex >= currentSize ) {
                currentIndex = currentSize - 1;
                lastReturned = currentIndex;
            }
        }

        @Override
        public void set(String s) {
            JedisList.this.set(lastReturned, s);
        }

        @Override
        public void add(String s) {
            JedisList.this.add(currentIndex, s);
        }

    }

}
