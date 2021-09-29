## Welcome to InterruptingJedisLocks



If you want to use Redis and java library Jedis to control resources and locks between threads and processes, take a look into this library.
It contains the next distributed synchronization classes:

- Locks  
- Semaphores  
- CountDownLatch  

All this classes use a Jedis pool connection to make them thread-safe and more efficient.

Also it contains collections that have a direct Redis storage, with no (or very little, as needed) local data. All changes and operations are made to the underlying redis collection type, but following strictly the interface contract. The implementations are

- JedisList for java List
- JedisMap for java Map
- JedisSet for java Set  

As java collections, you also can rely on iterator and streams to operate (be aware that under the hood there is a redis connection)

Also you have iteators por SCAN, HSCAN, SCAN and ZSCAN operations. The iterable-iterator pair will give you easy Java control and semantics over iterating an scan operation in redis.  

```markdown
  
  // Get a resource under a lock
  JedisLock myJedisLock = new JedisLock(jedisPool, "myJedisLock");   
  myJedisLock.underLock(() -> {  
      // Critical code  
  });  
  
  // Put an element in a share list
  List<String> myJedisList = new JedisList(jedisPool, "myJedisList");
  myJedisList.add("shared data");
  
  // Iterate from a key search 
  ScanIterable scanIterable = new ScanIterable(jedisPool, "my*");
  for(String foundKey: scanIterable) {
      LOGGER.info("Key found {}", foundKey);
  }
  
```

All in an open-source, clean-code, well-tested project avalible now !

Like it ?
See the [code](https://github.com/oscar-besga-panel/InterruptingJedisLocks/) or the [Link](https://github.com/oscar-besga-panel/InterruptingJedisLocks/wiki) for more !

