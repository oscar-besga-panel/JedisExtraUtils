## Welcome to InterruptingJedisLocks and more

[![MIT License](https://img.shields.io/apm/l/atomic-design-ui.svg?)](https://opensource.org/licenses/MIT)
[![Build Status](https://app.travis-ci.com/oscar-besga-panel/InterruptingJedisLocks.svg?branch=master)](https://app.travis-ci.com/github/oscar-besga-panel/InterruptingJedisLocks)
![Top languaje](https://img.shields.io/github/languages/top/oscar-besga-panel/InterruptingJedisLocks)
![Issues](https://img.shields.io/github/issues/oscar-besga-panel/InterruptingJedisLocks)
[![codecov](https://codecov.io/gh/oscar-besga-panel/InterruptingJedisLocks/branch/master/graph/badge.svg?token=ED9XKSC2F7)](https://codecov.io/gh/oscar-besga-panel/InterruptingJedisLocks)
[![Open Source Love](https://badges.frapsoft.com/os/v3/open-source.svg?v=103)](https://github.com/ellerbrock/open-source-badges/)



If you want to use Redis and java library Jedis to control resources and locks between threads and processes, take a look into this library.
It contains the next distributed synchronization classes:

- Locks  
- Semaphores  
- CountDownLatch  




```java
  
  // Get a resource under a lock
  JedisLock myJedisLock = new JedisLock(jedisPool, "myJedisLock");   
  myJedisLock.underLock(() -> {  
      // Critical code  
  });  
```

Also it contains collections that have a direct Redis storage, with no (or very little, as needed) local data. All changes and operations are made to the underlying redis collection type, but following strictly the interface contract. The implementations are

- JedisList for java List
- JedisMap for java Map
- JedisSet for java Set

As java collections, you also can rely on iterator and streams to operate (be aware that under the hood there is a redis connection)

```java
  // Put an element in a share list
  List<String> myJedisList = new JedisList(jedisPool, "myJedisList");
  myJedisList.add("shared data");

```
Also you have iteators por SCAN, HSCAN, SCAN and ZSCAN operations.  
The iterable-iterator pair will give you easy Java control and semantics over iterating an scan operation in redis.


```java
  // Iterate from a key search 
  ScanIterable scanIterable = new ScanIterable(jedisPool, "my*");
  for(String foundKey: scanIterable) {
      LOGGER.info("Key found {}", foundKey);
  }
  
```
All this classes use a Jedis pool connection to make them thread-safe and more efficient.


All in an open-source, clean-code, well-tested project avalible now !

**Like it ?**  
See the [code](https://github.com/oscar-besga-panel/InterruptingJedisLocks/) or the [wiki](https://github.com/oscar-besga-panel/InterruptingJedisLocks/wiki) for more !

