
[![Open Source Love](https://badges.frapsoft.com/os/v3/open-source.svg?v=103)](https://github.com/ellerbrock/open-source-badges/)

_Project information_        
[![MIT License](https://img.shields.io/badge/License-MIT-blue)](https://opensource.org/licenses/MIT)
![Top languaje](https://img.shields.io/github/languages/top/oscar-besga-panel/JedisExtraUtils)
[![Wiki](https://badgen.net/badge/icon/wiki?icon=wiki&label)](https://github.com/oscar-besga-panel/JedisExtraUtils/wiki)
[![OpenHub](https://badgen.net/badge/%20/openhub/purple?icon=awesome)](https://openhub.net/p/JedisExtraUtils)

_Current Build_  
![Issues](https://img.shields.io/github/issues/oscar-besga-panel/InterruptingJedisLocks)
[![codecov](https://codecov.io/gh/oscar-besga-panel/JedisExtraUtils/branch/master/graph/badge.svg?token=ED9XKSC2F7)](https://codecov.io/gh/oscar-besga-panel/JedisExtraUtils)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/010964cad8f94b07838e53aa41259792)](https://app.codacy.com/gh/oscar-besga-panel/JedisExtraUtils/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

# Introduction to Jedis Extra Utils

This is a Java project based on a collection of utilities and helpers to be used with Redis and with Jedis libraries.

Originally conceived as a group of locks, then some synchronization primitives, it has grown until having a respectable collection of utilities.

These include

* Synchronization: primitives to synchronize process and threads one with other
  * Lock: exclusive locks. Normal locks, also interrupting locks and a java Lock implementation.
  * Semaphores
  * CountDownLatch: count down to open the flood gates and allow all waiters to progress
* Collections: redis-backed implementation of Java collection interfaces, with all data stored on Redis, like
  * Lists
  * Map
  * Set
* Iterator: free yourself from Redis SCAN internal hassle and use easy Java iterables or iterators for these operations:
  * HScanIterable: To scan maps
  * ScanIterable: To scan all keys
  * SScanIterable: To scan sets
  * ZScanIterable: To scan ordered sets
  * Some utils more
* Cache: A simple cache with readthrougth and writethrougth operations
* Cycle: A list of elements that cycles for one to the next, and to the initial one; one result per call, in a cycle
* RateLimiter: temporal or bucket limited distributed rate
* More utils like
  * SimplePubSub: a simple pub/sub that only consumes messages via a BiConsumer function


All this classes use a Jedis pool connection to make them thread-safe and more efficient.

It's intended to make possible distributes locking and synchronization, share data across process and aid with distributed computing.

All classes have tests, unit and functional ones.   
You can test the latter ones by activating them and configuring your own redis server, to test that all the classes work properly in theory and practice.  
There are **more than 500 working tests**, so the code is pretty secure.


**See the [wiki](https://github.com/oscar-besga-panel/JedisExtraUtils/wiki) for more documentation**




## Made with

Jedis is a Java library to use a Redis server with Java, at a low-level commands
https://github.com/xetorthio/jedis.  
See it on mvn repository 
https://mvnrepository.com/artifact/redis.clients/jedis

Made with
- Intellij
- Mackdown editor [Editor.md](https://pandao.github.io/editor.md/en.html) 
- Diagrams with [Draw io](https://app.diagrams.net/)
- Bages from [awesome-badges](https://github.com/badges/awesome-badges) and [badgen](https://badgen.net/) and [open-source-badges](https://github.com/ellerbrock/open-source-badges/) 
- Help from Stackoveflow, forums like [Jedis redis forum](https://groups.google.com/g/jedis_redis)
- SHA1 code from [olivertech](http://oliviertech.com/es/java/generate-SHA1-hash-from-a-String/)
- Rate limiters ideas from [Bucket4j](https://bucket4j.com/) and [vbukhtoyarov-java](https://vbukhtoyarov-java.blogspot.com/2021/11/non-formal-overview-of-token-bucket.html)

See also
- [Awesome-redis](https://github.com/JamzyWang/awesome-redis)
- And redis/jedis tutorial made by me [YaitRedisAndJedis](https://github.com/oscar-besga-panel/YaitRedisAndJedis)
- A lock (and other synchronization primitives) server based in Java which can be used with RMI, gRPC or REST [LockFactoryServer](https://github.com/oscar-besga-panel/LockFactoryServer)

## How to build
This project uses JDK11 and Gradle (provided gradlew 7.5.1), and its build top of jedis 4.X libraries

Also, you will find a little Groovy and a docker composer to setup a testing redis server.


## Miscelanea

This project was formerly known as *InterruptingJedisLocks*, and it was renamed to *Jedis Extra Utils*


As Redis stores data into Strings, you may need to convert from POJO to String and viceversa.   
This library doesn't help with that, but in this [wiki page](https://github.com/oscar-besga-panel/JedisExtraUtils/wiki/POJO-Mapping) you may find some clues on how to do it.

Help, suggestions, critics and tests will be greatly appreciated.

## Others

There are other jedis utils in github, most notably
- [siahsang red utils](https://github.com/siahsang/red-utils): redis safe lock
- [yatechorg jedis-utils](https://github.com/yatechorg/jedis-utils): set, list, map, key scanner, lua script util
- [andrepnh jedis-utils](https://github.com/andrepnh/jedis-utils): command blocks
- [vnechiporenko jedis-utils](https://github.com/vnechiporenko/jedis-utils): distributed locks 
- and more....


## In depth

### Locks

See [wiki](https://github.com/oscar-besga-panel/JedisExtraUtils/wiki/Locks) for more information and schemas 

### Collections

Jedis collections have a direct Redis storage, with no (or very little, as needed) local data.
All changes and operations are made to the underlying redis collection type, but following strictly the interface contract.
The implementations are
- JedisList for java List
- JedisMap for java Map
- JedisSet for java Set

As java collections, you also can rely on iterator and streams to operate
(be aware that under the hood there is a redis connection, and parallel streams are not recommended)

See [wiki](https://github.com/oscar-besga-panel/JedisExtraUtils/wiki/Collections) for more information

### Scan iterators

Also you have iterators por SCAN, HSCAN, SCAN and ZSCAN operations. The iterable-iterator pair will
give you easy Java control and semantics over iterating an scan operation in redis.
Also you can have all the data in a list/map with a simple method (the data will be recovered in multiple xSCAN operations
to avoid blocking Redis)

See [wiki](https://github.com/oscar-besga-panel/JedisExtraUtils/wiki/Collections) for more information


### Cache

You can use a simple cache implementation on redis. This is done in a javax.cache fashion but simpler (you don't have factories, events, mxbeans, statistics included)
It can load and write data in external datasource at your choice, automatically when retrieving or storing data.
Or iterate by the keys and values stored in the cache.

See [wiki](https://github.com/oscar-besga-panel/JedisExtraUtils/wiki/Iterators) for more information


### Rate Limiters 

Rate limiters, as bucket and throttling type on redis. Can help you to limit the access to a resource or the cadence of requests
for a server.
The throttling one limits the frequency for one on a specified time, allowing the first and discarding others in this time frame.
The bucket one has a number of permits, that the requesters can try to get. Over time, the number of permits in the group is refilled
(thou there are different strategies to refill). If there are no permits, the requester is not allowed.

See [wiki](https://github.com/oscar-besga-panel/JedisExtraUtils/wiki/RateLimiters) for more information

### Cycle Data

A list of elements that every request gets the next element, and if it reaches the last one y cycles to the first.
Any call gives only one result.
It can be used with an iterator, but because the list cycles it can lead to an infinite loop, so be aware.


See [wiki](https://github.com/oscar-besga-panel/JedisExtraUtils/wiki/CycleData) for more information



