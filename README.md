# InterruptingJedisLocks

[![Open Source Love](https://badges.frapsoft.com/os/v3/open-source.svg?v=103)](https://github.com/ellerbrock/open-source-badges/)

_Project information_        
[![MIT License](https://img.shields.io/apm/l/atomic-design-ui.svg?)](https://opensource.org/licenses/MIT)
![Top languaje](https://img.shields.io/github/languages/top/oscar-besga-panel/InterruptingJedisLocks)
[![Wiki](https://badgen.net/badge/icon/wiki?icon=wiki&label)](https://github.com/oscar-besga-panel/InterruptingJedisLocks/wiki)
[![Github Web page](https://badgen.net/badge/github/website?icon=github)](https://oscar-besga-panel.github.io/InterruptingJedisLocks)
[![OpenHub](https://badgen.net/badge/%20/openhub/purple?icon=awesome)](https://openhub.net/p/InterruptingJedisLocks)


_Current Build_  
[![Build Status](https://app.travis-ci.com/oscar-besga-panel/InterruptingJedisLocks.svg?branch=master)](https://app.travis-ci.com/github/oscar-besga-panel/InterruptingJedisLocks)
![Issues](https://img.shields.io/github/issues/oscar-besga-panel/InterruptingJedisLocks)
[![codecov](https://codecov.io/gh/oscar-besga-panel/InterruptingJedisLocks/branch/master/graph/badge.svg?token=ED9XKSC2F7)](https://codecov.io/gh/oscar-besga-panel/InterruptingJedisLocks)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/oscar-besga-panel/InterruptingJedisLocks.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/oscar-besga-panel/InterruptingJedisLocks/context:java)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/010964cad8f94b07838e53aa41259792)](https://www.codacy.com/gh/oscar-besga-panel/InterruptingJedisLocks/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=oscar-besga-panel/InterruptingJedisLocks&amp;utm_campaign=Badge_Grade)


## Introduction

If you want to use Redis and java library Jedis to control resources and locks between threads and processes, take a look into this library.  
It contains the next distributed synchronization classes:
- Locks
- Semaphores
- CountDownLatch

All this classes use a Jedis pool connection to make them thread-safe and more efficient.

Also it contains collections that have a direct Redis storage, with no (or very little, as needed) local data.
All changes and operations are made to the underlying redis collection type, but following strictly the interface contract.
The implementations are
- JedisList for java List
- JedisMap for java Map
- JedisSet for java Set

As java collections, you also can rely on iterator and streams to operate (be aware that under the hood there is a redis connection)


Also you have iteators por SCAN, HSCAN, SCAN and ZSCAN operations. The iterable-iterator pair will 
give you easy Java control and semantics over iterating an scan operation in redis.
Also you can have all the data in a list/map with a simple method.

You can use a simple cache implementation on redis. This is done in a javax.cache fashion but simpler (you don't have factories, events, mxbeans, statistics included)
It can load and write data in external datasource at your choice, automatically when retrieving or storing data.
Or iterate by the keys and values stored in the cache.
  
All classes have tests, unit and functional ones.   
You can test the latter ones by activating them and configuring your own redis server, to test that all the classes work properly in theory and practice.  
There are **more than 500 working tests**, so the code is pretty secure.

  
   

**See the [wiki](https://github.com/oscar-besga-panel/InterruptingJedisLocks/wiki) for more documentation**




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

See also
- [Awesome-redis](https://github.com/JamzyWang/awesome-redis)
- And redis/jedis tutorial made by me [YaitRedisAndJedis](https://github.com/oscar-besga-panel/YaitRedisAndJedis)
- A lock (and other synchronization primitives) server based in Java which can be used with RMI, gRPC or REST [LockFactoryServer](https://github.com/oscar-besga-panel/LockFactoryServer)

## How to build
This project uses JDK8 and Gradle (provided gradlew 7.0.2)

Also, you will find a little Groovy and a docker composer to setup a testing redis server.

## TODOs

On branch ``develop/mapper`` I'm trying to make a POJO <-> RedisObject mapper to store objects in Redis directly.  
It's barely working, so it is not production ready.  

On branch ``cache`` I want to implement a javax.cache (JSR107 API and SPI 1.0.0 API) class based on redis; with the most simple 
and straigthforward implementation. Also some simple caches for everyday use.  

On branch ``develop/lockNotification`` I want to implement lock with messages and notifications instead of pooling





Help, suggestions, critics and tests will be greatly appreciated.


