# InterruptingJedisLocks

[![MIT License](https://img.shields.io/apm/l/atomic-design-ui.svg?)](https://opensource.org/licenses/MIT)
![Top languaje](https://img.shields.io/github/languages/top/oscar-besga-panel/InterruptingJedisLocks)
[![Wiki](https://badgen.net/badge/icon/wiki?icon=wiki&label)](https://github.com/oscar-besga-panel/InterruptingJedisLocks/wiki)
[![Github Web page](https://badgen.net/badge/github/website?icon=github)](https://oscar-besga-panel.github.io/InterruptingJedisLocks)
[![Open Source Love](https://badges.frapsoft.com/os/v3/open-source.svg?v=103)](https://github.com/ellerbrock/open-source-badges/)



## Current build

[![Build Status](https://app.travis-ci.com/oscar-besga-panel/InterruptingJedisLocks.svg?branch=master)](https://app.travis-ci.com/github/oscar-besga-panel/InterruptingJedisLocks)
![Issues](https://img.shields.io/github/issues/oscar-besga-panel/InterruptingJedisLocks)
[![codecov](https://codecov.io/gh/oscar-besga-panel/InterruptingJedisLocks/branch/master/graph/badge.svg?token=ED9XKSC2F7)](https://codecov.io/gh/oscar-besga-panel/InterruptingJedisLocks)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/oscar-besga-panel/InterruptingJedisLocks.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/oscar-besga-panel/InterruptingJedisLocks/context:java)



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

  
All classes have tests, unit and functional ones.   
You can test the latter ones by activating them and configuring your own redis server, to test that all the classes work properly in theory and practice.  
There are more than 300 working tests, so the code is pretty secure.

  
   

**See the [wiki](https://github.com/oscar-besga-panel/InterruptingJedisLocks/wiki) for more documentation**




## Made with

Jedis is a Java library to use a Redis server with Java, at a low-level commands
https://github.com/xetorthio/jedis

Made with
- Intellij
- Mackdown editor [Editor.md](https://pandao.github.io/editor.md/en.html) 
- Diagrams with [Draw io](https://app.diagrams.net/)
- Bages from [awesome-badges](https://github.com/badges/awesome-badges) and [badgen](https://badgen.net/) and [open-source-badges](https://github.com/ellerbrock/open-source-badges/) 
- Help from Stackoveflow, forums like [Jedis redis forum](https://groups.google.com/g/jedis_redis)

See also
- [Awesome-redis](https://github.com/JamzyWang/awesome-redis)
- And redis/jedis tutorial made by me [YaitRedisAndJedis](https://github.com/oscar-besga-panel/YaitRedisAndJedis)



## TODOs

On branch ``develop/mapper`` I'm trying to make a POJO <-> RedisObject mapper to store objects in Redis directly.  
It's barely working, so it is not production ready.  

On branch ``cache`` I want to implement a javax.cache (JSR107 API and SPI 1.0.0 API) class based on redis; with the most simple 
and straigthforward implementation. Also some simple caches for everyday use.  



Help, suggestions, critics and tests will be greatly appreciated.


