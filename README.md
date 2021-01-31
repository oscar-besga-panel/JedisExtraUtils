# InterruptingJedisLocks

[![MIT License](https://img.shields.io/apm/l/atomic-design-ui.svg?)](https://github.com/tterb/atomic-design-ui/blob/master/LICENSEs)
[![Build Status](https://travis-ci.org/oscar-besga-panel/InterruptingJedisLocks.svg?branch=master)](https://travis-ci.org/oscar-besga-panel/InterruptingJedisLocks)

## Basics

If you want to use Redis and java library Jedis to control resources and locks between threads and processes, take a look into this library.  
It contains the next distributed synchronization classes:
- Locks
- Semaphores
- CountDownLatch

All classes have tests, unit and functional ones. You can test the latter ones by activating them and configuring your own redis server, to test that all the classes work properly in theory and practice.

See the [wiki](https://github.com/oscar-besga-panel/InterruptingJedisLocks/wiki) for more documentation

## Extra

Jedis is a Java library to use a Redis server with Java, at a low-level commands
https://github.com/xetorthio/jedis


WORK IN PROGRESS !!!  
UNDER CONSTRUCTION !!!


Made with
- Intellij
- https://pandao.github.io/editor.md/en.html 
- Draw io https://app.diagrams.net/


## TODO

On branch ``develop/mapper`` I'm trying to make a POJO <-> RedisObject mapper to store objects in Redis directly.  
It's barely working, so it is not production ready.
  
  
On branch ``develop/mapper`` I'm trying to make a direct implementation on Redis of basic Java collections; like List, Map and Set.
For the moment only List is implemented, but more tests are needed. So it is not production ready. 


Help, suggestions, critics and tests will be greatly appreciated.

 
 
