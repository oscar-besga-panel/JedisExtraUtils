# InterruptingJedisLocks

[![MIT License](https://img.shields.io/apm/l/atomic-design-ui.svg?)](https://opensource.org/licenses/MIT)
[![Build Status](https://app.travis-ci.com/oscar-besga-panel/InterruptingJedisLocks.svg?branch=master)](https://app.travis-ci.com/github/oscar-besga-panel/InterruptingJedisLocks)
![Top languaje](https://img.shields.io/github/languages/top/oscar-besga-panel/InterruptingJedisLocks)
![Issues](https://img.shields.io/github/issues/oscar-besga-panel/InterruptingJedisLocks)
[![codecov](https://codecov.io/gh/oscar-besga-panel/InterruptingJedisLocks/branch/master/graph/badge.svg?token=ED9XKSC2F7)](https://codecov.io/gh/oscar-besga-panel/InterruptingJedisLocks)
[![Open Source Love](https://badges.frapsoft.com/os/v3/open-source.svg?v=103)](https://github.com/ellerbrock/open-source-badges/)


## Basics

If you want to use Redis and java library Jedis to control resources and locks between threads and processes, take a look into this library.  
It contains the next distributed synchronization classes:
- Locks
- Semaphores
- CountDownLatch
- 
All this classes use a Jedis pool connection to make them thread-safe and more efficient.

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

 
 
