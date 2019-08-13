# InterruptingJedisLocks
Jedis based lock which interrupts the locking thread if the leasing time is out

Two implementing classes
- InterruptingLock will create a new background thread to control the leasing time
- InterruptingLockExecutor will use a ExecutorService to retrieve a  thread to control the leasing time


Jedis is a Java library to use a Redis server with Java, at a low-level commands
https://github.com/xetorthio/jedis


WORK IN PROGRESS !!!
UNDER CONSTRUCTION !!!


¿ Using Redisson instead of Jedis ?
Try my sibling project at: https://github.com/oscar-besga-panel/InterruptingRedissonLocks/
