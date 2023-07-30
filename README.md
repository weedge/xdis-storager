# xdis-storager
This library is used to implement the mapping between resp cmd(like redis commands) and openkv store impl

# feature
1. support slot tag key migrate, for (smart client/proxy)'s configSrv admin contoller lay use it~~
use `SLOTSMGRTTAGSLOT` cmd to migrate slot's key with same tag
```shell
127.0.0.1:6660> setex 122{tag} 86400 v3
OK
127.0.0.1:6660> setex 1{tag} 86400 v3
OK
127.0.0.1:6660> sadd 12{tag} m1 m2 m3
(integer) 0
127.0.0.1:6660> hmset 123{tag} f1 v1 f2 v2
OK
127.0.0.1:6660> lpush 123{tag} l1 l2
(integer) 2
127.0.0.1:6660> zadd 123{tag} 100 z1 10 z2
(integer) 2
127.0.0.1:6660> slotshashkey 123{tag}
1) (integer) 899
127.0.0.1:6660> slotsinfo 899 0 withsize
1) 1) (integer) 899
   2) (integer) 6
127.0.0.1:6660> SLOTSMGRTTAGSLOT 127.0.0.1 6666 300000 899
(integer) 6
127.0.0.1:6660> SLOTSMGRTTAGSLOT 127.0.0.1 6666 300000 899
(integer) 0
```
```shell
127.0.0.1:6666> slotsinfo 0 1024 withsize
1) 1) (integer) 899
   2) (integer) 6
127.0.0.1:6666> get 122{tag}
"v3"
127.0.0.1:6666> ttl 122{tag}
(integer) 86133
127.0.0.1:6666> get 1{tag}
"v3"
127.0.0.1:6666> ttl 1{tag}
(integer) 86120
127.0.0.1:6666> hgetall 123{tag}
1) "f1"
2) "v1"
3) "f2"
4) "v2"
127.0.0.1:6666> lrange 123{tag} 0 -1
1) "l2"
2) "l1"
127.0.0.1:6666> zrange 123{tag} 0 10 withscores
1) "z2"
2) "10"
3) "z1"
4) "100"
```

# reference
* [ledisdb](https://github.com/ledisdb/ledisdb)
