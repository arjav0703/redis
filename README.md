# Redis 

This is a simple Redis server written in Rust 🦀.

### Supported Commands (through the redis-cli):
- `ECHO`
- `PING`
- `SET`
- `GET`
- `KEYS`
- `CONFIG GET`
- `INFO`
- `WAIT`
- `TYPE`
- `XRANGE` 
- `XADD`
- `XREAD`
- `BLOCK`
- `SUBSCRIBE`
- `PUBLISH`
- `UNSUBSCRIBE`
- `ZADD`
- `ZRANGE`
- `ZREM`
- `ZRANK`
- `ZCARD`
- `GEOADD`
- `GEOPOS`
- `GEODIST`
- `GEOSEARCH`
- `INCR`
- `MULTI`
- `EXEC`
- `DISARD`

### CLI args for the server:
- `--dir <DIR>`: Directory where the rdb file is located (default: current directory)
- `--dbfilename <DBFILENAME>`: Name of the rdb file (default: dump.rdb)
- `--port <PORT>`: Port to run the server on (default: 6379)


### How to use: 
Download the binary from the [releases](github.com/arjav0703/redis/releases) page or build it from source using
Binary: 
```bash
./redis --dir ./ --dbfilename dump.rdb
```

Source code:
```bash
git clone https://github.com/arjav0703/redis.git
cd redis
cargo run -- --dir ./ --dbfilename dump.rdb
```
### Testing Instructions

1. Install Redis cli from https://redis.io/docs/latest/operate/oss_and_stack/install/archive/install-redis/
2. Start the Redis server using the above instructions.
3. Open a new terminal and run the Redis CLI:
```bash
redis-cli PING
redis-cli ECHO "Hello, Redis!"
redis-cli SET foo bar
redis-cli GET foo

# setting a key with expiry
redis-cli SET fruit banana PX 100 # milliseconds
redis-cli GET fruit

sleep 0.1 
redis-cli GET fruit # should return nil

# Using the CONFIG command (partial support)
redis-cli CONFIG GET dir 
redis-cli CONFIG GET dbfilename

# Using the KEYS command
# get all keys
redis-cli KEYS *
# get keys that start with 'f'
redis-cli KEYS f

# Using the INFO command 
redis-cli INFO replication

# Using the WAIT command
redis-cli WAIT 1 1000 # waits for 1 replica to acknowledge that it is synced with the master within 1000 ms

# Using the XADD and XRANGE commands to work with streams
redis-cli XADD stream_key 0-1 temperature 95
redis-cli XADD other_stream_key 0-2 humidity 97

# Reading from streams using XRANGE
redis-cli XRANGE stream_key - +

# Determining if a key is a stream or a string 
redis-cli TYPE stream_key
# responds with either `stream`, `string` or `none`

redis-cli XREAD streams stream_key other_stream_key 0-0 0-1

## Blocking read from streams
redis-cli XREAD BLOCK 5000 STREAMS stream_key $

# Subscribe to a channel
redis-cli SUBSCRIBE my_channel
# In another terminal, publish a message to the channel
redis-cli PUBLISH my_channel "Hello, Subscribers!"
# Unsubscribe from the channel
redis-cli UNSUBSCRIBE my_channel


# Using sorted sets commands
redis-cli ZADD my_zset 1 "one"
redis-cli ZADD my_zset 2 "two"
redis-cli ZRANGE my_zset 0 -1 

redis-cli ZRANK my_zset "two"
redis-cli ZCARD my_zset
redis-cli ZREM my_zset "one"

# Using geospatial commands
redis-cli GEOADD my_geo 13.361389 38.115556 "Palermo" #add to db
redis-cli GEOADD my_geo 15.087269 37.502669 "Catania" #add to db
redis-cli GEOPOS my_geo "Palermo" "Catania" #returns their coordinates
redis-cli GEODIST my_geo "Palermo" "Catania" #returns distance between them
redis-cli GEOSEARCH places FROMLONLAT 2 48 BYRADIUS 1000000 m #search for places within the specified radius from the specified coordinates

# Using INCR command
redis-cli SET counter 10
redis-cli INCR counter
redis-cli GET counter

# Using MULTI and EXEC commands for transactions
redis-cli MULTI
redis-cli SET key1 value1
redis-cli GET key1
redis-cli SET key2 value2
redis-cli EXEC

# Using DISCARD to cancel a transaction
redis-cli MULTI
redis-cli SET key3 value3
redis-cli DISCARD

```
### Features
- In-memory key-value store
- Load previous state from RDB file
- Set keys with expiry
- Works with the official redis-cli
- Replication support (both as master and replica)
- Channel-based pub/sub
- Transactions using MULTI/EXEC
- Geospatial data support
- Sorted sets
- Streams


### Replication
- The server can act as a replica and sync data from a master Redis server. To start the server as a replica, use the following command:
1. Run a master redis server:
```bash
cargo run # -- --port <MASTER_PORT>
```
2. Run the replica server:
```bash
cargo run -- --replicaof <MASTER_IP> <MASTER_PORT> --port <REPLICA_PORT>
```

3. Send commands to the master server using redis-cli:
```bash
redis-cli -p <MASTER_PORT> SET key value
```

4. Check if the replica server has synced the data:
```bash
redis-cli -p <REPLICA_PORT> GET key
```

--- 
<div align="center">
  <a href="https://shipwrecked.hackclub.com/?t=ghrm" target="_blank">
    <img src="https://hc-cdn.hel1.your-objectstorage.com/s/v3/739361f1d440b17fc9e2f74e49fc185d86cbec14_badge.png" 
         alt="This project is part of Shipwrecked, the world's first hackathon on an island!" 
         style="width: 35%;">
  </a>
</div>
