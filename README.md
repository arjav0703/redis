# Redis 

This is a simple Redis server written in Rust 🦀.

### Supported Commands (through the redis-cli):
- `ECHO`
- `PING`
- `SET`
- `GET`
- `KEYS`
- `CONFIG GET`
- `INFO` (work in progress)

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

# Using the INFO command (work in progress)
redis-cli INFO replication
```
### Features
- In-memory key-value store
- Load previous state from RDB file
- Set keys with expiry
- Works with the official redis-cli

--- 
<div align="center">
  <a href="https://shipwrecked.hackclub.com/?t=ghrm" target="_blank">
    <img src="https://hc-cdn.hel1.your-objectstorage.com/s/v3/739361f1d440b17fc9e2f74e49fc185d86cbec14_badge.png" 
         alt="This project is part of Shipwrecked, the world's first hackathon on an island!" 
         style="width: 35%;">
  </a>
</div>
