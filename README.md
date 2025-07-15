# Redis 

This is a simple Redis server written in Rust 🦀.

### Supported Commands
- `ECHO`
- `PING`
- `SET`
- `GET`


### How to use: 
Download the binary from the [releases](github.com/arjav0703/redis/releases) page or build it from source using
```bash
git clone https://github.com/arjav0703/redis.git
cd redis
cargo run
```

### Testing Instructions

1. Install Redis cli from https://redis.io/docs/latest/operate/oss_and_stack/install/archive/install-redis/
2. Start the Redis server using the above instructions.
3. Open a new terminal and run the Redis CLI:
```bash
redis-cli PING
redis-cli ECHO "Hello, Redis!"
redis-cli SET key value
redis-cli GET key
```


--- 
<div align="center">
  <a href="https://shipwrecked.hackclub.com/?t=ghrm" target="_blank">
    <img src="https://hc-cdn.hel1.your-objectstorage.com/s/v3/739361f1d440b17fc9e2f74e49fc185d86cbec14_badge.png" 
         alt="This project is part of Shipwrecked, the world's first hackathon on an island!" 
         style="width: 35%;">
  </a>
</div>
