# redis-tool

**redis-tool** is a convenient tool to use redis.

## Usage

    $ python redis-tool.py -h
    python redis-tool.py [options]
    
    options:
    -h,--help: show this help message
    -v,--version: show the version
    -H: target host
    -P: target port
    -p: target user password
    -o: output the status to this file
    -e: output error message to this file
    -i: time interval to show the status, unit is second
    --socket: the socket file to use for connection
    
## Dependence

[redis-py](https://github.com/andymccurdy/redis-py)

## Run

### Export all the keys from redis in the local file.

    $ python redis-tool.py -H 127.0.0.1 -P 6379 -o output.txt
    
All the keys and values will be dump to the output.txt file.