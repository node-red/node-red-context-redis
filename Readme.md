# Redis Context store plugin

The Redis Context store plugin holds context data in the Redis.

## Pre-requisite

To run this you need a Redis server running. For details see <a href="http://redis.io/" target="_new">the Redis site</a>.

## Install

1. Run the following command in your Node-RED user directory - typically `~/.node-red`

    npm install git+https://github.com/node-red/node-red-context-redis

1. Add a configuration in settings.js:

```javascript
contextStorage: {
    redis: {
        module: require("node-red-context-redis"),
        config: {
            // see below options
        }
    }
}
```

### Options

This plugin exposes some options defined in [node_redis](https://github.com/NodeRedis/node_redis) as itself options.
It needs following configuration options:

| Options        | Description                                                                                                 |
| -------------- | ----------------------------------------------------------------------------------------------------------- |
| host           | The IP address of the Redis server.    `Default: "127.0.0.1"`                                               |
| port           | The port of the Redis server.          `Default: 6379`                                                      |
| db             | The Redis logical database to connect. `Default: 0`                                                         |
| prefix         | If set, the string used to prefix all used keys.                                                            |
| password       | If set, the plugin will run Redis AUTH command on connect. *Note: the password will be sent as plaintext.*  |
| tls            | An object containing options to pass to tls.connect to set up a TLS connection to the server.               |
| retry_strategy | Specifies a function to reconnect if the connection to Redis is lost.<br> `default: undefined (Use the default retry strategy)`|

see https://github.com/NodeRedis/node_redis#options-object-properties

## Data Model

```text
Node-RED                      Redis
+-------------------+         +-------------------------------+
| global context    |         | logical database              |
| +---------------+ |         | +---------------------------+ |
| | +-----+-----+ | |         | | +-----------------+-----+ | |
| | | key |value| | | <-----> | | | global:key      |value| | |
| | +-----+-----+ | |         | | +-----------------+-----+ | |
| +---------------+ |         | |                           | |
|                   |         | |                           | |
| flow context      |         | |                           | |
| +---------------+ |         | |                           | |
| | +-----+-----+ | |         | | +-----------------+-----+ | |
| | | key |value| | | <-----> | | | <flow's id>:key |value| | |
| | +-----+-----+ | |         | | +-----------------+-----+ | |
| +---------------+ |         | |                           | |
|                   |         | |                           | |
| node context      |         | |                           | |
| +---------------+ |         | |                           | |
| | +-----+-----+ | |         | | +-----------------+-----+ | |
| | | key |value| | | <-----> | | | <node's id>:key |value| | |
| | +-----+-----+ | |         | | +-----------------+-----+ | |
| +---------------+ |         | +---------------------------+ |
+-------------------+         +-------------------------------+
```

- This plugin uses a Redis logical database for all context scope.
- This plugin prefixes all used keys with context scope in order to identify the scope of the key.
  - The keys of `global context` will be prefixed with `global:` .
    e.g.  Set `"foo"` to hold `"bar"` in the global context -> Set `"global:foo"` to hold `"bar"` in the Redis logical database.
  - The keys of `flow context` will be prefixed with `<id of the flow>:` .
    e.g.  Set `"foo"` to hold `"bar"` in the flow context whose id is `8588e4b8.784b38` -> Set `"8588e4b8.784b38:foo"` to hold `"bar"` in the Redis.
  - The keys of `node context` will be prefixed with `<id of the node>:` .
    e.g.  Set `"foo"` to hold `"bar"` in the node context whose id is `80d8039e.2b82:8588e4b8.784b38` -> Set `"80d8039e.2b82:8588e4b8.784b38:foo"` to hold `"bar"` in the Redis.

## Data Structure

- This plugin converts a value of context to JSON and stores it as string type to the Redis.
- After getting a value from the Redis, the plugin also converts the value to an object or a primitive value.

```text
Node-RED                                 Redis
+------------------------------+         +---------------------------------------------+
| global context               |         | logical database                            |
| +--------------------------+ |         | +-----------------------------------------+ |
| | +--------+-------------+ | |         | | +---------------+---------------------+ | |
| | | str    | "foo"       | | | <-----> | | | global:str    | "\"foo\""           | | |
| | +--------+-------------+ | |         | | +---------------+---------------------+ | |
| | | num    | 1           | | | <-----> | | | global:num    | "1"                 | | |
| | +--------+-------------+ | |         | | +---------------+---------------------+ | |
| | | nstr   | "10"        | | | <-----> | | | global:nstr   | "\"10\""            | | |
| | +--------+-------------+ | |         | | +---------------+---------------------+ | |
| | | bool   | false       | | | <-----> | | | global:bool   | "false"             | | |
| | +--------+-------------+ | |         | | +---------------+---------------------+ | |
| | | arr    | ["a","b"]   | | | <-----> | | | global:arr    | "[\"a\",\"b\"]"     | | |
| | +--------+-------------+ | |         | | +---------------+---------------------+ | |
| | | obj    | {foo,"bar"} | | | <-----> | | | global:obj    | "{\"foo\",\"bar\"}" | | |
| | +--------+-------------+ | |         | | +---------------+---------------------+ | |
| +--------------------------+ |         | +-----------------------------------------+ |
+------------------------------+         +---------------------------------------------+
```

Other Redis client(e.g. redis-cli) can get the value stored by Node-RED like followings.

Node-RED

```javascript
global.set("foo","bar","redis");
global.set("obj",{key:"value"},"redis");
```

redis-cli

```console
redis> GET global:foo
"\"var\""
redis> GET global:obj
"{\"key\":\"value\"}"
redis>
```
