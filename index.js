/**
 * Copyright JS Foundation and other contributors, http://js.foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

/**
 * Redis based context storage
 *
 * Configuration options:
 * {
 *    host: '127.0.0.1',       // The IP address of the Redis server
 *                             // default: '127.0.0.1'
 *    port: 6379,              // The port of the Redis server
 *                             // default: 6379
 *    db: 0                    // The Redis logical database to connect
 *                             // default: 0
 *    prefix:                  // The string used to prefix all used keys
 *                             // If set, the plugin uses 'prefix + scope + keyname' as key
 *                             // (e.g. prefix:'foo', global.get('key') -> GET foo:global:key )
 *                             // default: undefined
 *    password:                // If set, the plugin will run Redis AUTH command on connect
 *                             // Note: the password will be sent as plaintext
 *                             // default: undefined
 *    tls:                     // An object containing options to pass to tls.connect to set up a TLS connection to Redis
 *                             // default: undefined
 *    retry_strategy:          // Specifies a function to reconnect if the connection to Redis is lost.
 *                             // default: undefined (Use the default retry strategy)
 *  }
 *
 * This plugin prefixes all used keys with context scope.
 * For example
 *   context.get('foo') ->  The plugin will get the value of '<id of Node>:foo' (e.g. '36b85111.47f5fe:5b17c82f.6a0888:foo')
 *   flow.get('foo')    ->  The plugin will get the value of '<id of Flow>:foo' (e.g. '5b17c82f.6a0888:foo')
 *   global.get('foo')  ->  The plugin will get the value of 'global:foo'
 *
 * If 'prefix' in above options is set, the key will be prefixed with it additionally.
 */

const redis = require('redis');
// Require @node-red/util loaded in the Node-RED runtime.
const util = process.env.NODE_RED_HOME ?
    require(require.resolve('@node-red/util', { paths: [process.env.NODE_RED_HOME] })).util :
    require('@node-red/util').util;
const log = process.env.NODE_RED_HOME ?
    require(require.resolve('@node-red/util', { paths: [process.env.NODE_RED_HOME] })).log :
    require('@node-red/util').log;

const safeJSONStringify = require('json-stringify-safe');

// This lua script sets a nested property to JSON atomically
// Usage: EVALSHA(SHA, 1, key, property, [property...], JSON)
// e.g. Set obj.a.b.c to {foo: 'bar'} -> EVALSHA(SHA, 1, 'obj', 'a', 'b', 'c', '{"foo":"bar"}');
const setScript = `
    -- get the value of key
    local data = redis.call('GET', KEYS[1]);
    if data then
        data = cjson.decode(data);
    else
        data = {}
    end
    -- parse path
    local path = data;
    local next;
    for i = 1, #ARGV-1 do
        next = tonumber(ARGV[i]);
        if next then
            next = next + 1;
        else
            next = ARGV[i]
        end
        if i == #ARGV-1 then
            break;
        end
        if not path[next] then
            path[next] = {};
        end
        path = path[next];
    end
    path[next] = cjson.decode(ARGV[#ARGV]);
    -- convert and set the value
    return redis.call('SET', KEYS[1], cjson.encode(data));
`;

// This lua script deletes a nested property atomically
// Usage: EVALSHA(SHA, 1, key, property, [property...]);
// e.g. Delete obj.a.b.c -> EVALSHA(SHA, 1, 'obj', 'a', 'b', 'c');
const deleteScript = `
    -- get the value of key
    local data = redis.call('GET', KEYS[1]);
    if data then
        data = cjson.decode(data);
    end
    -- parse path
    local path = data;
    local next = tonumber(ARGV[1]);
    for i = 2, #ARGV do
        if not path then
            return 0;
        end
        if next then
            path = path[next+1];
        else
            path = path[ARGV[i-1]];
        end
        next = tonumber(ARGV[i]);
    end
    -- delete the property
    if next and path[next+1] then
        table.remove(path, next+1);
    elseif path[ARGV[#ARGV]] then
        path[ARGV[#ARGV]] = nil;
    else
    -- return if try to delete non-existent value
        return 0
    end
    -- convert and set the value
    return redis.call('SET', KEYS[1], cjson.encode(data));
`;

function stringify(value) {
    let hasCircular;
    let result = safeJSONStringify(value, null, null, function (k, v) { hasCircular = true; });
    return { json: result, circular: hasCircular };
}

function addPrefix(prefix, scope, key) {
    if (prefix) {
        scope = prefix + ':' + scope;
    }
    return scope + ':' + key;
}

function removePrefix(prefix, scope, key) {
    if (prefix) {
        key = key.substring((prefix + ':').length);
    }
    return key.substring((scope + ':').length);
}

function scan(client, pattern, cursor = 0) {
    return new Promise((resolve, reject) => {
        client.SCAN(cursor, 'MATCH', pattern, 'COUNT', 1000, (err, results) => {
            if (err) {
                return reject(err);
            } else {
                const cursor = results[0];
                const elements = results[1];
                if (cursor === "0") {
                    //the iteration finished
                    resolve(elements);
                } else {
                    scan(client, pattern, cursor).then(result => {
                        resolve(elements.concat(result));
                    });
                }
            }
        });
    });
}

function Redis(config) {
    this.host = config.host || '127.0.0.1';
    this.port = config.port || 6379;
    this.prefix = config.prefix;
    this.options = {
        db: config.db || 0,
        password: config.password,
        tls: config.tls,
        retry_strategy: config.retry_strategy || undefined
    };
    this.client = null;
    this.knownCircularRefs = {};
}

Redis.prototype.open = function () {
    const promises = [];
    this.client = redis.createClient(this.port, this.host, this.options);
    this.client.on('error', function (err) {
        log.error(err);
    });
    promises.push(new Promise((resolve, reject) => {
        // Load the script into the scripts cache of Redis
        this.client.SCRIPT('load', setScript, (err, res) => {
            if (err) {
                reject(err.origin || err);
            } else {
                this.setSHA = res;
                resolve();
            }
        });
    }));
    promises.push(new Promise((resolve, reject) => {
        // Load the script into the scripts cache of Redis
        this.client.SCRIPT('load', deleteScript, (err, res) => {
            if (err) {
                reject(err.origin || err);
            } else {
                this.deleteSHA = res;
                resolve();
            }
        });
    }));
    return Promise.all(promises);
};

Redis.prototype.close = function () {
    return new Promise((resolve, reject) => {
        this.client.QUIT((err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
};

Redis.prototype.get = function (scope, key, callback) {
    if (typeof callback !== 'function') {
        throw new Error('Callback must be a function');
    }
    try {
        if (!Array.isArray(key)) {
            key = [key];
        }
        const mgetArgs = [];
        // Filter duplicate keys in order to reduce response data
        const rootKeys = key.map(key => util.normalisePropertyExpression(key)[0]).filter((key, index, self) => self.indexOf(key) === index);
        rootKeys.forEach(key => mgetArgs.push(addPrefix(this.prefix, scope, key)));
        this.client.MGET(...mgetArgs, (err, replies) => {
            if (err) {
                callback(err);
            } else {
                let results = [];
                let data = {};
                let value;
                for (let i = 0; i < rootKeys.length; i++) {
                    try {
                        if (replies[i]) {
                            data[rootKeys[i]] = JSON.parse(replies[i]);
                        }
                    } catch (err) {
                        // If data is not JSON, return `undefined`
                        break;
                    }
                }
                for (let i = 0; i < key.length; i++) {
                    try {
                        value = util.getObjectProperty(data, key[i]);
                    } catch (err) {
                        if (err.code === 'INVALID_EXPR') {
                            throw err;
                        }
                        value = undefined;
                    }
                    results.push(value);
                }
                callback(null, ...results);
            }
        });
    } catch (err) {
        callback(err);
        return;
    }
};

Redis.prototype.set = function (scope, key, value, callback) {
    if (callback && typeof callback !== 'function') {
        throw new Error('Callback must be a function');
    }
    try {
        if (!Array.isArray(key)) {
            key = [key];
            value = [value];
        } else if (!Array.isArray(value)) {
            // key is an array, but value is not - wrap it as an array
            value = [value];
        }
        const multi = this.client.MULTI();
        let msetArgs = [];
        let delArgs = [];
        // parse key
        const keyParts = key.map(key => util.normalisePropertyExpression(key));

        for (let i = 0; i < key.length; i++) {
            if (i >= value.length) {
                value[i] = null;
            }
            keyParts[i][0] = addPrefix(this.prefix, scope, keyParts[i][0]);

            if (value[i] !== undefined) { // set a value
                const stringifiedContext = stringify(value[i]);

                if (stringifiedContext.circular && !this.knownCircularRefs[keyParts[i][0]]) {
                    log.warn(log._('context.localfilesystem.error-circular', { scope: keyParts[i][0] }));
                    this.knownCircularRefs[keyParts[i][0]] = true;
                } else {
                    delete this.knownCircularRefs[keyParts[i][0]];
                }

                if (delArgs.length > 0) {
                    // Queue a command in order to execute commands sequentially
                    multi.DEL(...delArgs);
                    delArgs = [];
                }
                if (keyParts[i].length === 1) {
                    msetArgs.push(keyParts[i][0], stringifiedContext.json);
                } else {
                    if (msetArgs.length > 0) {
                        multi.MSET(...msetArgs);
                        msetArgs = [];
                    }
                    // To set a nested property atomically, call the lua script
                    multi.EVALSHA(this.setSHA, 1, ...keyParts[i], stringifiedContext.json);
                }
            } else { // delete a value
                delete this.knownCircularRefs[keyParts[i][0]];

                if (msetArgs.length > 0) {
                    // Queue a command in order to execute commands sequentially
                    multi.MSET(...msetArgs);
                    msetArgs = [];
                }
                if (keyParts[i].length === 1) {
                    delArgs.push(keyParts[i][0]);
                } else {
                    if (delArgs.length > 0) {
                        multi.DEL(...delArgs);
                        delArgs = [];
                    }
                    // To delete a nested property atomically, call the lua script
                    multi.EVALSHA(this.deleteSHA, 1, ...keyParts[i]);
                }
            }
        }
        if (msetArgs.length > 0) {
            multi.MSET(...msetArgs);
        }
        if (delArgs.length > 0) {
            multi.DEL(...delArgs);
        }
        // Execute commands at once with transactions
        multi.EXEC((err, replies) => {
            if (err) {
                if (callback) {
                    callback(err);
                }
            } else {
                if (callback) {
                    callback(null);
                }
            }
        });
    } catch (err) {
        if (callback) {
            callback(err);
        }
    }
};

Redis.prototype.keys = function (scope, callback) {
    if (typeof callback !== 'function') {
        throw new Error('Callback must be a function');
    }
    scan(this.client, addPrefix(this.prefix, scope, '*')).then(result => {
        callback(null, result.map(v => removePrefix(this.prefix, scope, v)));
    }).catch(err => {
        callback(err);
    });
};

Redis.prototype.delete = function (scope) {
    return scan(this.client, addPrefix(this.prefix, scope, '*')).then(result => {
        if (result.length === 0) {
            return;
        } else {
            return new Promise((resolve, reject) => {
                this.client.DEL(...result, err => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
        }
    });
};

Redis.prototype.clean = function (_activeNodes) {
    this.knownCircularRefs = {};
     return new Promise((resolve, reject) => {
        this.client.KEYS((this.prefix || '') + '*', (err, res) => {
            if (err) {
                reject(err);
            } else {
                if(this.prefix){
                    res = res.map(key => key.substring(this.prefix.length + 1))
                }
                res = res.filter(key => !key.startsWith("global"))
                _activeNodes.forEach(scope => {
                    res = res.filter(key => !key.startsWith(scope))
                })
                var remove = [];
                res.forEach(key => remove.push(this.prefix + ":" + key));
                if (remove.length > 0) {
                    this.client.DEL(...remove, (err) => {
                        if (err) {
                            reject(err);
                        } else {
                            resolve();
                        }
                    });
                } else {
                    resolve()
                }
            }
        });
    });
};

module.exports = function (config) {
    return new Redis(config);
};
