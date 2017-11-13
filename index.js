var Redis = require('ioredis');
var Q = require('q');

function ret(opt) {

    opt = opt || { keyPrefix: "devcache:" };

    if (opt.hasOwnProperty('connect_timeout')) {

        opt.connectTimeout = opt.connect_timeout;
        opt.db = opt.database;
        opt.keyPrefix = opt.prefix+':';
    
        delete opt.connect_timeout;
        delete opt.database;
        delete opt.engine;
        delete opt.prefix;
        delete opt.retry_strategy;
        //this.baseOpt = opt;
    }
    
    this.opt = opt

    if (!global.joinRedis) {
        if (!this.opt.cluster)
            global.joinRedis = new Redis(this.opt);
        else
            global.joinRedis = new Redis.Cluster(this.opt);
    } else {
        console.log('has redis');
    }
    return this;
};

function genericPromiseCallback(callback) {
    var deferred = Q.defer();
    var cb = function (err, value) {
        if (err) {
            return deferred.reject(err);
        }
        if (typeof value === 'string'){
            if (value.indexOf('{')>=0 || value.indexOf('[')>=0)
                value = JSON.parse(value);
        }
        return deferred.resolve(value);
    }
    deferred.promise.nodeify(callback);
    return {'promise': deferred.promise, 'cb': cb};
}

/**
 * string get
 */
ret.prototype.get = function (key, callback) {
    var deferred = genericPromiseCallback(callback);
    global.joinRedis.get(this.opt.subPrefixKey + ':' + key, deferred.cb);
    return deferred.promise;
}

/**
 * string set
 */
ret.prototype.set = function (key, value, ex, callback) {
    var deferred = genericPromiseCallback(callback);
    if (typeof value !== 'string')
        value = JSON.stringify(value);
    if (ex !== undefined) {
        if (typeof ex !== 'number') {
            var unit = 1;
            if (ex.indexOf('d') > 0)
                unit = 3600 * 24;
            else if (ex.indexOf('h') > 0)
                unit = 3600;
            else if (ex.indexOf('m') > 0)
                unit = 60;
            ex = parseInt(ex) * unit;
        }
        global.joinRedis.setex(this.opt.subPrefixKey + ':' + key, ex, value, deferred.cb);
    } else {
        global.joinRedis.set(this.opt.subPrefixKey + ':' + key, 60, value, deferred.cb);
    }
    return deferred.promise;
}


/**
 * delete key
 */
ret.prototype.del = function (keyPath, callback) {
    var deferred = genericPromiseCallback(callback);
    var self = this;
    var perKey = self.opt.keyPrefix + self.opt.subPrefixKey + ':';
    var _key = perKey + keyPath;

    global.joinRedis.keys(_key).then(function (result) {
        var len = result.length;
        var i = 0;

        if (len === 0) {
            deferred.cb(null, null);
            return;
        }

        result.forEach(function (key) {
            global.joinRedis.del(key.replace(self.opt.keyPrefix, ''), function (err, result) {
                console.log('delete key=' + key, result === 1);
                if (i == len-1)
                    deferred.cb(err, result);
                i++;
            });
        });
    });
    return deferred.promise;
}

ret.prototype.clear = function (callback) {
    var deferred = genericPromiseCallback(callback);
    this.del('*', deferred.cb);
    return deferred.promise;
}

/**
 * List set
 */
ret.prototype.sadd = function (key, list, callback) {
    var deferred = genericPromiseCallback(callback);
    global.joinRedis.sadd(this.opt.subPrefixKey + ':' + key, list, deferred.cb);
    return deferred.promise;
}

/**
 * List get //SMEMBERS 
 */
ret.prototype.sget = function (key, callback) {
    var deferred = genericPromiseCallback(callback);
    global.joinRedis.smembers(this.opt.subPrefixKey + ':' + key, deferred.cb);
    return deferred.promise;
}

/**
 * hash set
 */
ret.prototype.hmset = function (key, obj, callback) {
    var deferred = genericPromiseCallback(callback);
    global.joinRedis.hmset(this.opt.subPrefixKey + ':' + key, obj, deferred.cb);
    return deferred.promise;
}

/**
 * hash get
 */
ret.prototype.hmget = function (key, fields, callback) {

    if (typeof fields === 'function') {
        callback = fields;
        fields = undefined;
    }

    var deferred = genericPromiseCallback(callback);

    if (fields !== undefined)
        global.joinRedis.hmget(this.opt.subPrefixKey + ':' + key, fields, deferred.cb);
    else
        global.joinRedis.hgetall(this.opt.subPrefixKey + ':' + key, deferred.cb);

    return deferred.promise;
}

module.exports = ret;