#!/usr/bin/nodejs
var async = require('async');
var redis = require('redis');
var redisLock = require('redislock');
var RedisSMQ = require('rsmq');

var client = redis.createClient();
var rsmq = new RedisSMQ({ client: client, ns: 'rsmq' });

function getQueues(cb) {
    rsmq.listQueues(function listQueues(err, queues) {
        if (err) {
            console.error('list queues', err);
        }

        cb(err, queues);
    });
}

function getMessage(queue, cb) {
    rsmq.receiveMessage({ qname: queue }, function rcvMsg(err, msg) {
        if (err && err.name === 'queueNotFound') {
            cb('empty', null, null);
        } else if (err) {
            console.log('message error', queue, err);
            cb(err, null, null);
        } else if (!msg.id) {
            cb('fin', queue, msg);
        } else {
            cb(err, queue, msg);
        }
    });
}

function processMessage(queue, msg, cb) {
    if (msg.id) {
        console.log('processed msg', msg);
    }

    cb(null, queue, msg);
}

function deleteMessage(queue, msg, cb) {
    if (!msg.id) {
        cb(null);
    } else {
        rsmq.deleteMessage({ qname: queue, id: msg.id }, function deleteResp(err) {
            if (err) {
                console.log('delete msg error', err);
            }

            cb(err);
        });
    }
}

function passQueue(queue) {
    return function passingQueue(cb) {
        cb(null, queue);
    };
}

function processMessages(queue, done) {
    async.timesSeries(20, function processXMessage(n, cb) {
        async.waterfall([
            passQueue(queue),
            getMessage,
            processMessage,
            deleteMessage
        ], cb);
    }, function fin(err) {
        if (err === 'empty') {
            done(null, false);
        } else if (err === 'fin') {
            done(null, true);
        } else {
            done(err, false);
        }
    });
}

function processQueue(queue, done) {
    var lock = redisLock.createLock(client, {
        timeout: 30 * 1000,
        retries: 0,
        delay: 100
    });

    async.waterfall([
        function lockQueue(cb) {
            lock.acquire(queue, function lockAcquire(err) {
                if (err) {
                    console.log('error acquire', queue, err);
                }

                cb(err ? 'acquire lock' : null);
            });
        },
        function process(cb) {
            processMessages(queue, cb);
        },
        function removeQueue(remove, cb) {
            if (remove) {
                console.log('removing queue', queue);
                rsmq.deleteQueue({ qname: queue }, function deleteResp(err) {
                    console.log('queue removed', queue, err);
                    cb(err);
                });
            } else {
                cb();
            }
        },
        function unlockQueue(cb) {
            lock.release(function lockRelease(err) {
                if (err) {
                    console.log('unlock queue err', queue, err);
                }

                cb(err);
            });
        }
    ], function fin(err) {
        var processedItem = err == null;
        if (err && err !== 'acquire lock') {
            done(err, false);
        } else {
            done(null, processedItem);
        }
    });
}

function processQueues(queues, cb) {
    async.mapSeries(queues, processQueue, function fin(err, lockErrors) {
        var processedItems = false;
        var x;

        if (err) {
            console.log('process err', err);
        } else {
            for (x = 0; x < lockErrors.length; x++) {
                if (lockErrors[x] === true) {
                    processedItems = true;
                    break;
                }
            }
        }

        cb(err, processedItems);
    });
}

function sleep(processedItems, cb) {
    setTimeout(cb, processedItems ? 0 : 1000);
}

async.forever(function iterate(cb) {
    async.waterfall([
        getQueues,
        processQueues,
        sleep
    ], cb);
}, function fin(err) {
    if (err) {
        console.log('finished', err);
    }

    client.quit();
    rsmq.quit();
});
