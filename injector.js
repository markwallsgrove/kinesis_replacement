#!/usr/bin/nodejs

var async = require('async');
var RedisSMQ = require('rsmq');
var rsmq = new RedisSMQ({ host: '127.0.0.1', port: 6379, ns: 'rsmq' });

var client = require('redis').createClient();
var lock = require('redislock').createLock(client, {
    timeout: 20000,
    retries: 3,
    delay: 100
});

var now = (new Date()).getTime();
var queue = 'myqueue_' + now;
var itemsToCreate = Math.random() * 100;
var payload = {
    qname: queue,
    message: 'hey'
};

function lockQueue(cb) {
    lock.acquire(queue, function lockAcquire(err) {
        if (err) {
            console.log('error acquire', queue, err);
        } else {
            console.log('locked queue', queue);
        }

        cb(err ? 'acquire lock' : null);
    });
}

function createQueue(cb) {
    rsmq.createQueue({ qname: queue }, function createResp(err) {
        console.log('queue created', queue, err);
        cb(err);
    });
}

function createItem(n, cb) {
    rsmq.sendMessage(payload, function sentMsg(err) {
        console.log('sent message', n, err);
        cb(err);
    });
}

function createItems(cb) {
    async.times(itemsToCreate, createItem, function fin(err) {
        console.log('created items', err);
        cb(err);
    });
}

function unlockQueue(cb) {
    lock.release(function lockRelease(err) {
        if (err) {
            console.log('unlock queue err', queue, err);
        } else {
            console.log('unlocked queue', queue);
        }

        cb(err);
    });
}

async.waterfall([
    lockQueue,
    createQueue,
    createItems,
    unlockQueue
], function fin(err) {
    console.log('fin', err);
    rsmq.quit();
    client.quit();
});
