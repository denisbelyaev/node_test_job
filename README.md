# node_test_job

var redis = require("redis");
var client = redis.createClient();
var pid = 0;

(process.argv[2] == 'getErrors') ? getErrorMessages() : init();

function init() {
    client.incr("PID", function (err, res) {
        if (err) closeClient(err);

        pid = res;

        client.setnx('generator', pid, function(err, res) {
            res ? initGenerator() : subscribe();
        });
    });
}

function initGenerator() {
    console.log("init generator: " + pid);

    var generator = setInterval(function () {
        client.pexpire('generator', 600);

        var message = getMessage();

        console.log("publihed message: " + message);
        publish(message);
    }, 500);
    //    if (message >= 1000000) {
    //        clearInterval(generator);
    //        //process.exit();
    //    }
    //}, 0);

//    generator.unref();
}

function subscribe() {
    var listen = setInterval(function () {
        client.setnx('generator', pid, function(err, res) {
            if (err) closeClient(err);

            if (!res) {
                client.sadd('listeners', pid, function (err) {
                    if (err) closeClient(err);
                    console.log("add listener: " + pid);
                });
            } else {
                client.srem('listeners', pid, function (err) {
                    if (err) closeClient(err);
                });
                clearInterval(listen);
                initGenerator();
            }
        })
    }, 0);
}

function publish(message) {
    client.smembers('listeners', function (err, listeners) {
        if (err) closeClient(err);

        listeners.forEach(function (listener) {
            eventHandler(message, function (err, message) {
                if (err) {
                    console.log("listener " + listener + " error message: " + message);
                    client.sadd('error_messages', message, function (err) {
                        if (err) closeClient(err);
                    });
                } else {
                    console.log("listener " + listener + " received message: " + message);
                }
            })
        })
    })

    client.del('listeners', function (err) {
        if (err) closeClient(err);
    });
}

function getMessage() {
    this.cnt = this.cnt || 0;
    return this.cnt++;
}

function eventHandler(msg, callback) {
    function onComplete() {
        var error = Math.random() > 0.85;
        callback(error, msg);
    }

    // processing takes time...
    setTimeout(onComplete, Math.floor(Math.random()*1000));
}

function getErrorMessages() {
    client.smembers('error_messages', function(err, messages) {
        if (err) closeClient(err);

        console.log('error_messages: ' + messages);

        client.del('error_messages', function(err) {
            if (err) closeClient(err);
        });
        client.end(false);
    });
}

function closeClient(err) {
    console.error(err);
    client.end(false);
}
