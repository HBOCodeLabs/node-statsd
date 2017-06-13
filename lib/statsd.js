var dgram = require('dgram'),
    dns   = require('dns');

/**
 * The UDP Client for StatsD
 * @param options
 *   @option host        {String}  The host to connect to default: localhost
 *   @option port        {String|Integer} The port to connect to default: 8125
 *   @option prefix      {String}  An optional prefix to assign to each stat name sent
 *   @option suffix      {String}  An optional suffix to assign to each stat name sent
 *   @option globalize   {boolean} An optional boolean to add "statsd" as an object in the global namespace
 *   @option cacheDns    {boolean} An optional option to cache dns result and refresh periodically.
 *   @option mock        {boolean} An optional boolean indicating this Client is a mock object, no stats are sent.
 *   @option global_tags {Array=} Optional tags that will be added to every metric
 *   @maxBufferSize      {Number} An optional value for aggregating metrics to send, mainly for performance improvement
 *   @bufferFlushInterval {Number} the time out value to flush out buffer if not
 *   @option socketRefreshInterval {Number} The maximum amount of time to use one second in milliseconds (default 1 minute)
 * @constructor
 */
var Client = function (host, port, prefix, suffix, globalize, cacheDns, mock, global_tags, maxBufferSize, bufferFlushInterval, socketRefreshInterval) {
    var options = host || {},
        self = this;

    if(arguments.length > 1 || typeof(host) === 'string'){
        options = {
            host        : host,
            port        : port,
            prefix      : prefix,
            suffix      : suffix,
            globalize   : globalize,
            cacheDns    : cacheDns,
            mock        : mock === true,
            global_tags : global_tags,
            maxBufferSize : maxBufferSize,
            bufferFlushInterval: bufferFlushInterval,
            socketRefreshInterval: socketRefreshInterval
        };
    }

    this.host        = options.host || 'localhost';
    this.port        = options.port || 8125;
    this.prefix      = options.prefix || '';
    this.suffix      = options.suffix || '';
    this.socket      = dgram.createSocket('udp4');
    this.socketCreateTime = new Date();
    this.mock        = options.mock;
    this.global_tags = options.global_tags || [];
    this.maxBufferSize = options.maxBufferSize || 0;
    this.bufferFlushInterval = options.bufferFlushInterval || 1000;
    this.buffer = "";
    this.socketRefreshInterval = options.socketRefreshInterval || 60000; // 1 minute

    if(this.maxBufferSize > 0) {
        this.intervalHandle = setInterval(this.timeoutCallback.bind(this), this.bufferFlushInterval);
    }

    this.cacheDns = options.cacheDns;
    // a default like /dev/null to send the metrics to. So the process won't crash even the name resolution fails.
    this.resolvedAddress = "127.0.0.1";
    if(options.cacheDns === true){
        this.refreshDns();
    }

    if(options.globalize){
        global.statsd = this;
    }
};

/**
 * Represents the timing stat
 * @param stat {String|Array} The stat(s) to send
 * @param time {Number} The time in milliseconds to send
 * @param sampleRate {Number=} The Number of times to sample (0 to 1). Optional.
 * @param tags {Array=} The Array of tags to add to metrics. Optional.
 * @param callback {Function=} Callback when message is done being delivered. Optional.
 */
Client.prototype.timing = function (stat, time, sampleRate, tags, callback) {
    this.sendAll(stat, time, 'ms', sampleRate, tags, callback);
};

/**
 * Increments a stat by a specified amount
 * @param stat {String|Array} The stat(s) to send
 * @param value The value to send
 * @param sampleRate {Number=} The Number of times to sample (0 to 1). Optional.
 * @param tags {Array=} The Array of tags to add to metrics. Optional.
 * @param callback {Function=} Callback when message is done being delivered. Optional.
 */
Client.prototype.increment = function (stat, value, sampleRate, tags, callback) {
    this.sendAll(stat, value || 1, 'c', sampleRate, tags, callback);
};

/**
 * Decrements a stat by a specified amount
 * @param stat {String|Array} The stat(s) to send
 * @param value The value to send
 * @param sampleRate {Number=} The Number of times to sample (0 to 1). Optional.
 * @param tags {Array=} The Array of tags to add to metrics. Optional.
 * @param callback {Function=} Callback when message is done being delivered. Optional.
 */
Client.prototype.decrement = function (stat, value, sampleRate, tags, callback) {
    this.sendAll(stat, -value || -1, 'c', sampleRate, tags, callback);
};

/**
 * Represents the histogram stat
 * @param stat {String|Array} The stat(s) to send
 * @param value The value to send
 * @param sampleRate {Number=} The Number of times to sample (0 to 1). Optional.
 * @param tags {Array=} The Array of tags to add to metrics. Optional.
 * @param callback {Function=} Callback when message is done being delivered. Optional.
 */
Client.prototype.histogram = function (stat, value, sampleRate, tags, callback) {
    this.sendAll(stat, value, 'h', sampleRate, tags, callback);
};


/**
 * Gauges a stat by a specified amount
 * @param stat {String|Array} The stat(s) to send
 * @param value The value to send
 * @param sampleRate {Number=} The Number of times to sample (0 to 1). Optional.
 * @param tags {Array=} The Array of tags to add to metrics. Optional.
 * @param callback {Function=} Callback when message is done being delivered. Optional.
 */
Client.prototype.gauge = function (stat, value, sampleRate, tags, callback) {
    this.sendAll(stat, value, 'g', sampleRate, tags, callback);
};

/**
 * Counts unique values by a specified amount
 * @param stat {String|Array} The stat(s) to send
 * @param value The value to send
 * @param sampleRate {Number=} The Number of times to sample (0 to 1). Optional.
 * @param tags {Array=} The Array of tags to add to metrics. Optional.
 * @param callback {Function=} Callback when message is done being delivered. Optional.
 */
Client.prototype.unique =
    Client.prototype.set = function (stat, value, sampleRate, tags, callback) {
        this.sendAll(stat, value, 's', sampleRate, tags, callback);
    };

/**
 * Checks if stats is an array and sends all stats calling back once all have sent
 * @param stat {String|Array} The stat(s) to send
 * @param value The value to send
 * @param sampleRate {Number=} The Number of times to sample (0 to 1). Optional.
 * @param tags {Array=} The Array of tags to add to metrics. Optional.
 * @param callback {Function=} Callback when message is done being delivered. Optional.
 */
Client.prototype.sendAll = function(stat, value, type, sampleRate, tags, callback){
    var completed = 0,
    calledback = false,
    sentBytes = 0,
    self = this;

    if(sampleRate && typeof sampleRate !== 'number'){
        callback = tags;
        tags = sampleRate;
        sampleRate = undefined;
    }

    if(tags && !Array.isArray(tags)){
        callback = tags;
        tags = undefined;
    }

    /**
     * Gets called once for each callback, when all callbacks return we will
     * call back from the function
     * @private
     */
    function onSend(error, bytes){
        completed += 1;
        if(calledback || typeof callback !== 'function'){
            return;
        }

        if(error){
            calledback = true;
            return callback(error);
        }

        sentBytes += bytes;
        if(completed === stat.length){
            callback(null, sentBytes);
        }
    }

    if(Array.isArray(stat)){
        stat.forEach(function(item){
            self.send(item, value, type, sampleRate, tags, onSend);
        });
    } else {
        this.send(stat, value, type, sampleRate, tags, callback);
    }
};

/**
 * Sends a stat across the wire
 * @param stat {String|Array} The stat(s) to send
 * @param value The value to send
 * @param type {String} The type of message to send to statsd
 * @param sampleRate {Number} The Number of times to sample (0 to 1)
 * @param tags {Array} The Array of tags to add to metrics
 * @param callback {Function=} Callback when message is done being delivered. Optional.
 */
Client.prototype.send = function (stat, value, type, sampleRate, tags, callback) {
    var message = this.prefix + stat + this.suffix + ':' + value + '|' + type,
        merged_tags = [];

    if(sampleRate && sampleRate < 1){
        if(Math.random() < sampleRate){
            message += '|@' + sampleRate;
        } else {
            //don't want to send if we don't meet the sample ratio
            return;
        }
    }

    if(tags && Array.isArray(tags)){
        merged_tags = merged_tags.concat(tags);
    }
    if(this.global_tags && Array.isArray(this.global_tags)){
        merged_tags = merged_tags.concat(this.global_tags);
    }
    if(merged_tags.length > 0){
        message += '|#' + merged_tags.join(',');
    }

    // Only send this stat if we're not a mock Client.
    if(!this.mock) {
        if(this.maxBufferSize === 0) {
            this.sendMessage(message, callback);
        }
        else {
            this.enqueue(message);
        }
    }
    else {
        if(typeof callback === 'function'){
            callback(null, 0);
        }
    }
};

/**
 *
 * @param message {String}
 */
Client.prototype.enqueue = function(message){
    this.buffer += message + "\n";
    if(this.buffer.length >= this.maxBufferSize) {
        this.flushQueue();
    }
}

/**
 *
 */
Client.prototype.flushQueue = function(){
    this.sendMessage(this.buffer);
    this.buffer = "";
}


/**
 * Close the old socket and create a new one, when desired
 */
Client.prototype.refreshSocketAndDns = function(){
    var now = new Date();
    if ((now - this.socketCreateTime) >= this.socketRefreshInterval) {
        var newSocket = dgram.createSocket('udp4');
        var oldSocket = this.socket;
        this.socket = newSocket;
        this.socketCreateTime = now;

        // There is a small window where there may still be unsent data on the old socket
        // (even 'tho socket.send has already been called).  Closing it in this case
        // would prevcent that data from being sent.  For that reason, we don't close it
        // immediately.  Instead, we Close the old socket after a short delay.
        setTimeout(oldSocket.close.bind(oldSocket), this.socketRefreshInterval);

        // if cacheDns is enabled, time to refresh the dns name.
        if (this.cacheDns) {
            this.refreshDns();
        }
    }
}

/**
 * refresh the dns resolution periodically. This is useful when k8s moves the statsd pods. No
 * more need to restart the service.
 */
Client.prototype.refreshDns = function(){
    var self = this;
    dns.lookup(this.host, function(err, address, family){
        if(err == null){
            self.resolvedAddress = address;
        }
    });
}
/**
 *
 * @param message {String}
 * @param callback {Function}
 */
Client.prototype.sendMessage = function(message, callback){
    var buf = new Buffer(message);
    this.refreshSocketAndDns();

    var address = this.cacheDns ? this.resolvedAddress : this.host;
    this.socket.send(buf, 0, buf.length, this.port, address, callback);
}

/**
 *
 */
Client.prototype.timeoutCallback = function(){
    if(this.buffer !== "") {
        this.flushQueue();
    }
}

/**
 * Close the underlying socket and stop listening for data on it.
 */
Client.prototype.close = function(){
    if(this.intervalHandle) {
        clearInterval(this.intervalHandle);
    }
    this.socket.close();
}

exports = module.exports = Client;
exports.StatsD = Client;

