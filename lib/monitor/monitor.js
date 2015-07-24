/**
 * Component for monitor.
 * Load and start monitor client.
 */
var utils = require('../util/utils');
var zkClient = require('./zkClient');
var countDownLatch = require('../util/countDownLatch');
var logger = require('pomelo-logger').getLogger('pomelo', __filename);

var Monitor = function(app, opts) {
  opts = opts || {};
  this.app = app;
  this.serverInfo = app.getCurServer();
};

module.exports = Monitor;

Monitor.prototype.start = function(cb) {
	var self = this;

	var monitor = new zkClient(this.app, function() {
		logger.error('monitor: ' + self.app.serverId);
		getResults(self, monitor, watchServer.bind(self, monitor));
		utils.invokeCallback(cb);
	});
};

Monitor.prototype.stop = function(cb) {
  process.nextTick(function() {
    utils.invokeCallback(cb);
  });
};

var getServers = function(app, zk, servers) {
	var success = true;
	var results = {};
	if(!servers.length)	{
		logger.error('get servers data is null.');
		return;
	}
	var latch = countDownLatch.createCountDownLatch(servers.length, {timeout: 1000 * 60}, function() {
		if(!success) {
			logger.error('get all children data failed, with serverId: %s', app.serverId);
			return;
		}
		app.replaceServers(results);
	});
	for(var i = 0; i < servers.length; i++) {
		(function(index) {
			var serverFromConfig = app.getServerFromConfig(servers[index]);
			if(!!serverFromConfig) {
				results[serverFromConfig.id] = serverFromConfig;
				latch.done();
			}	else {
				zk.getData(zk.path + '/' + servers[index], function(err, data) {
					if(!!err)	{
						logger.error('%s get data failed for server %s, with err: %j', app.serverId, servers[index], err.stack);
						latch.done();
						success = false;
						return;
					}
					var serverInfo = JSON.parse(data);
					results[serverInfo.id] = serverInfo;
					latch.done();
				});
			}
		})(i);
	}
};

var watchServer = function(zookeeper) {
	var self = this;
	logger.error('watchServer: ' + self.app.serverId);
	logger.error('watchServer path: ' + zookeeper.nodePath);
	zookeeper.getChildren(watchServer.bind(self, zookeeper), function(err, children) {
		if(!!err)	{
			logger.error('get children failed when watch server, with err: %j', err.stack);
			return;
		}
		getServers(self.app, zookeeper, children);
	});
};

var getResults = function(self, zookeeper, func)	{
	zookeeper.getChildren(func, function(err, children) {
		if(!!err)	{
			logger.error('get results failed when watch server, with err: %j', err.stack);
			return;
		}
		getServers(self.app, zookeeper, children);
	});
};