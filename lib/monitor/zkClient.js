var async = require('async');
var crypto = require('crypto');
var zookeeper = require("node-zookeeper-client");
var CreateMode = zookeeper.CreateMode;
var constants = require('../util/constants');
var utils = require('../util/utils');
var logger = require('pomelo-logger').getLogger('pomelo', __filename);

function ZKClient(app, cb) {
  var self = this;

  this.app = app;
  zkConfig = app.zookeeper;
  this.path = zkConfig.path || '/pomelo-cluster';
  this.username = zkConfig.username || 'pomelo';
  this.password = zkConfig.password || 'pomelo';
  this.timeout = zkConfig.timeout || 15000;
  this.setACL = zkConfig.setACL;
  this.retries = zkConfig.retries || 3;
  this.spinDelay = zkConfig.spinDelay || 1000;
  this.reconnectTimes = zkConfig.reconnectTimes || 3;
  this.nodePath = this.path + '/' + app.serverId;
  this.authentication = this.username + ':' + this.password;
  var shaDigest = crypto.createHash('sha1').update(this.authentication).digest('base64');
  this.acls = [
  new zookeeper.ACL(
    zookeeper.Permission.ALL,
    new zookeeper.Id('digest', this.username + ':' + shaDigest)
    )
  ];

  this.client = zookeeper.createClient(zkConfig.servers, {sessionTimeout: this.timeout, retries: this.retries, spinDelay: this.spinDelay});

  var cbTimer;
  if(!!cb) {
    cbTimer = setTimeout(function() {
      utils.invokeCallback(cb, new Error('cannot connect to zookeeper.'));
    }, 1000 * 15);
  }

  var clearTimer = function(err) {
    if(!!cbTimer) {
      clearTimeout(cbTimer);
      if(!!err) {
        throw err;
      } else {
        self.registerZK(cb);
      }
    }
  };

  this.client.once('connected', function() {
    self.client.addAuthInfo('digest', new Buffer(self.authentication));
    if(self.setACL) {
      self.client.setACL(self.path, self.acls, -1, function(err, stat) {
        if(!!err) {
          logger.error('failed to set ACL: %j', err.stack);
          clearTimer(err);
          return;
        }
        clearTimer();
        logger.info('ACL is set to: %j', self.acls);
      });
    } else {
      clearTimer();
    }
  });

  this.client.on('disconnected', function() {
    logger.error('%s disconnect with zookeeper server.', self.app.serverId);
    self.reconnect();
  });

  this.client.connect();
};

module.exports = ZKClient;

ZKClient.prototype.close = function() {
  this.client.close();
  this.client.removeAllListeners();
  this.client = null;
};

ZKClient.prototype.registerZK = function(cb) {
  var serverInfo = this.app.getCurServer();
  serverInfo.pid = process.pid;
  var buffer = new Buffer(JSON.stringify(serverInfo));
  this.createNode(this.nodePath, buffer, CreateMode.EPHEMERAL, function(err, path) {
    if(!!err) {
      logger.error('create server node %s failed, with err : %j ', this.nodePath, err.stack);
      utils.invokeCallback(cb, err);
      return;
    }
    utils.invokeCallback(cb);
  });
};

ZKClient.prototype.createNode = function(path, value, mode, cb) {
  var self = this;
  self.client.exists(path, function(err, stat) {
    if(!!err) {
      utils.invokeCallback(cb, err);
      return;
    }
    if(!stat) {
      self.client.create(path, value, mode, function(err, result) {
        utils.invokeCallback(cb, err, result);
        return;
      });
    } else {
      utils.invokeCallback(cb);
      return;
    }
  });
};

ZKClient.prototype.getData = function(path, cb) {
  this.client.getData(path, function(err, data) {
    if(!!err) {
      utils.invokeCallback(cb, err);
      return;
    }
    utils.invokeCallback(cb, null, data.toString());
  });
};

ZKClient.prototype.getChildren = function(fun, cb) {
  var self = this;
  this.client.getChildren(this.path, fun, function(err, children, stats) {
    if(!!err) {
      utils.invokeCallback(cb, err);
      return;
    }
    utils.invokeCallback(cb, null, children);
  });
};

ZKClient.prototype.reconnect = function() {
  var self = this;
  var count = 0;
  var retry = true;
  var retries = this.reconnectTimes;
  async.whilst(
    function () {
      return count <= retries && retry;
    },
    function (next) {
      count += 1;
      self.connectZK(function(err) {
        if(!!err) {
          setTimeout(
            next,
            count * 1000 * 5
          );
        } else {
          self.registerZK(function(err) {
            if(!!err) {
              setTimeout(
                next,
                count * 1000 * 5
                );
            } else {
              retry = false;
            }
          });
        }
      });
    },
    function (error) {

    }
  );
};