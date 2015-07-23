/**
 * Component for monitor.
 * Load and start monitor client.
 */
var logger = require('pomelo-logger').getLogger('pomelo', __filename);
var utils = require('../util/utils');
var Constants = require('../util/constants');

var Monitor = function(app, opts) {
  opts = opts || {};
  this.app = app;
  this.serverInfo = app.getCurServer();

};

module.exports = Monitor;

Monitor.prototype.start = function(cb) {

  process.nextTick(function() {
    utils.invokeCallback(cb);
  });
};


Monitor.prototype.stop = function(cb) {

  process.nextTick(function() {
    utils.invokeCallback(cb);
  });
};