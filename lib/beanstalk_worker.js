(function() {
  var BeanstalkWorker, client, events,
    __hasProp = Object.prototype.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor; child.__super__ = parent.prototype; return child; };

  events = require('events');

  client = require('beanstalk_client').Client;

  BeanstalkWorker = (function(_super) {

    __extends(BeanstalkWorker, _super);

    function BeanstalkWorker(id, server, handlers, logger) {
      this.id = id;
      this.server = server;
      this.handlers = handlers;
      this.logger = logger;
      this.logger || (this.logger = console);
      this.stopped = false;
    }

    BeanstalkWorker.prototype.start = function(tubes, ignore_default) {
      var _this = this;
      this.log('Starting...');
      this.on('next', function() {
        return _this.handle_next();
      });
      return client.connect(this.server, function(err, conn) {
        if (err) {
          return _this.log('Error connecting: ' + err);
        } else {
          _this.connection = conn;
          return _this.watch_tubes(tubes, function() {
            var ignored;
            ignored = [];
            if (ignore_default) ignored.push('default');
            return _this.ignore_tubes(ignored, function() {
              _this.log('Started');
              return _this.emit('next');
            });
          });
        }
      });
    };

    BeanstalkWorker.prototype.watch_tubes = function(tubes, done) {
      var tube,
        _this = this;
      if (tubes && (tube = tubes[0])) {
        this.log('Watching tube ' + tube);
        return this.connection.watch(tube, function(err) {
          if (err) _this.log('Error watching tube : ' + tube);
          return _this.watch_tubes(tubes.slice(1), done);
        });
      } else {
        return done();
      }
    };

    BeanstalkWorker.prototype.ignore_tubes = function(tubes, done) {
      var tube,
        _this = this;
      if (tubes && (tube = tubes[0])) {
        this.log('Ignoring tube ' + tube);
        return this.connection.ignore(tube, function(err) {
          if (err) _this.log('Error ignoring tube : ' + tube);
          return _this.ignore_tubes(tubes.slice(1), done);
        });
      } else {
        return done();
      }
    };

    BeanstalkWorker.prototype.stop = function() {
      this.log('Stopping...');
      return this.stopped = true;
    };

    BeanstalkWorker.prototype.find_handler = function(job_type) {
      var handler, _i, _len, _ref;
      _ref = this.handlers;
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        handler = _ref[_i];
        if (handler[job_type]) return handler[job_type];
      }
      return null;
    };

    BeanstalkWorker.prototype.handle_next = function() {
      var _this = this;
      if (this.stopped) {
        this.connection.end();
        this.log('Stopped');
        return;
      }
      return this.connection.reserve_with_timeout(5, function(err, job_id, job_json) {
        if (err) {
          if ('TIMED_OUT' === err) {
            return _this.emit('next');
          } else {
            return _this.log('Error reserving job : ' + err.toString());
          }
        } else {
          ({
            job: null
          });
          try {
            ({
              job: JSON.parse(job_json)
            });
          } catch (e) {
            _this.log('Error parsing job JSON : ' + job_id + ' : ' + e.toString());
          }
          if (typeof job !== "undefined" && job !== null) {
            return _this.handle_job(job_id, job);
          } else {
            _this.log("Error handling job : " + job_id + " : couldn't parse job : " + job_json);
            return _this.bury_and_emit_next(job_id);
          }
        }
      });
    };

    BeanstalkWorker.prototype.handle_job = function(job_id, job) {
      ({
        handler: this.find_handler(job.type)
      });
      if (typeof handler !== "undefined" && handler !== null) {
        return this.run_handler_on_job_data(handler, job_id, job.data);
      } else {
        this.log("Error handling job : " + job_id + " : no handler for " + (JSON.stringify(job)));
        return this.bury_and_emit_next(job_id);
      }
    };

    BeanstalkWorker.prototype.run_handler_on_job_data = function(handler, job_id, job_data) {
      var job_canceled, start,
        _this = this;
      start = new Date().getTime();
      try {
        job_canceled = false;
        return handler(job_data, function(action, data) {
          var duration, reason;
          if (!job_canceled) {
            duration = new Date().getTime() - start;
            if (!(action != null) || ('next' === action)) {
              _this.log("Ran job : " + job_id + " in " + duration + " ms (" + (JSON.stringify(job_data)) + ")");
              return _this.destroy_and_emit_next(job_id);
            } else if ('release' === action) {
              job_canceled = true;
              _this.log("Released job : " + job_id + " after " + duration + " ms)");
              return _this.release_and_emit_next(job_id, data);
            } else if ('bury' === action) {
              _this.log('Buried job : ' + job_id);
              return _this.bury_and_emit_next(job_id);
            } else {
              reason = action;
              _this.log("Failed to run job : " + job_id + " : " + reason);
              return _this.bury_and_emit_next(job_id);
            }
          }
        });
      } catch (e) {
        this.log("Exception running job : " + job_id + " : " + (e.toString()));
        return this.bury_and_emit_next(job_id);
      }
    };

    BeanstalkWorker.prototype.bury_and_emit_next = function(job_id) {
      var _this = this;
      return this.connection.bury(job_id, client.LOWEST_PRIORITY, function(err) {
        if (err) {
          _this.log("Error burying job : " + job_id + " : " + (err.toString()));
        }
        return _this.emit('next');
      });
    };

    BeanstalkWorker.prototype.release_and_emit_next = function(job_id, delay) {
      var _this = this;
      if (!(delay != null)) delay = 30;
      return this.connection.release(job_id, client.LOWEST_PRIORITY, delay, function(err) {
        if (err) {
          _this.log("Error releasing job : " + job_id + " : " + (err.toString()));
        }
        return _this.emit('next');
      });
    };

    BeanstalkWorker.prototype.destroy_and_emit_next = function(job_id) {
      var _this = this;
      return this.connection.destroy(job_id, function(err) {
        if (err) {
          _this.log("Error destroying job : " + job_id + " : " + (err.toString()));
        }
        return _this.emit('next');
      });
    };

    BeanstalkWorker.prototype.log = function(message) {
      return this.logger.log("[ " + (new Date().toString()) + " ] [ " + process.pid + " ( " + this.id + " ) ] : " + message);
    };

    return BeanstalkWorker;

  })(events.EventEmitter);

  exports.BeanstalkWorker = BeanstalkWorker;

}).call(this);
