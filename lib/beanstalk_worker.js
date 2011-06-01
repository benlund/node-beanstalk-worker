(function() {
  var BeanstalkWorker, client, events;
  var __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  }, __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  events = require('events');
  client = require('beanstalk_client').Client;
  BeanstalkWorker = (function() {
    __extends(BeanstalkWorker, events.EventEmitter);
    function BeanstalkWorker(id, server, handlers, logger) {
      this.id = id;
      this.server = server;
      this.handlers = handlers;
      this.logger = logger || console;
      this.stopped = false;
    }
    BeanstalkWorker.prototype.start = function(tubes, ignore_default) {
      this.log('Starting...');
      this.on('next', __bind(function() {
        return this.handle_next();
      }, this));
      return client.connect(this.server, __bind(function(err, conn) {
        if (err) {
          return this.log('Error connecting: ' + err);
        } else {
          this.connection = conn;
          return this.watch_tubes(tubes, __bind(function() {
            var ignored;
            ignored = [];
            if (ignore_default) {
              ignored.push('default');
            }
            return this.ignore_tubes(ignored, __bind(function() {
              this.log('Started');
              return this.emit('next');
            }, this));
          }, this));
        }
      }, this));
    };
    BeanstalkWorker.prototype.watch_tubes = function(tubes, done) {
      var tube;
      if ((tubes != null) && (tube = tubes[0])) {
        this.log('Watching tube ' + tube);
        return this.connection.watch(tube, __bind(function(err) {
          if (err) {
            this.log('Error watching tube : ' + tube);
          }
          return this.watch_tubes(tubes.slice(1), done);
        }, this));
      } else {
        return done();
      }
    };
    BeanstalkWorker.prototype.ignore_tubes = function(tubes, done) {
      var tube;
      if ((tubes != null) && (tube = tubes[0])) {
        this.log('Ignoring tube ' + tube);
        return this.connection.ignore(tube, __bind(function(err) {
          if (err) {
            this.log('Error ignoring tube : ' + tube);
          }
          return this.ignore_tubes(tubes.slice(1), done);
        }, this));
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
        if (handler[job_type]) {
          return handler[job_type];
        }
      }
      return null;
    };
    BeanstalkWorker.prototype.handle_next = function() {
      if (this.stopped) {
        this.connection.end();
        this.log('Stopped');
        return;
      }
      return this.connection.reserve_with_timeout(5, __bind(function(err, job_id, job_json) {
        var job;
        if (err) {
          if ('TIMED_OUT' === err) {
            return this.emit('next');
          } else {
            return this.log('Error reserving job : ' + err.toString());
          }
        } else {
          try {
            job = JSON.parse(job_json);
          } catch (e) {
            job = null;
            this.log('Error parsing job JSON : ' + job_id + ' : ' + e.toString());
          }
          if (job != null) {
            return this.handle_job(job_id, job);
          } else {
            this.log('Error handling job : ' + job_id + ' : couldn\'t parse job : ' + job_json);
            return this.bury_and_emit_next(job_id);
          }
        }
      }, this));
    };
    BeanstalkWorker.prototype.handle_job = function(job_id, job) {
      var handler;
      handler = this.find_handler(job.type);
      if (handler != null) {
        return this.run_handler_on_job_data(handler, job_id, job.data);
      } else {
        this.log('Error handling job : ' + job_id + ' : no handler for ' + JSON.stringify(job));
        return this.bury_and_emit_next(job_id);
      }
    };
    BeanstalkWorker.prototype.run_handler_on_job_data = function(handler, job_id, job_data) {
      var job_canceled, start;
      start = new Date().getTime();
      try {
        job_canceled = false;
        return handler(job_data, __bind(function(action, data) {
          var duration;
          if (!job_canceled) {
            duration = new Date().getTime() - start;
            if (!(action != null) || ('next' === action)) {
              this.log('Ran job : ' + job_id + ' in ' + duration + 'ms (' + JSON.stringify(job_data) + ')');
              return this.destroy_and_emit_next(job_id);
            } else if ('release' === action) {
              job_canceled = true;
              this.log('Released job : ' + job_id + ' after ' + duration + 'ms');
              return this.release_and_emit_next(job_id, data);
            } else if ('bury' === action) {
              this.log('Buried job : ' + job_id);
              return this.bury_and_emit_next(job_id);
            } else {
              return this.log('Failed to run job : ' + job_id + ' : ' + reason);
            }
          }
        }, this));
      } catch (ex) {
        this.log('Exception running job : ' + job_id + ' : ' + ex.toString());
        return this.bury_and_emit_next(job_id);
      }
    };
    BeanstalkWorker.prototype.bury_and_emit_next = function(job_id) {
      return this.connection.bury(job_id, client.LOWEST_PRIORITY, __bind(function(err) {
        if (err) {
          this.log('Error burying job : ' + job_id + ' : ' + err.toString());
        }
        return this.emit('next');
      }, this));
    };
    BeanstalkWorker.prototype.release_and_emit_next = function(job_id, delay) {
      if (!(delay != null)) {
        delay = 30;
      }
      return this.connection.release(job_id, client.LOWEST_PRIORITY, delay, __bind(function(err) {
        if (err) {
          this.log('Error releasing job : ' + job_id + ' : ' + err.toString());
        }
        return this.emit('next');
      }, this));
    };
    BeanstalkWorker.prototype.destroy_and_emit_next = function(job_id) {
      return this.connection.destroy(job_id, __bind(function(err) {
        if (err) {
          this.log('Error destroying job : ' + job_id + ' : ' + err.toString());
        }
        return this.emit('next');
      }, this));
    };
    BeanstalkWorker.prototype.log = function(message) {
      return this.logger.log('[ ' + new Date().toString() + ' ] [ ' + process.pid + ' (' + this.id + ') ] : ' + message);
    };
    return BeanstalkWorker;
  })();
  exports.BeanstalkWorker = BeanstalkWorker;
}).call(this);
