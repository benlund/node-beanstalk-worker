var sys = require('sys');
var events = require('events');
var bt = require('beanstalk_client');

var BeanstalkWorker = function(id, server, handlers, logger) {
  process.EventEmitter.call(this);
  this.id = id;
  this.server = server;
  this.handlers = handlers;
  this.logger = logger || sys;

  this.stopped = false;
};

sys.inherits(BeanstalkWorker, events.EventEmitter);

BeanstalkWorker.prototype.start = function(options) {
  if(!options) options={};

  var self = this;
  self.log('Starting...');

  self.addListener('next', function() {
    self.handle_next();
  });

  bt.Client.connect(this.server).addCallback(function(conn) {
    self.connection = conn;

    self.watch_tubes(options.tubes, function() {

      var ignored = [];
      if(options.ignore_default) {
	ignored.push('default');
      }

      self.ignore_tubes(ignored, function() {

	self.log('Started');
	self.emit('next');

      });

    });
  }).addErrback(self.error_handler('connecting'));
};

BeanstalkWorker.prototype.watch_tubes = function(tubes, done) {
  var tube;
  var self = this;
  if(tubes && (tube = tubes[0])) {
    this.log('Watching tube ' + tube);
    this.connection.watch(tube).addCallback(function() {
      self.watch_tubes(tubes.slice(1), done);
    }).addErrback(this.error_handler('watching tube ' + tube));
  }
  else {
    done();
  }
};

BeanstalkWorker.prototype.ignore_tubes = function(tubes, done) {
  var tube;
  var self = this;
  if(tubes && (tube = tubes[0])) {
    this.log('Ignoring tube ' + tube);
    this.connection.ignore(tube).addCallback(function() {
      self.ignore_tubes(tubes.slice(1), done);
    }).addErrback(this.error_handler('ignoring tube ' + tube));
  }
  else {
    done();
  }
};

BeanstalkWorker.prototype.stop = function() {
  this.log('Stopping...');
  this.stopped = true;
};

BeanstalkWorker.prototype.handle_next = function() {
  if(this.stopped) {
    this.connection.close();
    this.log('Stopped');
    return;
  }

  var self = this;

  this.connection.reserve_with_timeout(5).addCallback(function(job_id, job_json) {
    var job, handler, start, duration;

    try {
      job = JSON.parse(job_json);
    }
    catch(e) {
      job = null;
      self.handle_error('parsing job JSON', [job_id, e.toString()]);
    }

    if(job) {
      handler = self.handlers[job.type];

      if(handler) {
	start = new Date().getTime();

	try {

	  handler(job.data, function() {
	    duration = new Date().getTime() - start;
	    self.log('Ran job : ' + job_id + ' in ' + duration + 'ms (' + job_json + ')');
	    self.destroy_and_emit_next(job_id);
	  });

	}
	catch(ex) {
	  self.log('Exception junning job : ' + job_id + ' : ' + ex.toString());
	  self.bury_and_emit_next(job_id);
	}

      }

      else {
	self.handle_error('handling job', [job_id, 'no handler for ' + JSON.stringify(job)]);
	self.bury_and_emit_next(job_id);
      }

    }
    else {
      self.bury_and_emit_next(job_id);
    }
  }).addErrback(function(reason) {
    if('TIMED_OUT' === reason) {
      self.emit('next');
    }
    else {
      self.handle_error('reserving job');
    }
  });

};

BeanstalkWorker.prototype.bury_and_emit_next = function(job_id) {
  var self = this;
  self.connection.bury(job_id, bt.Client.LOWEST_PRIORITY).addCallback(function() {
    self.log('Buried ' + job_id);
    self.emit('next');
  }).addErrback(function(reason) {
    self.handle_error('burying ', [job_id, reason]);
    self.emit('next');
  });
};

BeanstalkWorker.prototype.destroy_and_emit_next = function(job_id) {
  var self = this;
  self.connection.destroy(job_id).addCallback(function() {
    self.emit('next');
  }).addErrback(function(reason) {
    self.handle_error('destroying ', [job_id, reason]);
    self.emit('next');
  });
};

BeanstalkWorker.prototype.error_handler = function(type) {
  var self = this;
  return function() {
    self.handle_error(type, Array.prototype.slice.call(arguments));
  };
}

BeanstalkWorker.prototype.handle_error = function(type, args) {
  if(!args) {
    args = [];
  }
  this.log('Error ' + type + ' : ' + args.join(' '));
};

BeanstalkWorker.prototype.log = function(message) {
  this.logger.puts('[ ' + new Date().toString() + ' ] [ ' + process.pid + ' (' + this.id + ') ] : ' + message);
};

exports.BeanstalkWorker = BeanstalkWorker;
