var BeanstalkWorkerCluster = require('../lib/beanstalk_worker_cluster').BeanstalkWorkerCluster;

var handlers = {};
process.mixin(handlers, require('../handlers/test'), require('../handlers/http_request'));

process.addListener('SIGINT', function() {
  BeanstalkWorkerCluster.stop();
});

process.addListener('TERM', function() {
  BeanstalkWorkerCluster.stop();
});

var worker_options = {
  tubes: ['external'],
  ignore_default: true
};

BeanstalkWorkerCluster.start(3, '127.0.0.1:11300', handlers, worker_options);

//hot stopping/starting of workers

//daemonize

//logging to file

//leaks memory heavily!!!

// var sys = require('sys');
// function log_mem() {
//   sys.puts(process.memoryUsage().rss / 1024);
// }

// var i = setInterval(log_mem, 1000);

// process.addListener('SIGINT', function() {
// 		      clearInterval(i);
// 		      });