var BeanstalkWorkerCluster = require('../lib/beanstalk_worker_cluster').BeanstalkWorkerCluster;

var handlers = {};
process.mixin(handlers, require('../handlers/test'), require('../handlers/httprequest'));

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

//leaks memory heavily!!!

//logging to file
