var BeanstalkWorkerCluster = require('../lib/beanstalk_worker_cluster').BeanstalkWorkerCluster;

process.on('SIGINT', function() {
  BeanstalkWorkerCluster.stop();
});

process.on('TERM', function() {
  BeanstalkWorkerCluster.stop();
});

var worker_options = {
  tubes: ['test'],
  ignore_default: true
};

BeanstalkWorkerCluster.start(3, '127.0.0.1:11300', [require('../handlers/test').handlers, require('../handlers/http_request').handlers], worker_options);


/* Todo:

 - hot stopping/starting of workers

 - daemonize properly

 - logging to file

*/
