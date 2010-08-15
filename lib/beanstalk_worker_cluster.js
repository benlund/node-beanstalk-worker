var BeanstalkWorker = require('./beanstalk_worker').BeanstalkWorker;

var BeanstalkWorkerCluster = {

  workers: [],

  start: function(server, num_workers, handlers, tubes, ignore_default) {

    for(var i= 0; i< num_workers; i++) {
      var w = new BeanstalkWorker(i, server, handlers);
      this.workers.push(w);
      w.start(tubes, ignore_default);
    }

  },

  stop: function() {
    for(var i= 0; i< this.workers.length; i++) {
      this.workers[i].stop();
    }
  }

};

exports.BeanstalkWorkerCluster = BeanstalkWorkerCluster;


/* Todo

 - stagger the initialization so they don't all connect/timeout at the same time

*/
