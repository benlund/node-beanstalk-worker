(function(){
  var client, sys;
  client = require('beanstalk_client').Client;
  sys = require('sys');
  //producer
  client.connect(null, function(err, conn) {
    if (err) {
      sys.puts('Producer connection error:', sys, inspect(err));
    } else {

    }
    sys.puts('Producer connected');
    return conn.use('test', function() {
      var send_job;
      send_job = function() {
        var job_data;
        job_data = {
          type: 'http_request',
          data: {
            method: 'POST',
            url: 'http://127.0.0.1:9876/search?q=blah',
            headers: {},
            body: 'bodystring'
          }
        };
        return conn.put(0, 0, 1, JSON.stringify(job_data), function(err, job_id) {
          sys.puts('Producer sent job: ' + job_id);
          return setTimeout(send_job, 1000);
        });
      };
      return send_job();
    });
  });
})();
