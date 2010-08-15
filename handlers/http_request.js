var http = require('http');
var url = require('url');

exports.handlers = {

  //data looks like {method: 'GET'|'POST', url: 'http://www.example.com/search?q=blah', headers: {'Extra-header': value}, body: 'bodystring'}
  http_request: function(data, done) {
    var timer;
    var uri = url.parse(data.url);

    if(!data.headers) {
      data.headers = {};
    }

    data.headers['Host'] = uri.host;

    var path = uri.pathname;
    if((data.method === 'GET') && uri.search) {
      path += uri.search;
    }
    if((data.method === 'POST') && data.body) {
      data.headers['Content-Length'] = data.body.length;
      if(!data.headers['Content-Type']) {
	data.headers['Content-Type'] = 'application/x-www-form-urlencoded';
      }
    }

    var port = parseInt(uri.port, 10) || 80;

    var client = http.createClient(port, uri.hostname);
    var request = client.request(data.method, path, data.headers);

    if('POST' === data.method) {
      request.write(data.body);
    }
    request.end();

    var handle_response = function(response) {
      if(timer) {
	clearTimeout(timer);
      }
      done();
    };

    var handle_timeout = function(response) {
      request.removeListener('response', handle_response); // v important to do this
      done('release', 30);
    };

    timer = setTimeout(handle_timeout, 30000);

    client.on('error', function() {
      if(timer) {
	clearTimeout(timer);
      }

      request.removeListener('response', handle_response); // v important to do this
      done('bury');
    });

    request.on('response', handle_response);
  }

};
