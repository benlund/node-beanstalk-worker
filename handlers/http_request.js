var http = require('http');
var url = require('url');

var handlers = {

  //data looks like {method: 'GET'|'POST', url: 'http://www.example.com/search?q=blah', headers: {'Extra-header': value}, body: 'bodystring'}
  http_request: function(data, done, failed) {
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
    }

    var port = uri.port || 80;

    var client = http.createClient(port, uri.hostname);
    var request = client.request(data.method, path, data.headers);
    var handle_response = function(response) {
			  done();
    };
    request.addListener('response', handle_response);
    if('POST' === data.method) {
      request.write(data.body);
    }
    request.close();
    setTimeout(function() {
      request.removeListener('response', handle_response); // v important to do this
      failed('timedout'); //@@fixme
    }, 30000);
  }

};

process.mixin(exports, handlers);