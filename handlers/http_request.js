var http = require('http');
var url = require('url');

var handlers = {

  //data looks like {method: 'GET'|'POST', url: 'http://www.example.com/search?q=blah', headers: {'Extra-header': value}, body: 'bodystring'}
  http_request: function(data, done) {
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
    request.addListener('response', function (response) {
			  done();
			});
    if('POST' === data.method) {
      request.write(data.body);
    }
    request.close();
  }

};

process.mixin(exports, handlers);