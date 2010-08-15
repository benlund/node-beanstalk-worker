var sys = require('sys'),
   http = require('http');
http.createServer(function (req, res) {
		    sys.puts('received a request');
		    if(Math.random() > 0.5) {
		      res.sendHeader(200, {'Content-Type': 'text/plain'});
		      res.write('ok');
		      sys.puts('send a resp');
		      res.end();
		    }
}).listen(9876);
sys.puts('Server running at http://127.0.0.1:9876/');
