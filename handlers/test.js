exports.handlers = {

  test: function(data, done) {
    require('sys').puts('test job passed data: ' + JSON.stringify(data));
    done();
  }

};
