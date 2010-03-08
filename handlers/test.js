var handlers = {

  test: function(data, done, failed) {
    require('sys').puts('test job passed data: ' + JSON.stringify(data));
    done();
  }

};

process.mixin(exports, handlers);