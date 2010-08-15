module.exports = {

  workers: 3,
  server: '127.0.0.1:11300',
  tubes: ['external'],
  ignore_default: true,
  handlers: ['../handlers/test', '../handlers/http_request']

};
console.log('Loaded conf/example.js config');