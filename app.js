var express = require('express')
  , routes = require('./routes')
  , http = require('http')
  , path = require('path')
  , util = require('util');

var app = module.exports = express();

var localhost = '127.0.0.1';
  
// all environments
app.set('port', process.env.PORT || process.env.NODE_PORT || 3000);
app.set('address', process.env.IP || process.env.NODE_IP || localhost);
app.set('views', __dirname + '/views');
app.set('view engine', 'jade');
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.json());
app.use(express.urlencoded());
app.use(express.methodOverride());
app.use(app.router);
app.use(require('less-middleware')(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'public')));

// development only
if ('development' == app.get('env')) {
  app.use(express.errorHandler());
}

//Routes
routes.initHome(app);
routes.initS(app);

http.createServer(app).listen(app.get('port'), function(){
  console.log('Express server listening on port ' + app.get('port'));
});
