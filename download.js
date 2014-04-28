// curl --retry 3 -C -O "ftp://ftp.fu-berlin.de/pub/misc/movies/database/movies.list.gz"
// curl --retry 3 -C -O "ftp://ftp.fu-berlin.de/pub/misc/movies/database/ratings.list.gz"

var JSFtp = require("jsftp"),
  async = require('async'),
  util = require('util'),
  domain = require('domain').create();

var remotePath = 'pub/misc/movies/database/',
  remoteFiles = ['ratings.list.gz'],
  localPath = 'data/source/';

var ftp = new JSFtp({
  host: "ftp.fu-berlin.de",
  // port: 3331, // defaults to 21
  // user: "user", // defaults to "anonymous"
  // pass: "1234" // defaults to "@anonymous"
});

function handleError(err) {
  if (err) {
    util.log('An error occured:\n');
    util.log(err.stack);
    process.exit(1);
  }
};

domain.on('error', function(err) {
  handleError(err);
});

domain.add(ftp);

domain.run(function() {
  util.log("Data import started");

  async.each(remoteFiles,
    function(file, next){
      ftp.get(remotePath + file, localPath + file, function(hadErr){
        handleError(hadErr);
        util.log(util.format("File %s copied successfully!", file));
        next();
      });
    },
    function(err){
      if (!err) {
        util.log('Files copied successfully! \n ');
        process.exit(0); 
      }
    }
  );
});
