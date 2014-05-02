var JSFtp = require("jsftp"),
  async = require('async'),
  util = require('util'),
  fs = require("fs"),
  path = require('path'),
  unzip = require('zlib').createGunzip(),
  events = require('events'),
  emitter = new events.EventEmitter();

var remotePath = 'pub/misc/movies/database/',
  remoteFiles = ['ratings.list.gz'],
  localPath = 'data/source/';

var ftp = new JSFtp({
  host: "ftp.fu-berlin.de",
});

function handleError(err) {
  if (err && err.code != 'EEXIST') {
    util.log('An error occured:\n');
    util.log(err.stack);
    process.exit(1);
  }
};

fs.mkdir(localPath, function(err){
  handleError(err);
  async.each(remoteFiles,
    function(file, next){
      util.log(util.format("Download of %s has started", file));
      ftp.get(remotePath + file, localPath + file, function(err){
        handleError(err);
        util.log(util.format("File %s copied successfully!", file));
        next();
      });
    },
    function(err){
      handleError(err);
      util.log('Finished downloading files');
      emitter.emit('done');
    }
  );
});

emitter.on('done',function(){
  async.each(remoteFiles, 
    function(file, next){
      var compresedFile = fs.createReadStream(localPath + file),
        decompressedFile = fs.createWriteStream(localPath + path.basename(file, '.gz'));

      compresedFile.on('error', handleError);
      decompressedFile.on('error', handleError);

      compresedFile
        .pipe(unzip)
        .pipe(decompressedFile);

      decompressedFile.on('close',function(){
        next();
      });
    },
    function(err){
      handleError(err);
      util.log(util.format("Finished unzipping files"));
      process.exit(0); 
    }
  );
});
