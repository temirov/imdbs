var JSFtp = require("jsftp"),
  async = require('async'),
  util = require('util'),
  fs = require("fs"),
  path = require('path'),
  unZip = require('zlib').createGunzip(),
  events = require('events'),
  emitter = new events.EventEmitter(),
  stream = require('stream');

util.inherits(FileByLine, stream.Transform);
util.inherits(FilterRatings, stream.Transform);

var fileByLine = new FileByLine(),
  filterRatings = new FilterRatings();

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

function FilterRatings(options) {
  if (!(this instanceof FilterRatings)) {
    return new FilterRatings(options);
  }

  stream.Transform.call(this, options);

  this._output = false;
  this._delayedOutput = false;
  this._chunkNumber = 0;
};

FilterRatings.prototype._transform = function(chunk, encoding, next) {
  var line = chunk.toString().trim();

  switch(line) {
    case 'MOVIE RATINGS REPORT':
      this._delayedOutput = true;
      break;
    case '------------------------------------------------------------------------------':
      this._output = false;
      this._delayedOutput = false;
      break;
  }

  if (this._delayedOutput) {
    this._chunkNumber += 1;
    if (this._chunkNumber === 4) {
      this._output = true;
      this._delayedOutput = false;
    }
  }

  if (this._output) {
    this.push(chunk);
  }

  next();
};

function FileByLine(options) {
  if (!(this instanceof FileByLine)) {
    return new FileByLine(options);
  }

  stream.Transform.call(this, options);

  this._broken_line = null;
  this._chunkNumber = 0;
};

FileByLine.prototype._flush = function(callback) {
  util.log(util.format("Chunks processed: %d", this._chunkNumber));
  callback(null);
}

FileByLine.prototype._transform = function(chunk, encoding, next) {
  var offset = chunk.length,
    line_offset = 0, 
    prev_offset = 0,
    output = false,
    delayed_output = false,
    i = 0;

  while (chunk[offset] !== 0x0a && offset >= 0) {
    offset -= 1;
  };
  offset = -(chunk.length - offset);

  if (this._broken_line) {
    chunk = Buffer.concat([this._broken_line, chunk]);
  }
  this._broken_line = chunk.slice(offset);
  chunk = chunk.slice(0,chunk.length + offset)

  while (line_offset < chunk.length) {
    if (chunk[line_offset] === 0x0a) {
      this.push(chunk.slice(prev_offset, line_offset));
      prev_offset = line_offset;
    }
    line_offset += 1;
  };

  this._chunkNumber += 1;
  next();
};

fileByLine.on('error', handleError);
filterRatings.on('error', handleError);

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
        .pipe(unZip)
        .pipe(fileByLine)
        .pipe(filterRatings)
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
