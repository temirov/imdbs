var localhost         = '127.0.0.1',
  redis               = require("redis"),
  fs                  = require('fs'),
  util                = require('util'),
  domain              = require('domain').create(),
  imdb_source_ratings = 'data/source/ratings.list',
  broken_line;

var redis_config = {local: 
                      {ip: localhost,
                      port: 6379},
                    c9:
                      {ip: process.env.IP,
                      port: 16349},
                    };

redis_config.current = redis_config.c9.ip ? redis_config.c9 : redis_config.local;
var db = redis.createClient(redis_config.current.port, redis_config.current.ip);

var stream           = require('stream');
var import_redis     = new stream.Writable();
var reshape_chunks   = new stream.Transform();
var split_chunks     = new stream.Transform();

import_redis._write = function (chunk, encoding, callback) {
  // util.log('Buffer length received by write stream: ' + chunk.length);
  split_rating(chunk, insert_rating);
  callback(null);
};

split_chunks._transform = function(chunk, encoding, callback) {
  var offset        = 0, 
    prev_offset     = 0;

  // util.log(util.format('Buffer length received by split_chunks transform stream: %d', chunk.length));
  while (offset < chunk.length) {
    if (chunk[offset] === 0x0a) {
      // util.log(util.format('split_chunks offset: %d', offset));
      // util.log(util.format('Split line is: %s', chunk.slice(offset)));
      split_chunks.push(chunk.slice(prev_offset, offset));
      prev_offset = offset;
    }
    offset += 1;
  };

  callback(null);
};

reshape_chunks._transform = function(chunk, encoding, callback) {
  // util.log(util.format('Buffer length received by reshape_chunks transform stream: %d', chunk.length));
  var offset = chunk.length;
  while (chunk[offset] !== 0x0a) {
    offset -= 1;
  };
  offset = -(chunk.length - offset);
  // util.log(util.format('reshape_chunks offset: %d', offset));

  if (broken_line) {
    chunk = Buffer.concat([broken_line, chunk]);
  }

  broken_line = chunk.slice(offset);
  // util.log(util.format('Broken line is: %s', broken_line));

  callback(null, chunk);
};

function handle_error(err) {
  if (err) {
    util.log('HORRIBLE ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!');
    util.log(util.inspect(err));
    process.exit(1);
  }
};
    
function isInt(year) {
  return !isNaN(parseInt(year, 10));
};

function parse_year_from_title(title, callback){
  var year_position = 0,
    year = null; 

  do {
    year_position = title.indexOf('(', year_position) + 1;
    if (year_position) {
      year = title.substr(year_position, 4);
    }
  } while (!(isInt(year) || year_position === 0));

  callback(null, year);
};

function insert_rating(distribution, votes, rank, title) {
  if (typeof title != 'undefined') {
    db.incr("next.ratings.id", function(err, incr){
      parse_year_from_title(title, function(err, year){
        handle_error(err);
        // util.log(util.format("Ready to insert title: %s;\n year: %d", title, year));
        db.multi()
        .hmset(
          "ratings:" + incr, 
          "distribution", distribution,
          "rank", rank,
          "votes", votes,
          "title", title,
          "year", year,
          function(err, resp) {
            handle_error(err);
          }
        )
        .zadd(
          "years", year, incr, function(err, resp) {
            handle_error(err);
          }
        )
        .zadd(
          "ranks", rank, incr, function(err, resp) {
            handle_error(err);
          }
        )
        .exec(function(err, resp) {
          handle_error(err);
        });
      });
    // util.log(util.format("Ready to Insert: Full title is: %s;\n Year is %d", title, year));
    });
  }
};

function split_rating(single_chunk, callback) {
  var short_string = single_chunk.toString().trim(),
    distribution   = short_string.substring(0,10).trim(),
    votes          = short_string.substring(11,19).trim(),
    rank           = short_string.substring(20,24).trim(),
    title          = short_string.substring(25).trim();
  callback(distribution, votes, rank, title);
};

db.on("error", function(err) {
  handle_error(err);
});

domain.on('error', function(err) {
  handle_error(err);
});

domain.add(db);
domain.add(import_redis);
domain.add(split_chunks);
domain.add(reshape_chunks);

domain.run(function() {
  db.select(0, function(err, res){
    if (res == 'OK') {
      db.flushdb(function(err, res){
        if (res == 'OK') {
          util.log("The DB has been flushed");
          db.set("next.ratings.id", 0, function(err, res){
            if (res == 'OK') {
              util.log("Ratings ID zeroed");
              var ratings = fs.createReadStream(imdb_source_ratings, {encoding: null});
              util.log("Data import started");

              ratings
                .pipe(reshape_chunks)
                .pipe(split_chunks)
                .pipe(import_redis);

              ratings.on('end', function() {
                db.bgsave();
                db.quit();
                util.log("The end");
                process.exit(0);       
              });  
            } else {
              handle_error(err);
            }
          });
        }
        else {
          handle_error(err);
        };
      });
    } else {
      handle_error(err);
    }
  });
});
