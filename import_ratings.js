var localhost         = '127.0.0.1',
  redis               = require("redis"),
  fs                  = require('fs'),
  util                = require('util'),
  d                   = require('domain').create(),
  imdb_source_ratings = 'data/source/ratings.list',
  // imdb_source_ratings = 'data/source/OWL.txt',
  first_chunk, 
  last_chunk, 
  first = true;

var redis_config = {local: 
                      {ip: localhost,
                      port: 6379},
                    c9:
                      {ip: process.env.IP,
                      port: 16349},
                    };

redis_config.current = redis_config.c9.ip ? redis_config.c9 : redis_config.local;
var client = redis.createClient(redis_config.current.port, redis_config.current.ip);

// var stream = require('stream');
// var redis_stream = new stream.Writable();
// var Writable = require('stream').Writable;
// var redis_stream = new Writable();

// redis_stream._write = function (chunk, enc, next) {
//     console.dir(chunk);
//     next();
// };

// var ts = require('stream').Transform;
// var uppercase = new ts({decodeStrings: false});

function process_error(err) {
  util.log('HORRIBLE ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!');
  util.log(util.inspect(err));
}
    
function isInt(year) {
  return !isNaN(parseInt(year, 10));
}

function parse_year_from_title(title, callback){
  var year_position = 0,
    year; 
    // err = null;

  do {
    year_position = title.indexOf('(', year_position) + 1;
    if (year_position) {
      year = title.substr(year_position, 4);
    }
  } while (!(isInt(year) || year_position === 0));

  // if (!year) {
  //   err = new Error("YEAR IS VERY UNDEFINED!!!!!!!");
  //   err.Title = title;
  // } 

  callback(null, year);
}

function insert_rating(distribution, votes, rank, title) {
  if (typeof title != 'undefined') {
    client.incr("next.ratings.id", function(err, incr){
      parse_year_from_title(title, function(err, year){
        if (err) {
          process_error(err);
          process.exit(1);
        }
        // util.log(util.format("Ready to Insert: Full title is: %s;\n Year is %d", title, year));
        client.multi()
        .hmset(
          "ratings:" + incr, 
          "distribution", distribution,
          "rank", rank,
          "votes", votes,
          "title", title,
          "year", year,
          function(err, resp) {
            if (err) {
              process_error(err);
              process.exit(1);
            }
          }
        )
        .zadd(
          "years", year, incr, function(err, resp) {
            if (err) {
              process_error(err);
              process.exit(1);
            }
          }
        )
        .zadd(
          "ranks", rank, incr, function(err, resp) {
            if (err) {
              process_error(err);
              process.exit(1);
            }
          }
        )
        .zadd(
          "votes", votes, incr, function(err, resp) {
            if (err) {
              process_error(err);
              process.exit(1);
            }
          }
        )
        .zadd(
          "distributions", distribution, incr, function(err, resp) {
            if (err) {
              process_error(err);
              process.exit(1);
            }
          }
        )
        .exec(function(err, resp) {
          if (err) {
            process_error(err);
            process.exit(1);
          }
        });
      });
    // util.log(util.format("Ready to Insert: Full title is: %s;\n Year is %d", title, year));
    });
  }
}

function split_rating(string, callback) {
  var short_string = string.trim(),
    distribution   = short_string.substring(0,10).trim(),
    votes          = short_string.substring(11,19).trim(),
    rank           = short_string.substring(20,24).trim(),
    title          = short_string.substring(25).trim();
  callback(distribution, votes, rank, title);
}

client.on("error", function (err) {
  console.log("Error " + err);
  process.exit(1); 
});

d.on('error', function(er) {
  util.log(util.inspect(er));
});

d.add(client);

d.run(function() {
  client.select(0, function(err, res){
    if (res == 'OK') {
      client.flushdb();
      util.log("The DB has been flushed");
      client.set("next.ratings.id", 0);
      util.log("Ratings ID zeroed");
      // var input = fs.createReadStream(imdb_source_ratings, {encoding:'utf8'});
      var ratings = fs.createReadStream(imdb_source_ratings, {encoding:'utf8'});
      util.log("Data import started");
      
      // ratings.on('readable', function() {
      //   util.log("Working");  
      //   ratings.read();
      // }); 

      ratings.on('end', function() {
        client.bgsave();
        client.quit();
        util.log("The end");
        process.exit(0);       
      }); 

      ratings.on('data', function(data) {
        var lump = data.split("\n");

        if (first) {
          lump.splice(0, 296);
          last_chunk = lump.splice(-1,1)[0];
        } else {
          first_chunk = lump.splice(0,1)[0];
          var broken_chunk = last_chunk + first_chunk;
          lump.unshift(broken_chunk);
          last_chunk = lump.splice(-1,1)[0];
        }
        first = false;
        
        lump.map(function(chunk){
          split_rating(chunk, insert_rating);
        });
      });
      
      // var data = "";
      // ratings.on('readable', function() {
      //   //this functions reads chunks of data and emits newLine event when \n is found
      //   data += ratings.read();
      //   while( data.indexOf('\n') >= 0 ){
      //     ratings.emit('newLine', data.substring(0,data.indexOf('\n')));
      //     data = data.substring(data.indexOf('\n')+1);
      //   }
      // });
      
      // var offset = 0;
      // ratings.on('readable', function () {
      //   util.log('SMTH');
      //   var buf = ratings.read();
      //   if (!buf) return;
      //   for (; offset < buf.length; offset++) {
      //     if (buf[offset] === 0x0a) {
      //       util.log('SMTH');
      //       console.dir(buf.slice(0, offset).toString());
      //       buf = buf.slice(offset + 1);
      //       offset = 0;
      //       ratings.unshift(buf);
      //       return;
      //     }
      //   }
      //   ratings.unshift(buf);
      // });
      
      // util.log("The end"); 
      // client.quit();
      // process.exit(0); 

    } else {
      util.log("An error occured: %s", res);
      if (err) {
        process_error(err);
        process.exit(1);
      }
    }
  });
});

// input.pipe();
