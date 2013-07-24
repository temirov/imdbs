var localhost         = '127.0.0.1',
  redis               = require("redis"),
  fs                  = require('fs'),
  util                = require('util'),
  d                   = require('domain').create(),
  imdb_source_ratings = 'data/source/ratings.list',
  first_chunk, 
  last_chunk, 
  first = true;

var redis_config = {local: 
                      {ip: localhost,
                      port: 6379},
                    c9:
                      {ip: process.env.IP,
                      port: 16379},
                    };

redis_config.current = redis_config.c9.ip ? redis_config.c9 : redis_config.local;
var client = redis.createClient(redis_config.current.port, redis_config.current.ip);

function process_error(err) {
  util.log('HORRIBLE ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!');
  util.log(util.format("Full title is: %s;\n Year is %d; Incr is: %d; Votes is: %d", title, year, incr, votes));
  util.log(util.inspect(client.hgetall("ratings:" + incr)));
  util.log(util.inspect(err));
}
    
function isInt(year) {
  return !isNaN(parseInt(year, 10));
}

function parse_year_from_title(title, callback){
  var year_position = 0,
    year, 
    err = null;

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
  client.incr("next.ratings.id", function(err, incr){
    if (typeof title != 'undefined') {
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
    }
    // util.log(util.format("Ready to Insert: Full title is: %s;\n Year is %d", title, year));
  });
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
      var input = fs.createReadStream(imdb_source_ratings, {encoding:'utf8'});
      util.log("Data import started");

      input.on('end', function() {
        util.log("The end"); 
        client.quit();
        process.exit(0);       
      }); 

      input.on('data', function(data) {
        var lump = data.split("\n"),
          reckage = new Array();

        if (first) {
          lump.splice(0, 296);
          last_chunk = lump.splice(-1,1)[0];
        } else {
          first_chunk = lump.splice(0,1)[0];
          broken_chunk = last_chunk + first_chunk;
          lump.unshift(broken_chunk) 
          // reckage.unshift(broken_chunk);
          last_chunk = lump.splice(-1,1)[0];
        }
        first = false;
        
        // reckage.map(function(chunk){
        //   split_rating(chunk, insert_rating);
        // });
        
        lump.map(function(chunk){
          split_rating(chunk, insert_rating);
        });
      });

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
