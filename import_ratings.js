var redis = require("redis"),
  redis_port = 16379,
  client = redis.createClient(redis_port, process.env.IP),
  fs = require('fs'),
  util = require('util'),
  domain = require('domain').create(),
  imdb_source_ratings = 'ratings.list',
  first_chunk, 
  last_chunk, 
  first = true;
    
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
    // console.log("year: %s, year_position: %s, isInt(year): %s, iterator: %s", year, year_position === -1, !isInt(year), i < 10);
  } while (!(isInt(year) || year_position === -1));

  if (!year) {
    err = new Error("YEAR IS VERY UNDEFINED!!!!!!!");
    err.Title = title;
  } 

  callback(err, year);
}

function insert_rating(distribution, votes, rank, title) {
  client.incr("next.ratings.id", function(err, incr){
    parse_year_from_title(title, function(err, year){
      client.hmset(
        "ratings:" + incr, 
        "distribution", distribution,
        "rank", rank,
        "votes", votes,
        "title", title,
        "year", year
      );
      
      client.zadd(
        "years", year, incr, function(err, resp) {
          if (err) {
            util.log('HORRIBLE ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!');
            util.log(util.format("Full title is: %s;\n Year is %d; Incr is: %d; Votes is: %d", title, year, incr, votes));
            util.log(util.inspect(client.hgetall("ratings:" + incr)));
            util.log(util.inspect(err));
            process.exit(1);
          }
        }
      );
      
    });
    // util.log(util.format("Ready to Insert: Full title is: %s;\n Year is %d", title, year));
  });
}

function split_rating(string, callback) {
  var short_string = string.trim(),
    distribution = short_string.substring(0,10).trim(),
    votes = short_string.substring(11,19).trim(),
    rank = short_string.substring(20,24).trim(),
    title = short_string.substring(25).trim();
  // util.log(util.format("\nFull title is: %s;\n Year is %d;", title, year));

  callback(distribution, votes, rank, title);
}

client.on("error", function (err) {
  console.log("Error " + err);
  process.exit(1); 
});

client.select(1, function(err, res){
  if (res == 'OK') {
    client.flushdb();
    util.log("The DB has been flushed");
    client.set("next.ratings.id",0);
    util.log("Ratings ID zeroed");
  } else {
    util.log("An error occured: %s", err);
  }
});

domain.on('error', function(err){
  // handle the error safely
  util.log(err);
});

domain.run(function(){
  var input = fs.createReadStream(imdb_source_ratings, {encoding:'utf8'}); 
  
  input.on('end', function() {
    console.log("The end"); 
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
      // lump.unshift(broken_chunk) 
      reckage.unshift(broken_chunk);
      last_chunk = lump.splice(-1,1)[0];
    }
    first = false;
    
    reckage.map(function(chunk){
      split_rating(chunk, insert_rating);
    });
    
    // lump.map(function(chunk){
    //   split_rating(chunk, insert_rating);
    // });
  
  });
});

// input.pipe();
