var redis = require("redis"),
    client = redis.createClient(),
    fs = require('fs'),
    util = require('util'),
    imdb_source_ratings = '/Volumes/External/imdb/ratings.list';
    imdb_source_ratings = '/Users/temirov/Documents/Development/imdb/ratings.list',
    first_chunk = "", 
    last_chunk = "", 
    first = true;

function isInt(n) {
  !isNaN(parseInt(year));
}

function insert_rating(distribution, votes, rank, title) {
  client.select(1, function(err, ret){
    client.incr("next.ratings.id", function(err, incr){
      year = parse_year_from_title(title);
      util.log(util.format("Ready to Insert: Full title is: %s;\n Year is %d", title, year));

      client.hmset(
        "ratings:" + incr, 
        "distribution", distribution,
        "rank", rank,
        "votes", votes,
        "title", title,
        "year", year
      );

      if (year) {
        client.zadd(
          "years", year, incr, function(err, resp) {
            if (err) {
              util.log('HORRIBLE ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!');
              util.log(util.format("Full title is: %s;\n Year is %d; Incr is: %d; Votes is: %d", title, year, incr, votes));
              util.log(client.hgetall("ratings:" + incr));
              process.exit(code=1);
            }
          }
        );
      } else {
        year = parse_year_from_title(title);
        util.log('YEAR IS UNDEFINED!!!!!!!');
        util.log(util.format("Full title is: %s;", title));
        util.log(client.hgetall("ratings:" + incr));
      }
    });
  });
};

function parse_year_from_title(title){
  var year_position = 0;

  do {
    year_position = title.indexOf('(', year_position) + 1;
    if (year_position) {
      year = title.substr(year_position, 4);
    }
    // util.log(util.format("\nFull title is: %s;\n Year is %d;\n Year is integer: %s;\n Year position is 0: %s", title, year, !isInt(year) , year_position == 0));
  } while ( !isInt(year) && year_position == 0);

  if (!year) {
    util.log('YEAR IS VERY UNDEFINED!!!!!!!');
    util.log(util.format("Full title is: %s;", title));
  }

  return year;
};

function split_rating(string, callback) {
  short_string = string.trim();
  var distribution = short_string.substring(0,10).trim(),
      votes = short_string.substring(11,19).trim(),
      rank = short_string.substring(20,24).trim(),
      title = short_string.substring(25).trim();
  // util.log(util.format("\nFull title is: %s;\n Year is %d;", title, year));

  callback(distribution, votes, rank, title);
}

client.on("error", function (err) {
  console.log("Error " + err);
  process.exit(code = 1); 
});

// client.select(1, function(err, res){
//   if (res == 'OK') {
//     client.flushDB();
//     console.log("The DB has been flushed");
//   }
// });
client.set("next.ratings.id",0);

input = fs.createReadStream(imdb_source_ratings, {encoding:'utf8'});

input.on('end', function() {
  console.log("The end"); 
  client.quit();
  process.exit(code = 0);       
}); 

input.on('data', function(data) {
  var lump = data.split("\n");

  if (first) {
    lump.splice(0, 296);
    last_chunk = lump.splice(-1,1)[0];
  } else {
    first_chunk = lump.splice(0,1)[0];
    broken_chunk = last_chunk + first_chunk;
    // lump.unshift(broken_chunk) 
    last_chunk = lump.splice(-1,1)[0];
  }
  first = false;
  
  lump.map(function(chunk){
    split_rating(chunk, insert_rating);
  });

});

// input.pipe();
