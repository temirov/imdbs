var redis      = require("redis"),
  util         = require("util"),
  async        = require("async"),
  localhost    = '127.0.0.1',
  redis_config = {local: 
                    {ip: localhost,
                    port: 6379},
                  c9:
                    {ip: process.env.IP,
                    port: 16349},
                  ec2:
                    {ip: process.env.REDIS_HOST,
                    port: 6379},
                  };

switch (true) { 
  case redis_config.c9.ip:
    redis_config.current = edis_config.c9;
    break;
  case redis_config.ec2.ip:
    redis_config.current = redis_config.ec2;
    break;
  default:
    redis_config.current = redis_config.local;
}

redis_config.current = redis_config.ec2;
                  
// redis_config.current = redis_config.c9.ip ? redis_config.c9 : redis_config.local;
var db = redis.createClient(redis_config.current.port, redis_config.current.ip);

module.exports = function initS (app) {
  app.get('/s/years', function(req, res) {
    getYearsCount(function(err, years){
      res.json(years);
    });
  });

  app.get('/s/ranks', function(req, res) {
    getRanksCount(function(err, ranks){
      res.json(ranks);
    });
  });

  app.get('/s/ranks_by_year', function(req, res) {
    getRanksCountByYear(function(err, ranksByYear){
      res.json(ranksByYear);
    });
  });

  app.get('/s/r_ranks_by_year', function(req, res) {
    getRoundedRanksByYear(function(err, ranksByYear){
      res.json(ranksByYear);
    });
  });
};

function getRanksCount(callback){
  var ranks = [],
    rank = 0.9;

  async.whilst(
    function() { 
      return rank < 10; 
    },
    function(next) {
      db.select(1, function(err, res){
        if (res == 'OK') {
          rank = (rank + 0.1).toFixed(1);
          db.scard("ranks:" + rank, function(err, count){
            ranks.push({r: rank, c: count});
            rank = parseFloat(rank);
            next();
          });
        }
      });
    },
    function(err) {
      callback(err, ranks);
    }
  );
};

function getYearsCount(callback){
  var years = [], 
    year = 1888,
    current_year = new Date().getFullYear();
  
  async.whilst(
    function() { 
      return year <= current_year; 
    },
    function(next) {
      db.select(1, function(err, res){
        if (res == 'OK') {
          db.scard("years:" + year, function(err, count){
            // util.log(util.format("year is: %s, count is: %d", year, count));
            years.push({y: year, c: count});
            year += 1;
            next();
          });
        }
      });
    },
    function(err) {
      callback(err, years);
    }
  );
}

function getRanksCountByYear(callback){
  var ranksByYear = [],
    year = 1888,
    current_year = new Date().getFullYear();
    rank = 1.0;

  async.whilst(
    function() { 
      return year <= current_year;
    },
    function(next) {
      db.select(1, function(err, res){
        if (res == 'OK') {
          db.scard("years_ranks:" + year + ":" + rank, function(err, count){
            if (+count > 0) {
              ranksByYear.push({year: year, rank: rank, count: count});
            }
            if (rank == 10) {
              rank = 1.0; 
              year += 1; 
            }
            rank = parseFloat((rank + 0.1).toFixed(1));
            next();
          });
        }
      });
    },
    function(err) {
      callback(err, ranksByYear);
    }
  );
};

function getRoundedRanksByYear(callback){
  var ranksByYear = [],
    year = 1888,
    current_year = new Date().getFullYear();
    rank = 1;

  async.whilst(
    function() { 
      return year <= current_year;
    },
    function(next) {
      db.select(1, function(err, res){
        if (res == 'OK') {
          db.sinter("ranks_grouped:" + rank, "years:" + year, function(err, intersection){
            if (intersection.length > 0) {
              ranksByYear.push({year: year, rank: rank, count: intersection.length});
            }
            if (rank == 10) {
              rank = 1; 
              year += 1; 
            }
            rank += 1;
            next();
          });
        }
      });
    },
    function(err) {
      callback(err, ranksByYear);
    }
  );
};