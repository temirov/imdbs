function isInt(year) {
  return !isNaN(parseInt(year, 10));
}

function parse_year_from_title(title, callback){
  var year_position = 0,
    year, 
    i = 0,
    err = null;

  do {
    year_position = title.indexOf('(', year_position) + 1;
    if (year_position) {
      year = title.substr(year_position, 4);
    }
    i += 1;
    console.log("year: %s, year_position: %s, isInt(year): %s, iterator: %s", year, year_position === -1, !isInt(year), i < 10);
  } while (!(isInt(year) || year_position === -1));

  if (!year) {
    err = new Error("YEAR IS VERY UNDEFINED!!!!!!!");
    err.Title = title;
  } 

  callback(err, year);
}

var title = '3 (t�rt�net a szerelemr�l) (2007)';

parse_year_from_title(title, function(err, year){
  console.log(year);
});
