function isYear(year) {
  // return !isNaN(parseInt(year, 10));
  return (year.match(/(18|19|20)\d{2}/g)[0] && +year>1850 && +year<=(new Date).getFullYear()) 
}

function parse_year_from_title(title, callback){
  var year = null, 
  err = null;

  year = title.match(/\s\((18|19|20)\d{2}\)/g);
  
  if (year) {
    year = year[0].substr(2,5)
  } else {
    err = new Error("YEAR IS VERY UNDEFINED!!!!!!!");
    err.Title = title;
  } 

  callback(err, year);
}

var title = '3 (t�rt�net a szerelemr�l) (2007)';

parse_year_from_title(title, function(err, year){
  console.log(year);
});
