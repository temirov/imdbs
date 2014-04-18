var m = [80, 80, 80, 80],
w = 960 - m[1] - m[3],
h = 500 - m[0] - m[2],
parse = d3.time.format("%Y").parse;

d3.json('/s/years', function(err, years){
  // Scales and axes. Note the inverted domain for the y-scale: bigger is up!
  var x = d3.time.scale().range([0, w]),
    y = d3.scale.linear().range([h, 0]),
    xAxis = d3.svg.axis().scale(x)
      .ticks(d3.time.years, 5)
      .tickSize(-h).orient("bottom"),
    yAxis = d3.svg.axis().scale(y)
      .ticks(4)
      .orient("right");

  // An area generator, for the light fill.
  var area = d3.svg.area()
    .interpolate("monotone")
    .x(function(d) { 
      return x(d.y); 
    })
    .y0(h)
    .y1(function(d) { 
      return y(d.c); 
    });

  // A line generator, for the dark stroke.
  var line = d3.svg.line()
    .interpolate("monotone")
    .x(function(d) { return x(d.y); })
    .y(function(d) { return y(d.c); });

  years.forEach(function(d) {
    d.y = parse(d.y.toString());
    d.c = +d.c;
  });

  // Compute the minimum and maximum date, and the maximum count.
  x.domain([years[0].y, years[years.length - 1].y]);
  y.domain([0, d3.max(years, function(d) { return d.c; })]).nice();

  var svg = d3.select("#movies_a_year_chart")
    .append("svg")
      .attr("width", w + m[1] + m[3])
      .attr("height", h + m[0] + m[2])
    .append("g")
      .attr("transform", "translate(" + m[3] + "," + m[0] + ")");

  // Add the clip path.
  svg.append("clipPath")
      .attr("id", "clip")
    .append("rect")
      .attr("width", w)
      .attr("height", h);

  // Add the area path.
  svg.append("path")
    .attr("class", "area")
    .attr("clip-path", "url(#clip)")
    .attr("d", area(years));

  // Add the x-axis.
  svg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0," + h + ")")
    .call(xAxis);

  // Add the y-axis.
  svg.append("g")
    .attr("class", "y axis")
    .attr("transform", "translate(" + w + ",0)")
    .call(yAxis);

  // Add the line path.
  svg.append("path")
    .attr("class", "line")
    .attr("clip-path", "url(#clip)")
    .attr("d", line(years));

  svg.append("text")
    .attr("x", w - 6)
    .attr("y", h - 6)
    .attr("text-anchor", "end")
    .text("Films a year");
});

d3.json('/s/ranks', function(err, ranks){
  var x = d3.scale.linear()
    .range([0, w]);

  var y = d3.scale.linear()
    .range([h, 0]);

  var xAxis = d3.svg.axis()
    .scale(x)
    .ticks(20)
    .tickSize(-h)
    .orient("bottom");

  var yAxis = d3.svg.axis()
    .scale(y)
    .ticks(4)
    .orient("left");

  var area = d3.svg.area()
    .x(function(r) { return x(r.r); })
    .y0(h)
    .y1(function(r) { return y(r.c); });

  var line = d3.svg.line()
    .x(function(r) { return x(r.r); })
    .y(function(r) { return y(r.c); });

  var svg = d3.select("#ranks_distribution_chart").append("svg")
      .attr("width", w + m[1] + m[3])
      .attr("height", h + m[0] + m[2])
    .append("g")
      .attr("transform", "translate(" + m[3] + "," + m[0] + ")");

  ranks.forEach(function(r) {
    r.r = +r.r;
    r.c = +r.c;
  });

  x.domain(d3.extent(ranks, function(r) { return r.r; }));
  y.domain([0, d3.max(ranks, function(r) { return r.c; })]);

  svg.append("path")
    .datum(ranks)
    .attr("class", "area")
    .attr("d", area);

  svg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0," + h + ")")
    .call(xAxis)
  .append("text")
    //- .attr("transform", "rotate(-90)")
    .attr("y", -12)
    .attr("x", w - 6)
    .attr("dy", ".71em")
    .style("text-anchor", "end")
    .text("Ranks");

  svg.append("g")
    .attr("class", "y axis")
    .call(yAxis)
  .append("text")
    .attr("transform", "rotate(-90)")
    .attr("y", 6)
    .attr("dy", ".71em")
    .style("text-anchor", "end")
    .text("Films");

  svg.append("path")
    .attr("class", "line")
    .attr("clip-path", "url(#clip)")
    .attr("d", line(ranks));
});
