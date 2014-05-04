function drawClusteredGraph(){
  var m = [80, 80, 80, 80],
  w = 960 - m[1] - m[3],
  h = 500 - m[0] - m[2];

  d3.json('s/r_ranks_by_year', function(err, nodes){
    var padding = 1.5, // separation between same-color nodes
    clusterPadding = padding * 4, // separation between different-color nodes
    minRadius = clusterPadding / 2,
    maxRadius = clusterPadding * 3;

    var minAndMaxCount = d3.extent(nodes, function(e){ return e.count });
    var radius = d3.scale.linear()
      .domain(minAndMaxCount)
      .range([minRadius, maxRadius]);

    nodes.forEach(function(d) {
      d.radius = radius(d.count);
    });

    var clusteredNodes = d3.nest()
      .key(function(d) { return d.rank; })
      .entries(nodes);

    // The largest node for each cluster.
    var clusters = [];
    clusteredNodes.forEach(function(element){
      var maxCountPerRate = d3.max(element.values, function(d){ return d.count });
      clusters.push(element.values.filter(function(e){ return e.count == maxCountPerRate})[0]);
    });

    var allRanks = clusteredNodes.map(function(d){return (+d.key)}).sort(d3.ascending);
    var color = d3.scale.category10()
      .domain(allRanks);
    var l = d3.scale.ordinal()
      .rangeBands([h, 0], 0.5, .3)
      .domain(allRanks);

    // Use the pack layout to initialize node positions.
    d3.layout.pack()
      .sort(null)
      .size([w, h])
      .children(function(d) { return d.values; })
      .value(function(d) { return d.radius * d.radius; })
      .nodes({values: clusteredNodes});

    var force = d3.layout.force()
      .nodes(nodes)
      .size([w, h])
      .gravity(.02)
      .charge(0)
      .on("tick", tick)
      .start();

    var svg = d3.select("#ranks_by_year_chart").append("svg")
      .attr("width", w + m[1] + m[3])
      .attr("height", h + m[0] + m[2]);

    var tooltip = d3.select("body")
      .append("div")
      .style("position", "absolute")
      .style("z-index", "10")
      .style("visibility", "hidden");

    var node = svg.selectAll("circle")
      .data(nodes)
    .enter().append("circle")
      .style("fill", function(d) { return color(d.rank); })
      .on("mouseover", function(d){ 
        expandCircle(d.rank);
        return tooltip
          .style("visibility", "visible")
          .text(d.year + " / " + d.count);
      })
      .on("mousemove", function(){
        return tooltip
          .style("top", (event.pageY-10)+"px")
          .style("left",(event.pageX+10)+"px");
      })
      .on("mouseout", function(d){
        expandCircle(d.rank);
        return tooltip.style("visibility", "hidden");
      })
      .call(force.drag);

    node.transition()
      .duration(750)
      .delay(function(d, i) { return i * 5; })
      .attrTween("r", function(d) {
        var i = d3.interpolate(0, d.radius);
        return function(t) { return d.radius = i(t); };
      });

    function expandCircle(rank) {
      var rank_radius = d3.select(".rank-" + rank).attr("r");
      if (rank_radius == minRadius * minRadius * (minRadius/2)) {
        var timer = null;
        clearInterval(timer);
        
        return timer = setTimeout(function() {
          return d3.select(".rank-" + rank).attr("r", minRadius * minRadius);
        }, 1000);
      } else {
        d3.select(".rank-" + rank).attr("r", minRadius * minRadius * (minRadius/2));
      }
    }

    function tick(e) {
      node
        .each(cluster(10 * e.alpha * e.alpha))
        .each(collide(.5))
        .attr("cx", function(d) { return d.x; })
        .attr("cy", function(d) { return d.y; });
    }

    // Move d to be adjacent to the cluster node.
    function cluster(alpha) {
      return function(d) {
        var cluster = clusters.filter(function(e){ return e.rank == d.rank; })[0];
        if (cluster === d) return;
        var x = d.x - cluster.x,
          y = d.y - cluster.y,
          l = Math.sqrt(x * x + y * y),
          r = d.radius + cluster.radius;
        if (l != r) {
          l = (l - r) / l * alpha;
          d.x -= x *= l;
          d.y -= y *= l;
          cluster.x += x;
          cluster.y += y;
        }
      };
    }

    // Resolves collisions between d and all other circles.
    function collide(alpha) {
      var quadtree = d3.geom.quadtree(nodes);
      return function(d) {
        var r = d.radius + maxRadius + clusterPadding,
          nx1 = d.x - r,
          nx2 = d.x + r,
          ny1 = d.y - r,
          ny2 = d.y + r;
        quadtree.visit(function(quad, x1, y1, x2, y2) {
          if (quad.point && (quad.point !== d)) {
            var x = d.x - quad.point.x,
              y = d.y - quad.point.y,
              l = Math.sqrt(x * x + y * y),
              r = d.radius + quad.point.radius + (d.rank === quad.point.rank ? padding : clusterPadding);
            if (l < r) {
              l = (l - r) / l * alpha;
              d.x -= x *= l;
              d.y -= y *= l;
              quad.point.x += x;
              quad.point.y += y;
            }
          }
          return x1 > nx2 || x2 < nx1 || y1 > ny2 || y2 < ny1;
        });
      };
    }

    // Draw legend
    var legend = svg.append("g")
      .classed('legend', true)
      .attr("transform", "translate(" + (w - m[1]) + ", 20)");

    var description = legend.selectAll('.description')
      .data(allRanks)
    .enter()
      .append("g")
        .classed("description", true)
        .attr("transform", function(d){ return "translate(0, " + l(d) + ")"; });
        
    description.append("circle")
      .style("fill", function(d){ return color(d); })
      .attr("class", function(d){ return "rank-" + d; })
      .attr("r", minRadius * minRadius);
    
    description.append("text")
      .attr("dx", minRadius * minRadius + 5)
      .attr("dy", ".25em")
      .text(function(d){ return d; });
  });
}

function drawFilmsPerYear(){
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
    yearsLine = svg.append("path")
      .attr("class", "line")
      .attr("clip-path", "url(#clip)")
      .attr("d", line(years));
    yearsLine.on

    svg.append("text")
      .attr("x", w - 6)
      .attr("y", h - 6)
      .attr("text-anchor", "end")
      .text("Films a year");
  });
}

function drawRanksDistribution(){
  var m = [80, 80, 80, 80],
  w = 960 - m[1] - m[3],
  h = 500 - m[0] - m[2];

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
}

drawFilmsPerYear();
drawRanksDistribution();
drawClusteredGraph();
// enableTooltips();
