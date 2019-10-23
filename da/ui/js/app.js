// var count = 10, padding = 5;
var columnWidth = 100
var rowHeight = 50
var svgContainer = d3.select("svg")
var messageNumber = 0;
var isPlaying = false;
var playInterval ;
var mode = 1;   //The current visualization mode (either time diagram or widget diagram)
const LOOP = 1000;   // Number of milliseconds between added lines, when in autoplay mode
var proc_centers = {};
var pidToState = {}
var c_centerx = 75
var c_centery = 75
var c_radius = 50
var c_row_separation = 100 + c_radius
var c_col_separation = 150 + c_radius
var listenersAdded = false;   //Indicates whether or not we have already added a set of listeners to the DOM body.


function escapeHTML(s)
{
    return s.replace(new RegExp(">", 'g'), "&gt;").replace(new RegExp("<", 'g'), "&lt;")
}

function strToColor(str)
{
    if (visualize_config && visualize_config.colors && visualize_config['colors'][str]) {
        return visualize_config['colors'][str];
    }
    str += "s"
    var hash = 0;
  for (var i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
  }
  var colour = '#';
  for (var i = 0; i < 3; i++) {
    var value = (hash >> (i * 8)) & 0xFF;
    colour += ('00' + value.toString(16)).substr(-2);
  }
  return colour;
}

// Define the div for the tooltip
var div = d3.select("body").append("div")
.attr("class", "tooltip")
.style("opacity", 0);

function sortMessages()
{
    function partition(A, p, r) {
      var x = A[r]['sender'][1];
      var pivot_PID = A[r]['sender'][0];
      var i = r;
      for (j = r ; j > 0 ; j--) {
        var y = A[j]['sender'][1];
        var curr_PID = A[j]['sender'][0];
        if (y > x || (y == x && curr_PID > pivot_PID)) {
          var temp1 = A[i];
          var temp2 = A[i - 1];
          var temp3 = A[j];
          A[j] = temp2;
          A[i - 1] = temp1;
          A[i] = temp3;
          i -= 1;
        }
      }

      return i;
    };

    function quickSort(A, p, r) {
      if (p < r) {
        q = partition(A, p, r);
        quickSort(A, p, q - 1);
        quickSort(A, q + 1, r);
      }
    };

    var indexed_data = [];
    for (var ind = 1; ind <= window.data["messages"].length; ind++) {
      indexed_data[ind] = window.data["messages"][ind-1];
    };

    quickSort(indexed_data, 1, indexed_data.length - 1);

    for (ind = 0; ind < window.data["messages"].length; ind++) {
      window.data["messages"][ind] = indexed_data[ind + 1];
    };

}

function drawMessage(senderProcess, senderClock, receiverProcess, receiverClock)
{
  var mtype = window.data["messages"][messageNumber]['msg'].split(',')[0];
  mtype = mtype.substr(2, mtype.length-3)
  // console.log((mtype));
  var startOffset = 20;
  if (mode == 1) {
    var line = svgContainer.append("line")
                          .attr("x1", columnWidth * senderProcess)
                          .attr("x2", columnWidth * receiverProcess)
                          .attr("y1", startOffset + rowHeight * senderClock)
                          .attr("y2", startOffset + rowHeight * receiverClock)
                          .attr("stroke", strToColor(mtype))
                          .attr("stroke-width", 2)
                          .attr("marker-end","url(#arrow)")
                          .attr("id", "line" + messageNumber)
                          .attr("class", "Message-Line")
                          .attr('data-payload', window.data["messages"][messageNumber]['msg'])
                          .on("mouseover", function(d ,i) {
                                div.transition()
                                    .duration(200)
                                    .style("opacity", .9);
                                div .html(d3.select(this).attr('data-payload'))
                                    .style("left", (d3.event.pageX) + "px")
                                    .style("top", (d3.event.pageY - 28) + "px");
                                })
                            .on("mouseout", function(d, i) {
                                div.transition()
                                    .duration(500)
                                    .style("opacity", 0);
                            });
    // receiver state
    svgContainer.append("circle")
                         .attr("cx", columnWidth * receiverProcess)
                         .attr("cy", startOffset + rowHeight * receiverClock)
                         .attr("r", 3)
                         .attr("class", "state-circle")
                         .attr("fill", "tomato")
                         .attr("stroke", "#ccc")
                         .attr("stroke-width", 1)
                         .attr("data-clk", receiverClock)
                         .attr("data-process", receiverProcess)
                         .on("mouseover", function(d ,i) {
                            div.transition()
                                .duration(200)
                                .style("opacity", .9);
                            var that = this;
                            div.html(function(){
                                var clk = d3.select(that).attr('data-clk');
                                var process = d3.select(that).attr('data-process');
                                return "<pre>" + JSON.stringify(window.states[process][clk], null, 2    ).replace(new RegExp(">", 'g'), "&gt;").replace(new RegExp("<", 'g'), "&lt;") + "</pre>";
                            })
                                .style("left", (d3.event.pageX) + "px")
                                .style("top", (d3.event.pageY - 28) + "px");
                            })
                         .on("mouseout", function(d, i) {
                            div.transition()
                                .duration(500)
                                .style("opacity", 0);
                        });

    // sender state
    svgContainer.append("circle")
                         .attr("cx", columnWidth * senderProcess)
                         .attr("cy", startOffset + rowHeight * senderClock)
                         .attr("r", 3)
                         .attr("class", "state-circle")
                         .attr("fill", "tomato")
                         .attr("stroke", "#ccc")
                         .attr("stroke-width", 1)
                         .attr("data-clk", senderClock)
                         .attr("data-process", senderProcess)
                         .on("mouseover", function(d ,i) {
                            div.transition()
                                .duration(200)
                                .style("opacity", .9);
                            var that = this;
                            div.html(function(){
                                var clk = d3.select(that).attr('data-clk');
                                var process = d3.select(that).attr('data-process');
                                return "<pre>" + JSON.stringify(window.states[process][clk], null, 2    ).replace(new RegExp(">", 'g'), "&gt;").replace(new RegExp("<", 'g'), "&lt;") + "</pre>";
                            })
                                .style("left", (d3.event.pageX) + "px")
                                .style("top", (d3.event.pageY - 28) + "px");
                            })
                         .on("mouseout", function(d, i) {
                            div.transition()
                                .duration(500)
                                .style("opacity", 0);
                        });
          } else {
            var senderID = data["messages"][messageNumber]["sender"][0];
            var receiverID = data["messages"][messageNumber]["receiver"][0];
            var senderCenter = proc_centers[senderID];
            var receiverCenter = proc_centers[receiverID];
            var line = svgContainer.append("line")
                      .attr("x1", senderCenter[0])
                      .attr("x2", receiverCenter[0])
                      .attr("y1", senderCenter[1])
                      .attr("y2", receiverCenter[1])
                      .attr("stroke", strToColor(mtype))
                      .attr("stroke-width", 2)
                      .attr("marker-end","url(#arrow)")
                      .attr("class", "curr_line")
                      .attr('data-payload', window.data["messages"][messageNumber]['msg'])
                      .on("mouseover", function(d ,i) {
                            div.transition()
                                .duration(200)
                                .style("opacity", .9);
                            div .html(escapeHTML(d3.select(this).attr('data-payload')))
                                .style("left", (d3.event.pageX) + "px")
                                .style("top", (d3.event.pageY - 28) + "px");
                            })
                        .on("mouseout", function(d, i) {
                            div.transition()
                                .duration(500)
                                .style("opacity", 0);
                        });
                      }


}


function stopInterval() {
  if (playInterval != undefined) {
    console.log("Clearing")
    clearInterval(playInterval);
    isPlaying = false;
    playInterval = undefined;
  }
}

function activateControls()
{

    function addMsg() {
      if (messageNumber < data["messages"].length) {
        if (mode == 1) {
        drawMessage(
                data["messages"][messageNumber]['sender'][0],
                data["messages"][messageNumber]['sender'][1],
                data["messages"][messageNumber]['receiver'][0],
                data["messages"][messageNumber]['receiver'][1],
                )
        messageNumber += 1;

        while (messageNumber-1 >= 0 && messageNumber < data["messages"].length &&
                data["messages"][messageNumber-1]['sender'][0] == data["messages"][messageNumber]['sender'][0] &&
                data["messages"][messageNumber-1]['sender'][1] == data["messages"][messageNumber]['sender'][1])
        {
            drawMessage(
                data["messages"][messageNumber]['sender'][0],
                data["messages"][messageNumber]['sender'][1],
                data["messages"][messageNumber]['receiver'][0],
                data["messages"][messageNumber]['receiver'][1],
                )
            messageNumber += 1;
        }
      } else {
        if (messageNumber < data["messages"].length) {
          if (messageNumber > 0) {
            d3.selectAll(".curr_line").remove();
          }
          var senderID = data["messages"][messageNumber]["sender"][0];
          var receiverID = data["messages"][messageNumber]["receiver"][0];
          var senderCenter = proc_centers[senderID];
          var receiverCenter = proc_centers[receiverID];
          drawMessage(
            senderID, senderCenter, receiverID, receiverCenter
          )
            var senderClock = data["messages"][messageNumber]["sender"][1];
            var receiverClock = data["messages"][messageNumber]["receiver"][1];
            pidToState[receiverID] = receiverClock;

            messageNumber += 1;
            while (messageNumber-1 >= 0 && messageNumber < data["messages"].length &&
                    data["messages"][messageNumber-1]['sender'][0] == data["messages"][messageNumber]['sender'][0] &&
                    data["messages"][messageNumber-1]['sender'][1] == data["messages"][messageNumber]['sender'][1])
            {
                receiverID = data["messages"][messageNumber]["receiver"][0];
                receiverCenter = proc_centers[receiverID];
                drawMessage(
                    senderID, senderCenter, receiverID, receiverCenter
                    )
                messageNumber += 1;
            }

          }
      }
      }
      else
      {
       stopInterval();
      }
    };

    function play() {
        isPlaying = true;
        if (messageNumber < window.data["messages"].length) {
          addMsg();   //So there is no delay on initial added message
          playInterval = setInterval(addMsg, LOOP);
        }
    };


    function prev() {
        if (isPlaying == false) {
          if (messageNumber > 0) {
            messageNumber -= 1;
            if (mode == 1) {
              d3.select("#line" + messageNumber).remove();
              d3.select("#text" + messageNumber).remove();
            } else {
              d3.selectAll(".curr_line").remove();
            }
              var currPID = data["messages"][messageNumber]["sender"][0]
              var currClk = data["messages"][messageNumber]["sender"][1]
              while(messageNumber >= 0 && data["messages"][messageNumber]["sender"][0] == currPID && data["messages"][messageNumber]["sender"][1] == currClk) {
                if (mode == 1) {
                  d3.select("#line" + messageNumber).remove();
                  d3.select("#text" + messageNumber).remove();
                }
                messageNumber -= 1;
              }
              if (mode == 1) {      // Just add back to the index, because we are not
                messageNumber += 1; // adding any prior lines, as in the process diagram.
              } else if (messageNumber >= 0) {
                  currPID = data["messages"][messageNumber]["sender"][0]
                  currClk = data["messages"][messageNumber]["sender"][1]
                  while(messageNumber >= 0 && data["messages"][messageNumber]["sender"][0] == currPID && data["messages"][messageNumber]["sender"][1] == currClk) {
                    messageNumber -= 1;
                  }
                messageNumber += 1;
                addMsg();
              } else {
                messageNumber += 1;
              }
            }
        }
    }

    function next() {
      if (isPlaying == false) {
        addMsg();
      }
    }


    function pause() {
      stopInterval();
    }


    function reset() {
        stopInterval();
        if (mode == 1) {
          d3.selectAll(".state-circle").remove();
          d3.selectAll(".Message-Line").remove();
          d3.selectAll(".Message-Text").remove();
        } else {
          d3.selectAll(".curr_line").remove();
        }
        isPlaying = false;
        messageNumber = 0;
    }

    var map_keys = Object.keys(data["process_map"]);
    for (var ind = 0; ind < map_keys.length; ind++) {
      var id = map_keys[ind];
      pidToState[id] = 0;
    }

if (listenersAdded == false) {    //Enforces that this logic is called only once.
  listenersAdded = true;
    $('#Play').on('click', play);
    $('#Prev').on('click', prev);
    $('#Next').on('click', next);
    $('#Pause').on('click', pause);
    $('#Reset').on('click', reset);

    $("body").on('keydown', function(e) {
      if(e.keyCode == 37) { // left
        prev();
      }
      else if(e.keyCode == 39) { // right
        next();
      }
      else if(e.keyCode == 27)
      {
        reset();
      }
      else if(e.keyCode == 32) {
        if (isPlaying)
        {
            pause();
        }
        else
        {
            play();
        }
      }
    });
  };
}

function showAll()
{
 for (var i = 0; i < data["messages"].length; i++) {
    drawMessage(
        data["messages"][messageNumber]['sender'][0],
        data["messages"][messageNumber]['sender'][1],
        data["messages"][messageNumber]['receiver'][0],
        data["messages"][messageNumber]['receiver'][1],
        )
    messageNumber += 1;
  }
}

function setDimensions()
{
    window.maxClock = data['maxClock'];
    window.processCount = data['process_count'];
    svgContainer.attr('height', rowHeight * (window.maxClock + 1));
    svgContainer.attr('width', columnWidth * (window.processCount + 1));
}


function processState()
{
  var m =  window.data["pid_map"];
  window.states = {}
  for (var i = 0; i < window.data["states"].length; i++) {
    var pname = window.data["states"][i]["id"]
    var state = window.data["states"][i]["state"]
    var clk = window.data["states"][i]["clk"]

    if (m[pname] in window.states)
    {
         window.states[m[pname]][clk] = state;
    }
    else
    {
        window.states[m[pname]] = {}
        window.states[m[pname]][clk] = state;
    }
  }
}

function drawTimeDiagram()
{
    setDimensions();

    drawGrid();

    sortMessages();

    activateControls();

    processState();

    //showAll();

}

function drawGrid()
{

  var startOffset = 20;
  if (mode == 1) {
    for(var i = 1; i <= processCount; ++i) {

      // vertical lines
      svgContainer.append("line")
        .attr("x1", i*columnWidth)
        .attr("x2", i*columnWidth)
        .attr("y1", startOffset + rowHeight)
        .attr("y2", startOffset + rowHeight*(maxClock+2))
        .attr("stroke", strToColor(window.data["process_map"][i][0]))
        .attr("stroke-width", 5);

      svgContainer.append("text")
        .attr("x", columnWidth*i)
        .attr("y", rowHeight-20)
        .attr("fill", strToColor(window.data["process_map"][i][0]))
         .append('svg:tspan')
          .attr('x', columnWidth*i-20)
          .attr('dy', -5)
          .text(function(d) { return window.data["process_map"][i][0]; })
        .append('svg:tspan')
          .attr('x',  columnWidth*i-20)
          .attr('dy', 20)
          .text(function(d) { return window.data["process_map"][i][1]; })
    }

    for(var i = 1; i <= maxClock; ++i) {
        // horizontal lines
        var line = svgContainer.append("line")
                                .attr("x1", columnWidth)
                                .attr("x2", processCount*columnWidth)
                                .attr("y1", startOffset + i *rowHeight)
                                .attr("y2", startOffset + i* rowHeight)
                                .attr("stroke", "#ccc")
                                .attr("stroke-dasharray","5,10,5")
                                .attr("stroke-width", 1);
        var t = svgContainer.append("text")
                                .attr("x", 40)
                                .attr("y", startOffset + i *rowHeight+8)
                                .text("clk " + (i-1));
    }
  } else {
    var proc_types = {};
    var keys = Object.keys(data["process_map"]);
    var curr_farthest_right = 0;
    for (var i = 0; i < keys.length; i++) {
      var p_type = data["process_map"][keys[i]][0];
      var p_pid_alias = keys[i];
      var p_keys = Object.keys(proc_types);
      if (p_keys.includes(p_type) == false) {
        curr_farthest_right += 1;
        proc_types[p_type] = [curr_farthest_right, [p_pid_alias]];
      } else {
        proc_types[p_type][1].push(p_pid_alias);
      }
    }

    var proc_keys = Object.keys(proc_types);
    // console.log(proc_types);
    for (var i = 0; i < proc_keys.length; i++) {    // For each process type
      var num_instances = proc_types[proc_keys[i]][1].length;
      for (var j = 0; j < num_instances; j++) {   // For each instance of the current process type
        var center = [c_centerx + (c_col_separation * i), c_centery + (c_row_separation * j)];
        proc_centers[proc_types[proc_keys[i]][1][j]] = center;
        // console.log()
        var pname = data["process_map"][proc_types[proc_keys[i]][1][j]][0];
        var pid = data["process_map"][proc_types[proc_keys[i]][1][j]][1];


        svgContainer.append("circle")
                        .attr("cx", c_centerx + (c_col_separation * i))
                        .attr("cy", c_centery + (c_row_separation * j))
                        .attr("r", c_radius)
                        // .attr("stroke", "#28a745")
                        .attr("stroke-width", 3)
                        .attr("fill", strToColor(pname))
                        .attr("id", proc_types[proc_keys[i]][1][j])
                        .on("mouseover", function(d ,i) {
                                div.transition()
                                    .duration(200)
                                    .style("opacity", .9);
                                var that = this;
                                div.html(function(){
                                    var process = d3.select(that).attr('id');
                                    var clk = pidToState[process];
                                    if (clk in window.states[process])
                                    {
                                        return "<pre>" + escapeHTML(JSON.stringify(window.states[process][clk], null, 2)) + "</pre>";
                                    }
                                    else
                                    {
                                        return "<pre>Not available</pre>";
                                    }
                                })
                                    .style("left", (d3.event.pageX) + "px")
                                    .style("top", (d3.event.pageY - 28) + "px");
                                })
                             .on("mouseout", function(d, i) {
                                div.transition()
                                    .duration(500)
                                    .style("opacity", 0);
        });

        svgContainer.append("text")
                    .attr("x", c_centerx + (c_col_separation * i))
                    .attr("y", c_centery + (c_row_separation * j))
                    .attr("text-anchor", "middle")
                    .append('svg:tspan')
                      .attr('x', c_centerx + (c_col_separation * i))
                      .attr("fill", "white")
                      .attr('dy', -5)
                      .text(function(d) { return pname; })
                    .append('svg:tspan')
                      .attr('x',  c_centerx + (c_col_separation * i))
                      .attr('dy', 20)
                      .attr("fill", "white")
                      .text(function(d) { return pid; });
      }
    }
  }
}

function loadData(path)
{
    var script = document.createElement('script');
    script.src = path;
    script.id = "da-data";
    document.body.append(script)

    console.log('loaded ' + path);
}

function GetVizData(data)
{
    window.data = data;



    // visualize
    drawTimeDiagram();

}

$(function(){

    // get the data path
    var urlParams = new URLSearchParams(window.location.search);

    if (urlParams.has("mode"))
    {
        window.mode = urlParams.get("mode");
    }

    if (urlParams.has("data"))
    {
        loadData(urlParams.get("data"));
    }

     $('select.spec').on('change', function(d)
    {
        // reset
        messageNumber = 0;
        stopInterval();
        svgContainer.selectAll("line,text,circle").remove();
        loadData(this.value);
    });

    $('select.mode').on('change', function(d)
    {
        // reset
       window.mode = this.value;
       mode = this.value;
        // reset
        stopInterval();
        messageNumber = 0;
        svgContainer.selectAll("line,text,circle").remove();
        drawTimeDiagram();
        stopInterval();
    });

});
