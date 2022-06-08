var sessionId;

var map = L.map('m', {
    renderer: L.canvas(),
}).setView([47.9965, 7.8469], 13);

L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a rel="noreferrer" target="_blank" href="#">OpenStreetMap</a>',
    opacity:0.9
}).addTo(map);

function trimStr(str) {
    if (str.length > 100) {
        return str.substring(0, 100) + " [...]";
    }

    return str;
}

function openPopup(data) {
    if (data.length > 0) {
        select_variables = [];
        row = [];

        for (var i in data[0]["attrs"]) {
            select_variables.push(data[0]["attrs"][i][0]);
            row.push(data[0]["attrs"][i][1]);
        }

        // code by hannah from old map UI

        // Build the HTML of the popup.
        //
        // NOTE: We assume that the last column contains the geometry information
        // (WKT), which we will not put in the table.
        geometry_column = select_variables.length - 1;
        // If the second to last variable exists and is called "?image" or ends in
        // "_image", then show an image with that URL in the first column of the
        // table. Note that we compute the cell contents here and add it during the
        // loop (it has to be the first cell of a table row).
        var image_cell = "";
        if (select_variables.length >= 2) {
          var image_column = select_variables.length - 2;
          if (select_variables[image_column] == "?image" ||
                select_variables[image_column] == "?flag" ||
                select_variables[image_column].endsWith("_image")) {
            var num_table_rows = select_variables.length - 2;
            image_url = row[image_column];
            if (image_url != null)
              image_cell = "<td rowspan=\"" + num_table_rows + "\"><a target=\"_blank\" href=\"" + image_url.replace(/^[<"]/, "").replace(/[>"]$/, "") + "\"><img src=\""
                           + image_url.replace(/^[<"]/, "").replace(/[>"]$/, "")
                           + "\"></a></td>";
          }
        }

        // Now compute the table rows in an array.
        var popup_content_strings = [];
        select_variables.forEach(function(variable, i) {
          // Skip the last column (WKT literal) and the ?image column (if it
          // exists).
          if (i == geometry_column ||
            variable == "?image" || variable == "?flag" || variable.endsWith("_image")) return;

          // Take the variable name as one table column and the result value as
          // another. Reformat a bit, so that it looks nice in an HTML table. and
          // the result value as another. Reformat a bit, so that it looks nice in
          // an HTML table.
          key = variable.substring(1);
          if (row[i] == null) { row[i] = "---"; }
          value = row[i].replace(/\\([()])/g, "$1")
                        .replace(/<((.*)\/(.*))>/,
                         "<a class=\"link\" href=\"$1\" target=\"_blank\">$3</a>")
                        .replace(/\^\^.*$/, "")
                        .replace(/\"(.*)\"(@[a-z]+)?$/, "$1");

          popup_content_strings.push(
            "<tr>" + (i == 0 ? image_cell : "") +
            "<td>" + key.replace(/_/g, " ") + "</td>" +
            "<td>" + value + "</td></tr>");
        })
        popup_html = "<table class=\"popup\">" + popup_content_strings.join("\n") + "</table>";
            var content = "<table>";

        L.popup({"maxWidth" : 600})
            .setLatLng(data[0]["ll"])
            .setContent(popup_html)
            .openOn(map);
    }
}

function showError(error) {
    console.error(error);
    document.getElementById("msg").style.display = "block";
    document.getElementById("loader").style.display = "none";
    document.getElementById("msg-inner").style.color = "red";
    document.getElementById("msg-inner").style.fontWeight = "bold";
    document.getElementById("msg-inner").style.fontSize = "20px";
    document.getElementById("msg-inner").innerHTML = error;
}

function loadMap(id, bounds) {
    console.log("Loading session " + id);
    document.getElementById("msg").style.display = "none";
    var ll = L.Projection.SphericalMercator.unproject({"x": bounds[0][0], "y":bounds[0][1]});
    var ur =  L.Projection.SphericalMercator.unproject({"x": bounds[1][0], "y":bounds[1][1]});
    var bounds = [[ll.lat, ll.lng], [ur.lat, ur.lng]];
    map.fitBounds(bounds);
    sessionId = id;
    L.nonTiledLayer.wms('heatmap', {
        minZoom: 0,
        opacity: 0.8,
        layers: id,
        format: 'image/png',
        transparent: true,
    }).addTo(map);

    map.on('click', function(e) {
        var ll= e.latlng;
        var pos = L.Projection.SphericalMercator.project(ll);

        fetch('pos?x=' + pos.x + "&y=" + pos.y + "&id=" + id + "&rad=" + (100 * Math.pow(2, 14 - map.getZoom())))
          .then(response => response.json())
          .then(data => openPopup(data));
        });
}

console.log("Loading data from QLever...");
fetch('query' + window.location.search)
  .then(response => {
        if (!response.ok) {
            return response.text().then(text => {throw new Error(text)});
        }
      return response;
    })
  .then(response => response.json())
  .then(data => loadMap(data["qid"], data["bounds"]))
  .catch(error => {showError(error);});
