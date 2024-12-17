let sessionId;
let curGeojson;
let curGeojsonId = -1;

let urlParams = new URLSearchParams(window.location.search);
let qleverBackend = urlParams.get("backend");
let query = urlParams.get("query");
let mode = urlParams.get("mode");

// id of SetInterval to stop loadStatus requests on error or load finish
let loadStatusIntervalId = -1;

let map = L.map('m', {
    renderer: L.canvas(),
    preferCanvas: true
}).setView([47.9965, 7.8469], 13);
map.attributionControl.setPrefix('University of Freiburg');

let layerControl = L.control.layers([], [], {collapsed:true, position: 'topleft'}).addTo(map);

let osmLayer = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a rel="noreferrer" target="_blank" href="#">OpenStreetMap</a>',
    maxZoom: 19,
    opacity:0.9
}).addTo(map);

let genError = "<p>Session has been removed from cache.</p> <p> <a href='javascript:location.reload();'>Resend request</a></p>";

function openPopup(data) {
    if (data.length > 0) {
        let select_variables = [];
        let row = [];

        for (let i in data[0]["attrs"]) {
            select_variables.push(data[0]["attrs"][i][0]);
            row.push(data[0]["attrs"][i][1]);
        }

        // code by hannah from old map UI

        // Build the HTML of the popup.
        //
        // NOTE: We assume that the last column contains the geometry information
        // (WKT), which we will not put in the table.
        let geometry_column = select_variables.length - 1;
        // If the second to last variable exists and is called "?image" or ends in
        // "_image", then show an image with that URL in the first column of the
        // table. Note that we compute the cell contents here and add it during the
        // loop (it has to be the first cell of a table row).
        let image_cell = "";
        if (select_variables.length >= 2) {
          let image_column = select_variables.length - 2;
          if (select_variables[image_column] == "?image" ||
                select_variables[image_column] == "?flag" ||
                select_variables[image_column].endsWith("_image")) {
            let num_table_rows = select_variables.length - 2;
            let image_url = row[image_column];
            if (image_url != null)
              image_cell = "<td rowspan=\"" + num_table_rows + "\"><a target=\"_blank\" href=\"" + image_url.replace(/^[<"]/, "").replace(/[>"]$/, "") + "\"><img src=\""
                           + image_url.replace(/^[<"]/, "").replace(/[>"]$/, "")
                           + "\"></a></td>";
          }
        }

        // Now compute the table rows in an array.
        let popup_content_strings = [];
        select_variables.forEach(function(variable, i) {
          // Skip the last column (WKT literal) and the ?image column (if it
          // exists).
          if (i == geometry_column ||
            variable == "?image" || variable == "?flag" || variable.endsWith("_image")) return;

          // Take the variable name as one table column and the result value as
          // another. Reformat a bit, so that it looks nice in an HTML table. and
          // the result value as another. Reformat a bit, so that it looks nice in
          // an HTML table.
          let key = variable.substring(1);
          if (row[i] == null) { row[i] = "---" }
          let value = row[i].replace(/\\([()])/g, "$1")
                        .replace(/<((.*)\/(.*))>/,
                         "<a class=\"link\" href=\"$1\" target=\"_blank\">$3</a>")
                        .replace(/\^\^.*$/, "")
                        .replace(/\"(.*)\"(@[a-z]+)?$/, "$1");

          popup_content_strings.push(
            "<tr>" + (i == 0 ? image_cell : "") +
            "<td>" + key.replace(/_/g, " ") + "</td>" +
            "<td>" + value + "</td></tr>");
        })
        let popup_html = "<table class=\"popup\">" + popup_content_strings.join("\n") + "</table>";
        popup_html += '<a class="export-link" href="geojson?gid=' + data[0].id + "&id=" + sessionId + '&rad=0&export=1">Export as GeoJSON</a>';

        if (curGeojson) curGeojson.remove();

        L.popup({"maxWidth" : 600})
            .setLatLng(data[0]["ll"])
            .setContent(popup_html)
            .openOn(map)
            .on('remove', function() {
                curGeojson.remove();
                curGeojsonId = -1;
            });

        curGeojson = getGeoJsonLayer(data[0].geom);
        curGeojsonId = data[0].id;
        curGeojson.addTo(map);
    }
}

function getGeoJsonLayer(geom) {
	const color = "#e6930e";
    return L.geoJSON(geom, {
			style: {color : color, fillColor: color, weight: 7, fillOpacity: 0.2},
            pointToLayer: function (feature, latlng) {
                return L.circleMarker(latlng, {
                    radius: 8,
                    fillColor: color,
                    color: color,
                    weight: 4,
                    opacity: 1,
                    fillOpacity: 0.2
                });}
            });
}

function showError(msg) {
    msg = msg.toString();
    document.getElementById("msg").style.display = "block";
    document.getElementById("msg-info").style.display = "none";
    document.getElementById("load").style.display = "none";
    const heading = document.getElementById("msg-heading");
    const error = document.getElementById("msg-error");
    heading.style.color = "red";
    heading.style.fontSize = "20px";
    heading.innerHTML = msg.split("\n")[0];
    if (msg.search("\n") > 0) error.innerHTML = "<pre>" + msg.substring(msg.search("\n")) + "</pre>";
    else error.innerHTML = "";
}

function loadMap(id, bounds, numObjects, autoThreshold) {
    const ll = L.Projection.SphericalMercator.unproject({"x": bounds[0][0], "y":bounds[0][1]});
    const ur =  L.Projection.SphericalMercator.unproject({"x": bounds[1][0], "y":bounds[1][1]});
    const boundsLatLng = [[ll.lat, ll.lng], [ur.lat, ur.lng]];
    map.fitBounds(boundsLatLng);
    sessionId = id;

    document.getElementById("stats").innerHTML = "<span>Showing " + numObjects.toLocaleString('en') + (numObjects > 1 ? " objects" : " object") + "</span>";

	const heatmapLayer = L.nonTiledLayer.wms('heatmap', {
        minZoom: 0,
        maxZoom: 19,
        opacity: 0.8,
        layers: id,
		styles: ["heatmap"],
        format: 'image/png',
        transparent: true,
    });

	const objectsLayer = L.nonTiledLayer.wms('heatmap', {
        minZoom: 0,
        maxZoom: 19,
        opacity: 0.9,
        layers: id,
        styles: ["objects"],
        format: 'image/png'
    });

    const autoHeatmapLayer = L.nonTiledLayer.wms('heatmap', {
        minZoom: 0,
        maxZoom: 15,
        opacity: numObjects > autoThreshold ? 0.8 : 0.9,
        layers: id,
        styles: numObjects > autoThreshold ? ["heatmap"] : ["objects"],
        format: 'image/png',
        transparent: true,
    });

    const autoObjectLayer = L.nonTiledLayer.wms('heatmap', {
        minZoom: 16,
        maxZoom: 19,
        opacity: 0.9,
        layers: id,
        styles: ["objects"],
        format: 'image/png'
    });
	const autoLayerGroup = L.layerGroup([autoHeatmapLayer, autoObjectLayer]);

    heatmapLayer.on('load', _onLayerLoad);
    objectsLayer.on('load', _onLayerLoad);
    autoHeatmapLayer.on('load', _onLayerLoad);
    autoObjectLayer.on('load', _onLayerLoad);

	layerControl.addBaseLayer(heatmapLayer, "Heatmap");
	layerControl.addBaseLayer(objectsLayer, "Objects");
	layerControl.addBaseLayer(autoLayerGroup, "Auto");

    if (mode == "heatmap") {
        heatmapLayer.addTo(map).on('error', function() {showError(genError);});
    } else if (mode == "objects") {
        objectsLayer.addTo(map).on('error', function() {showError(genError);});
    } else {
        autoLayerGroup.addTo(map).on('error', function() {showError(genError);});
    }

    map.on('click', function(e) {
        const pos = L.Projection.SphericalMercator.project(e.latlng);

        const w = map.getPixelBounds().max.x - map.getPixelBounds().min.x;
        const h = map.getPixelBounds().max.y - map.getPixelBounds().min.y;

        const sw = L.Projection.SphericalMercator.project((map.getBounds().getSouthWest()));
        const ne = L.Projection.SphericalMercator.project((map.getBounds().getNorthEast()));

        const bounds = [sw.x, sw.y, ne.x, ne.y];

        let styles = "objects";
        if (map.hasLayer(heatmapLayer)) styles = "heatmap";
        if (map.hasLayer(objectsLayer)) styles = "objects";

        fetch('pos?x=' + pos.x + "&y=" + pos.y + "&id=" + id + "&rad=" + (100 * Math.pow(2, 14 - map.getZoom())) + '&width=' + w + '&height=' + h + '&bbox=' + bounds.join(',') + '&styles=' + styles)
          .then(response => {
              if (!response.ok) return response.text().then(text => {throw new Error(text)});
              return response.json();
            })
          .then(data => openPopup(data))
          .catch(error => showError(error));
        });

    map.on('zoomend', function(e) {
        if (curGeojsonId > -1) {
            fetch('geojson?gid=' + curGeojsonId + "&id=" + id + "&rad=" + (100 * Math.pow(2, 14 - map.getZoom())))
              .then(response => response.json())
              .then(function(data) {
                curGeojson.remove();
                curGeojson = getGeoJsonLayer(data);
                curGeojson.addTo(map);
              })
              .catch(error => showError(genError));
        }
    });
}

function updateLoad(stage, percent, totalProgress, currentProgress) {
    const infoElem = document.getElementById("msg-info");
    const infoHeadingElem = document.getElementById("msg-info-heading");
    const infoDescElem = document.getElementById("msg-info-desc");
    const stageElem = document.getElementById("load-stage");
    const barElem = document.getElementById("load-bar");
    const percentElem = document.getElementById("load-percent");
    switch (stage) {
        case 1:
            infoHeadingElem.innerHTML = "Filling the geometry cache";
            infoDescElem.innerHTML = "This needs to be done only once for each new version of the dataset and does not have to be repeated for subsequent queries.";
            stageElem.innerHTML = `Parsing ${currentProgress}/${totalProgress} geometries... (1/2)`;
            document.getElementById("load-status").style.display = "grid";
            break;
        case 2:
            infoHeadingElem.innerHTML = "Filling the geometry cache";
            infoDescElem.innerHTML = "This needs to be done only once for each new version of the dataset and does not have to be repeated for subsequent queries.";
            stageElem.innerHTML = `Fetching ${currentProgress}/${totalProgress} geometries... (2/2)`;
            document.getElementById("load-status").style.display = "grid";
            break;
        case 3:
            infoHeadingElem.innerHTML = "Reading cached geometries from disk";
            infoDescElem.innerHTML = "This needs to be done only once after the server has been started and does not have to be repeated for subsequent queries.";
            stageElem.innerHTML = `Reading ${currentProgress}/${totalProgress} objects from disk... (1/1)`;
            document.getElementById("load-status").style.display = "grid";
            break;
        case 4:
            infoHeadingElem.innerHTML = "Fetching query result...";
            infoDescElem.innerHTML = "";
            stageElem.innerHTML = "";
            document.getElementById("load-status").style.display = "none";
            break;
    }
    barElem.style.width = percent + "%";
    percentElem.innerHTML = percent.toString() + "%";
    infoElem.style.display = "block";
}

function fetchResults() {
    fetch('query' + window.location.search)
    .then(response => {
        if (!response.ok) return response.text().then(text => {throw new Error(text)});
        return response;
        })
    .then(response => response.json())
    .then(data => {
        loadMap(data["qid"], data["bounds"], data["numobjects"], data["autothreshold"]);
    })
    .catch(error => showError(error));
}

function fetchLoadStatusInterval(interval) {
    fetchLoadStatus();
    loadStatusIntervalId = setInterval(fetchLoadStatus, interval);
}

async function fetchLoadStatus() {
    fetch('loadstatus?backend=' + qleverBackend)
    .then(response => {
        if (!response.ok) return response.text().then(text => {throw new Error(text)});
        return response;
    })
    .then(response => response.json())
    .then(data => {
        var stage = data["stage"];
        var percent = parseFloat(data["percent"]).toFixed(2);
        var totalProgress = data["totalProgress"].toLocaleString('en');
        var currentProgress = data["currentProgress"].toLocaleString('en');
        updateLoad(stage, percent, totalProgress, currentProgress);
    })
    .catch(error => {
        showError(error);
        clearInterval(loadStatusIntervalId);
    });
}

fetchResults();
fetchLoadStatusInterval(333);

function _onLayerLoad(e) {
    clearInterval(loadStatusIntervalId);
    document.getElementById("msg").style.display = "none";
}

document.getElementById("ex-geojson").onclick = function() {
    if (!sessionId) return;
    let a = document.createElement("a");
    a.href = "export?id="+ sessionId;
    a.setAttribute("download", "export.json");
    a.click();
}

document.getElementById("ex-tsv").onclick = function() {
    let a = document.createElement("a");
    a.href = qleverBackend + "?query=" + encodeURIComponent(query) + "&action=tsv_export";
    a.setAttribute("download", "export.tsv");
    a.click();
}

document.getElementById("ex-csv").onclick = function() {
    let a = document.createElement("a");
    a.href = qleverBackend + "?query=" + encodeURIComponent(query) + "&action=csv_export";
    a.setAttribute("download", "export.csv");
    a.click();
}
