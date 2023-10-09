let sessionId;
let curGeojson;
let curGeojsonId = -1;

// id of SetInterval to stop loadStatus requests on error or load finish
let loadStatusIntervalId = -1;

// tabName of current submit tab
let tabName = "";

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

function showError(error) {
    error = error.toString();
    document.getElementById("msg").style.display = "block";
    document.getElementById("load").style.display = "none";
    document.getElementById("msg-inner").style.color = "red";
    document.getElementById("msg-inner").style.fontSize = "20px";
    document.getElementById("msg-inner").innerHTML = error.split("\n")[0];
    if (error.search("\n") > 0) document.getElementById("msg-inner-desc").innerHTML = "<pre>" + error.substring(error.search("\n")) + "</pre>";
    else document.getElementById("msg-inner-desc").innerHTML = "";
}

function loadMap(id, bounds, numObjects) {
    document.getElementById("msg").style.display = "none";
    
    const ll = L.Projection.SphericalMercator.unproject({"x": bounds[0][0], "y":bounds[0][1]});
    const ur =  L.Projection.SphericalMercator.unproject({"x": bounds[1][0], "y":bounds[1][1]});
    const boundsLatLng = [[ll.lat, ll.lng], [ur.lat, ur.lng]];
    map.fitBounds(boundsLatLng);
    sessionId = id;

    document.getElementById("stats").innerHTML = "<span>Showing " + numObjects + " objects</span>";

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
        layers: id,
        styles: ["objects"],
        format: 'image/png'
    });

	const autoLayer = L.layerGroup([
        L.nonTiledLayer.wms('heatmap', {
            minZoom: 0,
            maxZoom: 15,
            opacity: 0.8,
            layers: id,
            styles: ["heatmap"],
            format: 'image/png',
            transparent: true,
        }), L.nonTiledLayer.wms('heatmap', {
            minZoom: 16,
            maxZoom: 19,
            layers: id,
            styles: ["objects"],
            format: 'image/png'
        })
    ]);

	heatmapLayer.on('error', function() {showError(genError);});
	objectsLayer.on('error', function() {showError(genError);});
	heatmapLayer.on('load', function() {console.log("Finished loading map!");});
	objectsLayer.on('load', function() {console.log("Finished loading map!");});
	autoLayer.addTo(map).on('error', function() {showError(genError);});

	layerControl.addBaseLayer(heatmapLayer, "Heatmap");
	layerControl.addBaseLayer(objectsLayer, "Objects");
	layerControl.addBaseLayer(autoLayer, "Auto");

    map.on('click', function(e) {
        const ll = e.latlng;
        const pos = L.Projection.SphericalMercator.project(ll);

        const w = map.getPixelBounds().max.x - map.getPixelBounds().min.x;
        const h = map.getPixelBounds().max.y - map.getPixelBounds().min.y;

        const sw = L.Projection.SphericalMercator.project((map.getBounds().getSouthWest()));
        const ne = L.Projection.SphericalMercator.project((map.getBounds().getNorthEast()));

        const bounds = [sw.x, sw.y, ne.x, ne.y];

        let styles = "objects";
        if (map.hasLayer(heatmapLayer)) styles = "heatmap";
        if (map.hasLayer(objectsLayer)) styles = "objects";
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

    document.getElementById("options-ex").style.display = "inline-block";
}

function updateLoad(stage, percent) {
    const stageElem = document.getElementById("load-stage");
    const barElem = document.getElementById("load-bar");
    const percentElem = document.getElementById("load-percent");
    switch (stage) {
        case 1:
            stageElem.innerHTML = "Parsing geometry... (1/2)";
            break;
        case 2:
            stageElem.innerHTML = "Parsing geometry Ids... (2/2)";
            break;
    }
    barElem.style.width = percent + "%";
    percentElem.innerHTML = percent.toString() + "%";
}

function fetchQuery(query, backend) {
    console.log("Fetching query...");

    const query_encoded = encodeURIComponent(query);
    const backend_encoded = encodeURIComponent(backend);

    document.getElementById("options-ex-tsv").onclick = function() {
        let a = document.createElement("a");
        a.href = backend + "?query=" + query_encoded + "&action=tsv_export";
        a.setAttribute("download", "export.tsv");
        a.click();
    }
    
    document.getElementById("options-ex-csv").onclick = function() {
        let a = document.createElement("a");
        a.href = backend + "?query=" + query_encoded + "&action=csv_export";
        a.setAttribute("download", "export.csv");
        a.click();
    }

    const url = "?query=" + query_encoded + "&backend=" + backend_encoded;
    fetchResults(url);
    fetchLoadStatusInterval(1000, backend_encoded);
}

function fetchGeoJsonFile(content) {
    console.log("Fetching GeoJson-File");
    console.log(content)

    const url = "geoJsonFile=" + content;
}

function fetchResults(url) {
    setSubmitMenuVisible(false);
    document.getElementById("submit-button").disabled = true;

    fetch('query' + url)
    .then(response => {
        if (!response.ok) return response.text().then(text => {throw new Error(text)});
        return response;
        })
    .then(response => response.json())
    .then(data => {
        clearInterval(loadStatusIntervalId);
        document.getElementById("submit-button").disabled = false;
        loadMap(data["qid"], data["bounds"], data["numobjects"]);
    })
    .catch(error => showError(error));

    document.getElementById("msg").style.display = "block";
}

function fetchLoadStatusInterval(interval, backend) {
    fetchLoadStatus();
    loadStatusIntervalId = setInterval(fetchLoadStatus, interval, backend);
    document.getElementById("load").style.display = "block";
}

async function fetchLoadStatus(backend) {
    console.log("Fetching load status...");

    fetch('loadstatus?backend=' + backend)
    .then(response => {
        if (!response.ok) return response.text().then(text => {throw new Error(text)});
        return response;
        })
    .then(response => response.json())
    .then(data => {
        const stage = data["stage"];
        const percent = parseFloat(data["percent"]).toFixed(2);
        updateLoad(stage, percent);
    })
    .catch(error => {
        showError(error);
        clearInterval(loadStatusIntervalId);
    });
}

function fileToText(file) {
    const reader = new FileReader();
    reader.readAsText(file, "UTF-8");
    reader.onprogress = fileToTextOnProgress;
    reader.onload = fileToTextOnLoad;
    reader.onerror = fileToTextOnError;
}

function fileToTextOnProgress(evt) {
}

function fileToTextOnLoad(evt) {
    const content = evt.target.result;
    const content_encoded = encodeURIComponent(content);
    fetchGeoJsonFile(content_encoded);
}

function fileToTextOnError(evt) {
    console.log("File Loading Error:", evt);
}

function openTab(evt, tabName) {
    // Tab changing for submit tabs
    // Hide tabs
    tab_content_elems = document.getElementsByClassName("submit-tab_content");
    for (let i = 0; i < tab_content_elems.length; i++) {
        tab_content_elems[i].style.display = "none";
    }

    // Deactivate tabs
    tab_links_elems = document.getElementsByClassName("submit-tab_link");
    for (let i = 0; i < tab_links_elems.length; i++) {
        tab_links_elems[i].className = tab_links_elems[i].className.replace(" active", "");
    }

    // Show current tab
    document.getElementById("submit-" + tabName).style.display = "block";
    evt.currentTarget.className += " active";
    this.tabName = tabName;
}

function setSubmitMenuVisible(visible) {
    if (visible) {
        document.getElementById("submit").style.display = "block";
        document.getElementById("options-submit").innerHTML = "Hide Submit Menu";
    } else {
        document.getElementById("submit").style.display = "none";
        document.getElementById("options-submit").innerHTML = "Show Submit Menu";
    }
}

// Focus default submit tab
document.getElementById("submit-tabs-default_open").click();
const queryElem = document.getElementById("submit-query-query");
const backendElem = document.getElementById("submit-query-backend");
backendElem.value = "https://qlever.cs.uni-freiburg.de/api/wikidata";

const url = window.location.search;
const urlParams = new URLSearchParams(url);
if (urlParams.has("query")) {
    const query = urlParams.get("query");
    queryElem.value = query;
}
if (urlParams.has("backend")) {
    const backend = urlParams.get("backend");
    backendElem.value = backend;
}
if (urlParams.has("query") && urlParams.has("backend")) {
    // User wants to send a SPARQL query
    const query = urlParams.get("query");
    const backend = urlParams.get("backend");
    fetchQuery(query, backend);
} else if (urlParams.has("geoJsonFile")) {
    // User wants to send content of a .geojson file
    console.log("GeoJson file.");
} else {
    // No useful information in url => Show submit menu
    setSubmitMenuVisible(true);
}

document.getElementById("options-ex-geojson").onclick = function() {
    if (!sessionId) return;
    const a = document.createElement("a");
    a.href = "export?id="+ sessionId;
    a.setAttribute("download", "export.json");
    a.click();
}

document.getElementById("options-submit").onclick = function() {
    const isVisible = document.getElementById("submit").style.display == "block";
    setSubmitMenuVisible(!isVisible);
}

document.getElementById("submit-button").onclick = function() {
    switch (tabName) {
        case "query":
            const query = queryElem.value;
            const backend = backendElem.value;
            fetchQuery(query, backend);
            break;

        case "geoJsonFile":
            const fileElem = document.getElementById("submit-geoJsonFile-file");
            const file = fileElem.files[0];
            if (file) {
                fileToText(file);
            }
            break;
    }
}