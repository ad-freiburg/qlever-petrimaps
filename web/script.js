let sessionId = "";
let curGeojson = null;
let curGeojsonId = -1;
let urlParams = new URLSearchParams(window.location.search);

// id of SetInterval to stop loadStatus requests on error or load finish
let loadStatusIntervalId = -1;

// tabName of current submit-menu tab
let tabName = "";

// Map
let map;
let layerControl;
let osmLayer;
let heatmapLayer;
let objectsLayer;
let autoLayerHeatmap;
let autoLayerObjects;
let autoLayer;
let firstMapUpdate = true;
let baseLayers = {} // Match Layer name to layer
let selectedBaseLayerName = "" // Selected base layer name before map update
let selectedBackendElem = null;

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
        // If one of the first to last variables is called "?image" or ends in
        // "_image", then show an image with that URL in the first column of the
        // table. Note that we compute the cell contents here and add it during the
        // loop (it has to be the first cell of a table row).
        let image_cell = "";
        let image_column = -1;
        for (let i = 0; i < select_variables.length; i++) {
            if (select_variables[i] == "?image" ||
                select_variables[i] == "?flag" ||
                select_variables[i].endsWith("_image")) {
                    image_column = i;
                    break;
            }
        }
        if (image_column != -1) {
            let image_url = row[image_column];
            if (image_url != null)
                image_cell = "<td rowspan=\"0\" style=\"width:212px\"><a target=\"_blank\" href=\""
                            + image_url.replace(/^[<"]/, "").replace(/[>"]$/, "") + "\"><img src=\""
                            + image_url.replace(/^[<"]/, "").replace(/[>"]$/, "")
                            + "\"></a></td>";
        }

        // Now compute the table rows in an array.
        let popup_content_strings = [];
        select_variables.forEach(function(variable, i) {
            // Filter out WKT literals and images
            if (row[i].includes("wkt") || variable == "?image" || variable == "?flag" || variable.endsWith("_image")) return;

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
                "<td>" + value + "</td></tr>"
            );
        })
        let popup_html = "<table class=\"popup\" style=\"width:100%\">" + popup_content_strings.join("\n") + "</table>";
        popup_html += '<a class="export-link" href="geojson?gid=' + data[0].id + "&id=" + sessionId + '&rad=0&export=1">Export as GeoJSON</a>';

        if (curGeojson) {
            curGeojson.remove();
        }

        L.popup({"maxWidth" : 600})
            .setLatLng(data[0]["ll"])
            .setContent(popup_html)
            .openOn(map)
            .on('remove', function() {
                curGeojson.remove();
                curGeojsonId = -1;
            });

        console.log(data[0].geom);

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
            });
        }
    });
}

function showError(error) {
    error = error.toString();
    document.getElementById("msg").style.display = "block";
    document.getElementById("msg-info").style.display = "none";
    document.getElementById("msg-load").style.display = "none";
    document.getElementById("msg-heading").style.color = "red";
    document.getElementById("msg-heading").style.fontSize = "20px";
    document.getElementById("msg-heading").innerHTML = error.split("\n")[0];
    if (error.search("\n") > 0) document.getElementById("msg-error").innerHTML = "<pre>" + error.substring(error.search("\n")) + "</pre>";
    else document.getElementById("msg-error").innerHTML = "";
}

function initMap() {
    map = L.map('m', {
        renderer: L.canvas(),
        preferCanvas: true
    }).setView([47.9965, 7.8469], 13);
    map.attributionControl.setPrefix('University of Freiburg');
    map.on('baselayerchange', onMapBaseLayerChange);
    map.on('click', onMapClick);
    map.on('zoomend', onMapZoomEnd);
    layerControl = L.control.layers([], [], {collapsed:true, position: 'topleft'});

    osmLayer = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a rel="noreferrer" target="_blank" href="#">OpenStreetMap</a>',
        maxZoom: 19,
        opacity:0.9
    });
    osmLayer.addTo(map);
}

function updateMap() {
    if (!firstMapUpdate) {
        heatmapLayer.remove();
        objectsLayer.remove();
        autoLayer.remove();
        layerControl.removeLayer(heatmapLayer);
        layerControl.removeLayer(objectsLayer);
        layerControl.removeLayer(autoLayer);
        layerControl.remove();

        map.closePopup();
        curGeojsonId = -1;
    }

    heatmapLayer = L.nonTiledLayer.wms('heatmap', {
        minZoom: 0,
        maxZoom: 19,
        opacity: 0.8,
        layers: sessionId,
		styles: ["heatmap"],
        format: 'image/png',
        transparent: true,
    });
    objectsLayer = L.nonTiledLayer.wms('heatmap', {
        minZoom: 0,
        maxZoom: 19,
        layers: sessionId,
        styles: ["objects"],
        format: 'image/png'
    });
    autoLayerHeatmap = L.nonTiledLayer.wms('heatmap', {
        minZoom: 0,
        maxZoom: 15,
        opacity: 0.8,
        layers: sessionId,
        styles: ["heatmap"],
        format: 'image/png',
        transparent: true,
    });
    autoLayerObjects = L.nonTiledLayer.wms('heatmap', {
        minZoom: 16,
        maxZoom: 19,
        layers: sessionId,
        styles: ["objects"],
        format: 'image/png'
    });
    autoLayer = L.layerGroup([autoLayerHeatmap, autoLayerObjects]);

    heatmapLayer.on('error', function() {showError(genError);});
	objectsLayer.on('error', function() {showError(genError);});
    autoLayerHeatmap.on('error', function() {showError(genError);});
    autoLayerObjects.on('error', function() {showError(genError);});
	//heatmapLayer.on('load', function() {console.log("Finished loading map!");});
	//objectsLayer.on('load', function() {console.log("Finished loading map!");});
    //autoLayerHeatmap.on('load', function() {console.log("Finished loading map!");});
	//autoLayerObjects.on('load', function() {console.log("Finished loading map!");});

    layerControl.addBaseLayer(heatmapLayer, "Heatmap");
	layerControl.addBaseLayer(objectsLayer, "Objects");
	layerControl.addBaseLayer(autoLayer, "Auto");
    layerControl.addTo(map);
    baseLayers["Heatmap"] = heatmapLayer;
    baseLayers["Objects"] = objectsLayer;
    baseLayers["Auto"] = autoLayer;

    if (firstMapUpdate) {
        // Select default layer: Auto
        autoLayer.addTo(map);
    } else {
        // Restore previously selected layer
        baseLayers[selectedBaseLayerName].addTo(map);
    }

    firstMapUpdate = false;
}

function loadMap(id, bounds, numObjects) {
    document.getElementById("msg").style.display = "none";
    document.getElementById("stats").innerHTML = "<span>Showing " + numObjects + " objects</span>";

    sessionId = id;
    updateMap();

    const ll = L.Projection.SphericalMercator.unproject({"x": bounds[0][0], "y":bounds[0][1]});
    const ur =  L.Projection.SphericalMercator.unproject({"x": bounds[1][0], "y":bounds[1][1]});
    const boundsLatLng = [[ll.lat, ll.lng], [ur.lat, ur.lng]];
    map.fitBounds(boundsLatLng);

    document.getElementById("options-ex").style.display = "inline-block";
}

function updateLoad(stage, percent, totalProgress, currentProgress) {
    const infoElem = document.getElementById("msg-info");
    const infoHeadingElem = document.getElementById("msg-info-heading");
    const infoDescElem = document.getElementById("msg-info-desc");
    const stageElem = document.getElementById("msg-load-stage");
    const barElem = document.getElementById("msg-load-bar");
    const percentElem = document.getElementById("msg-load-percent");
    switch (stage) {
        case 1:
            infoHeadingElem.innerHTML = "Filling the geometry cache";
            infoDescElem.innerHTML = "This needs to be done only once for each new version of the dataset and does not have to be repeated for subsequent queries.";
            stageElem.innerHTML = `Parsing ${currentProgress}/${totalProgress} geometries... (1/2)`;
            break;
        case 2:
            infoHeadingElem.innerHTML = "Filling the geometry cache";
            infoDescElem.innerHTML = "This needs to be done only once for each new version of the dataset and does not have to be repeated for subsequent queries.";
            stageElem.innerHTML = `Fetching ${currentProgress}/${totalProgress} geometries... (2/2)`;
            break;
        case 3:
            infoHeadingElem.innerHTML = "Reading cached geometries from disk";
            infoDescElem.innerHTML = "This needs to be done only once after the server has been started and does not have to be repeated for subsequent queries.";
            stageElem.innerHTML = `Reading ${currentProgress}/${totalProgress} geometries from disk... (1/1)`;
            break;
    }
    barElem.style.width = percent + "%";
    percentElem.innerHTML = percent.toString() + "%";
    infoElem.style.display = "block";
}

function fetchQuery(query, backend) {
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

    const url = "query?query=" + query_encoded + "&backend=" + backend_encoded;
    fetchResults(url);
    fetchLoadStatusInterval(1000, backend_encoded);
}

function fetchGeoJsonHash(content) {
    // Fetch MD5-Hash of GeoJson-File content
    fetch("geoJsonHash", {
        method: "POST",
        body: "geoJsonFile=" + content,
        headers: {
            "Content-type": "application/json; charset=UTF-8"
        }
    })
    .then((response) => response.text())
    .then(md5_hash => {
        fetchGeoJsonFile(md5_hash);
    })
    .catch(error => showError(error));
}

function fetchGeoJsonFile(md5_hash) {
    // Fetch data using MD5-Hash
    fetch("geoJsonFile", {
        method: "POST",
        body: "geoJsonHash=" + md5_hash,
        headers: {
            "Content-type": "application/json; charset=UTF-8"
        }
    })
    .then(response => response.json())
    .then(data => {
        clearInterval(loadStatusIntervalId);
        document.getElementById("submit-button").disabled = false;
        loadMap(data["qid"], data["bounds"], data["numobjects"]);
    })
    .catch(error => {
        clearInterval(loadStatusIntervalId);
        document.getElementById("submit-button").disabled = false;
        showError(error);
    });

    setSubmitMenuVisible(false);
    document.getElementById("submit-button").disabled = true;
    document.getElementById("msg").style.display = "block";

    fetchLoadStatusInterval(1000, md5_hash);
}

function fetchResults(url) {
    setSubmitMenuVisible(false);
    document.getElementById("submit-button").disabled = true;

    fetch(url)
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
    .catch(error => {
        clearInterval(loadStatusIntervalId);
        document.getElementById("submit-button").disabled = false;
        showError(error);
    });

    document.getElementById("msg").style.display = "block";
}

function fetchLoadStatusInterval(interval, source) {
    fetchLoadStatus();
    loadStatusIntervalId = setInterval(fetchLoadStatus, interval, source);
    document.getElementById("msg-load").style.display = "block";
}

async function fetchLoadStatus(source) {
    fetch('loadstatus?source=' + source)
    .then(response => {
        if (!response.ok) {
            return response.text().then(text => {throw new Error(text)});
        }
        return response;
        })
    .then(response => response.json())
    .then(data => {
        const stage = data["stage"];
        const percent = parseFloat(data["percent"]).toFixed(2);
        const totalProgress = data["totalProgress"];
        const currentProgress = data["currentProgress"];
        updateLoad(stage, percent, totalProgress, currentProgress);
    })
    .catch(error => {
        clearInterval(loadStatusIntervalId);
        document.getElementById("submit-button").disabled = false;
        showError(error);
    });
}

function fileToText(file) {
    document.getElementById("submit-geoJsonFile-load").style.display = "block";

    const reader = new FileReader();
    reader.readAsText(file, "UTF-8");
    reader.onprogress = fileToTextOnProgress;
    reader.onload = fileToTextOnLoad;
    reader.onerror = fileToTextOnError;
}

function fileToTextOnProgress(evt) {
    const barElem = document.getElementById("submit-geoJSonFile-load-bar");
    const percentElem = document.getElementById("submit-geoJsonFile-load-percent");
    const loaded = evt.loaded;
    const total = evt.total;
    const percent = loaded / total * 100.0;

    barElem.style.width = percent + "%";
    percentElem.innerHTML = percent.toFixed(2) + "%";
}

function fileToTextOnLoad(evt) {
    document.getElementById("submit-geoJsonFile-load").style.display = "none";

    let content = evt.target.result;
    content = encodeURIComponent(content);
    fetchGeoJsonHash(content);
}

function fileToTextOnError(evt) {
    console.error("File Loading Error:", evt);
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

const queryElem = document.getElementById("query");
$(document).ready(function () {
    initMap();

    // Focus default submit tab
    document.getElementById("submit-tabs-default_open").click();
    selectedBackendElem = document.getElementById("backend-wikidata");

    // Process params in URL
    if (urlParams.has("query")) {
        const query = urlParams.get("query");
        editor.getDoc().setValue(query);
    }
    if (urlParams.has("backend")) {
        const backend = urlParams.get("backend");
        const dropdownOptionsElem = document.getElementById("dropdown-options");
        let listItems = dropdownOptionsElem.getElementsByTagName("li");
        for (const listItem of listItems) {
            const dataUrl = listItem.getAttribute("data-url");
            if (backend === dataUrl) {
                const id = listItem.getAttribute("id");
                onBackendSelected(id);
                break;
            }
        }
    }
    if (urlParams.has("query") && urlParams.has("backend")) {
        // User wants to send a SPARQL query
        const query = urlParams.get("query");
        const backend = urlParams.get("backend");
        fetchQuery(query, backend);
    } else {
        // No useful information in url => Show submit menu
        setSubmitMenuVisible(true);
    }

    // Fix query editor code lines overlapping with code
    // According to https://github.com/mdn/bob/issues/976 this is fixed in CodeMirror v6
    editor.refresh();
});

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

function onClickSubmitButton() {
    switch (tabName) {
        case "query":
            const query = editor.getDoc().getValue();
            const backend = selectedBackendElem.getAttribute("data-url")
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

function onMapClick(event) {
    const ll = event.latlng;
    const pos = L.Projection.SphericalMercator.project(ll);

    const w = map.getPixelBounds().max.x - map.getPixelBounds().min.x;
    const h = map.getPixelBounds().max.y - map.getPixelBounds().min.y;

    const sw = L.Projection.SphericalMercator.project((map.getBounds().getSouthWest()));
    const ne = L.Projection.SphericalMercator.project((map.getBounds().getNorthEast()));

    const bounds = [sw.x, sw.y, ne.x, ne.y];

    // ToDo: Choose this by selectedBaseLayerName
    let styles = "objects";
    if (map.hasLayer(heatmapLayer)) styles = "heatmap";
    if (map.hasLayer(objectsLayer)) styles = "objects";

    fetch('pos?x=' + pos.x + "&y=" + pos.y + "&id=" + sessionId + "&rad=" + (100 * Math.pow(2, 14 - map.getZoom())) + '&width=' + w + '&height=' + h + '&bbox=' + bounds.join(',') + '&styles=' + styles)
    .then(response => {
        if (!response.ok) return response.text().then(text => {throw new Error(text)});
            return response.json();
        })
    .then(data => openPopup(data))
    .catch(error => showError(error));
}

function onMapZoomEnd(event) {
    if (curGeojsonId > -1) {
        fetch('geojson?gid=' + curGeojsonId + "&id=" + sessionId + "&rad=" + (100 * Math.pow(2, 14 - map.getZoom())))
        .then(response => response.json())
        .then(function(data) {
            curGeojson.remove();
            curGeojson = getGeoJsonLayer(data);
            curGeojson.addTo(map);
        })
        .catch(error => showError(genError));
    }
}

function onMapBaseLayerChange(event) {
    selectedBaseLayerName = event.name;
}

function onBackendSelected(id) {
    const elemId = "backend-" + id;
    const ids = [selectedBackendElem.id, elemId];
    for (let i = 0; i < 2; i++) {
        const isSelected = i == 1;
        const curId = ids[i];
        const curElem = document.getElementById(curId);
        let text = "";
        if (isSelected) text += '<i class="glyphicon glyphicon-ok"></i> ';
        text += curElem.getAttribute("data-text");
        curElem.innerHTML = text;
    }

    selectedBackendElem = document.getElementById(elemId);
    const dropdownButtonTextElem = document.getElementById("dropdown-button-text");
    dropdownButtonTextElem.innerHTML = selectedBackendElem.getAttribute("data-text");
}
