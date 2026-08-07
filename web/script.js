// Copyright 2022-2026 University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Patrick Brosi <brosi@cs.uni-freiburg.de>

let sessionId;
let curGeojson;
let curGeojsonId = -1;
let curGeojsonLayer = "";

let currentTileConfig = null;

let urlParams = window.postParams;
let qleverBackend = urlParams["backend"];
let query = urlParams["query"];
let mode = urlParams["mode"];

const heatmapStyles = ["spectralexp", "spectral", "RdYlGn", "RdYlGnexp", "RdYlBu","RdYlBuexp", "w2b", "b2w", "RdGy","RdGyexp","YlOrRd","YlOrRdexp","Blues","Bluesexp","Greens","Greensexp","Greys","Greysexp","Oranges","Orangesexp","Reds", "Redsexp"];
const rasterStyles = ["spectral", "RdYlGn", "RdYlBu","RdGy","YlOrRd","Blues","Greens","Greys","Oranges","Reds"];

let fieldsRaw = (urlParams["fields"] || "").split(";");
let fields = [];

for (let fieldRaw of fieldsRaw) {
    let parts = fieldRaw.split(",");
    if (parts.length == 0) continue;
    fields.push({geo : parts[0], value : parts.length > 1 ? parts[1] : null});
}

// id of SetInterval to stop loadStatus requests on error or load finish
let loadStatusIntervalId = -1;

let map = L.map('m', {
    renderer: L.canvas(),
    preferCanvas: true
}).setView([47.9965, 7.8469], 3);
map.attributionControl.setPrefix('<a rel="noreferrer" target="_blank" href="//ad.cs.uni-freiburg.de">University of Freiburg</a>');

let osmLayer = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a rel="noreferrer" target="_blank" href="//openstreetmap.org">OpenStreetMap</a>',
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
        // let geometry_column = select_variables.length - 1;
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
            if (isGeometryVariable(variable) ||
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
            popup_html += '<a class="export-link" href="' + getWfsExportUrl(data[0]) + '">Export via GeoJSON</a>';
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
        curGeojsonLayer = data[0].geomfield;
        curGeojson.addTo(map);
    }
}

function isGeometryVariable(variable) {
    return variable == "?geometry" ||
           variable == "?geom" ||
           variable == "?wkt" ||
           variable.endsWith("geometry") ||
           variable.endsWith("geom") ||
           variable.endsWith("wkt");
}

function popupAttributePriority(variable) {
    if (variable == "?country") return 0;
    if (variable == "?countryLabel" || variable == "?countryName") return 1;
    if (variable == "?name" || variable == "?label" || variable == "?placelabel") return 2;
    return 10;
}

function getWfsExportUrl(feature) {
    let params = new URLSearchParams({
        service: "WFS",
        version: "2.0.0",
        request: "GetFeature",
        typeNames: sessionId + ":" + feature.geomfield,
        outputFormat: "application/json"
    });
    if (feature.id) params.gid = feature.id;
    return "wfs?" + params.toString();
}

function wfsFeatureCollectionToPopupData(data) {
    if (!data || data.type !== "FeatureCollection" || !data.features || data.features.length == 0) {
        return [];
    }

    const feature = data.features[0];
    const props = feature.properties || {};

    const attrs = Object.keys(props)
        .filter(key => !["id", "gid", "featureID", "geomfield", "popup_lat", "popup_lng"].includes(key))
        .filter(key => !isGeometryVariable(key))
        .map(key => [key, String(props[key])])
        .sort((a, b) => popupAttributePriority(a[0]) - popupAttributePriority(b[0]));
    return [{
        id: props.id,
        geomfield: props.geomfield,
        attrs: attrs,
        ll: {
            lat: parseFloat(props.popup_lat),
            lng: parseFloat(props.popup_lng)
        },
        geom: feature.geometry
    }];
}

function getGeoJsonLayer(geom) {
    const color = "#e6930e";
    return L.geoJSON(geom, {
        style: {color : color, fillColor: color, weight: 6, fillOpacity: 0.6},
        pointToLayer: function (feature, latlng) {
            return L.circleMarker(latlng, {
                radius: 8,
                fillColor: color,
                color: color,
                weight: 4,
                opacity: 1,
                fillOpacity: 0.6
            });}
    });
}

function showError(err) {
    msg = err.toString();
    document.getElementById("msg-info").style.display = "none";
    document.getElementById("load").style.display = "none";
    const heading = document.getElementById("msg-heading");
    const error = document.getElementById("msg-error");
    heading.style.color = "red";
    heading.style.fontSize = "20px";
    heading.innerHTML = msg.split("\n")[0];
    if (msg.search("\n") > 0) error.innerHTML = "<pre>" + msg.substring(msg.search("\n")) + "</pre>";
    else error.innerHTML = "";
    clearInterval(loadStatusIntervalId);
}

function loadLayers(sessionId, layers) {

    let groups = new Set();

    for (layer of layers) {
        if (layer["group"]) groups.add(layer["group"]);
    }

    let themes = {};

    if (groups.size) {
        for (group of groups) {
            themes[group] = {
                name: group,
                overlays: [{name:"", type:"radio", layers: []}, {name:"", type:"checkbox", layers: []}]
            }
        }
    }
    themes["default"] = {
        name: "Layers",
        overlays: [{name:"", type:"radio", layers: []}, {name:"", type:"checkbox", layers: []}]
    };

    for (layer of layers) {
        let theme = themes["default"];
        if (layer["group"]) theme = themes[layer["group"]];

        let prepedLayer = getLayer(sessionId, layer);
        if (prepedLayer) {
            prepedLayer.layer = trackTileStyle(prepedLayer.layer, layer);
            prepedLayer.layer.on('load', _onLayerLoad);
            if (layer["toggle"] == "checkbox") {
                theme.overlays[1].layers.push(prepedLayer);
            } else {
                theme.overlays[0].layers.push(prepedLayer);
            }
        }
    }

    const themeControl = new L.Control.ThemeLayerSwitcher(themes, {
        position: 'topleft',
        defaultTheme: 'auto',
    });

    map.addControl(themeControl);
}

function trackTileStyle(layer, params) {
    layer.on("add", function() {
                currentTileConfig = {layerId: params["id"], geomField: params["geomfield"]};
            });
    return layer;
}

function getLayer(sessionId, layer) {
    return { name: layer["name"], layer: L.nonTiledLayer.wms('heatmap', {
        minZoom: 0,
        maxZoom: 19,
        opacity: 0.9,
        layers: sessionId + ":" + layer["geomfield"],
        styles: [layer["id"]],
        format: 'image/png'
    })};

    return null;
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
    fetch('query',
        {
            method: "POST",
            headers: {
                "Content-Type": "application/x-www-form-urlencoded"
            },
            body: new URLSearchParams(urlParams).toString()
        })
        .then(response => {
            if (!response.ok) return response.text().then(text => {throw new Error(text)});
            return response;
        })
        .then(response => response.json())
        .then(data => {

            const ll = L.Projection.SphericalMercator.unproject({"x": data["bounds"][0][0], "y":data["bounds"][0][1]});
            const ur =  L.Projection.SphericalMercator.unproject({"x": data["bounds"][1][0], "y":data["bounds"][1][1]});
            const boundsLatLng = [[ll.lat, ll.lng], [ur.lat, ur.lng]];
            map.fitBounds(boundsLatLng, {animate: false});
            sessionId = data["qid"];

            document.getElementById("stats").innerHTML = "<span>Showing " + data["numobjects"].toLocaleString('en') + (data["numobjects"] > 1 ? " objects" : " object") + "</span>";

            if (data["layers"].length == 0) {
                showError("No layers specified in config");
                clearInterval(loadStatusIntervalId);
            } else {
                loadLayers(data["qid"], data["layers"]);
            }

            let id = data["qid"];

            map.on('click', function(e) {
                const pos = L.Projection.SphericalMercator.project(e.latlng);

                const bounds = [pos.x, pos.y, pos.x, pos.y];

                fetch('wfs?service=WFS&version=2.0.0&request=GetFeature'
                    + '&typeNames=' + id + ":" + currentTileConfig.geomField
                    + '&rad=' + (100 * Math.pow(2, 14 - map.getZoom()))
                    + '&bbox=' + bounds.join(',')
                    + '&srsName=EPSG:3857'
                    + '&outputFormat=application/json')
                    .then(response => {
                        if (!response.ok) return response.text().then(text => {throw new Error(text)});
                        return response.json();
                    })
                    .then(data => openPopup(wfsFeatureCollectionToPopupData(data)))
                    .catch(error => showError(error));
            });

            map.on('zoomend', function(e) {
                if (curGeojsonId > -1) {
                    fetch('geojson?gid=' + curGeojsonId + "&id=" + id + "&layer=" + curGeojsonLayer + "&rad=" + (100 * Math.pow(2, 14 - map.getZoom())))
                        .then(response => response.json())
                        .then(function(data) {
                            curGeojson.remove();
                            curGeojson = getGeoJsonLayer(data);
                            curGeojson.addTo(map);
                        })
                        .catch(error => showError(genError));
                }
            });
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

document.getElementById("msg").classList.add("show");
fetchResults();
fetchLoadStatusInterval(333);

function _onLayerLoad(e) {
    clearInterval(loadStatusIntervalId);
    document.getElementById("msg").classList.remove("show");
}

function getVisibleTileLayerId(layerId) {
    return layerId.replace(/-([?$])/, "-");
}

function buildTileExportUrls() {
    if (!sessionId || !currentTileConfig) return null;

    const style = encodeURIComponent(currentTileConfig.style);
    const wfsTypeName = encodeURIComponent("session_" + sessionId);
    const layerId = currentTileConfig.layerId;

    const bounds = map.getBounds();
    const west = bounds.getWest();
    const south = bounds.getSouth();
    const east = bounds.getEast();
    const north = bounds.getNorth();
    const bbox = encodeURIComponent(`${west},${south},${east},${north}`);

    return {
        tms: `${window.location.origin}/tms/${sessionId}/${layerId}/{x}/{y}/{z}.png`,
        wmts: `${window.location.origin}/wmts?service=WMTS&request=GetTile&version=1.0.0&layer=${sessionId}&style=${layerId}&format=image/png&tilematrixset=WebMercatorQuad&tilematrix={z}&tilerow={y}&tilecol={x}`,
        wfs: `${window.location.origin}/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=${wfsTypeName}&layerid=${layerId}&bbox=${bbox}&srsName=EPSG:4326&outputFormat=application/json&count=100`
    };
}

function showTileDialog(urls) {
    document.getElementById("tms-code").textContent = urls.tms;
    document.getElementById("wmts-code").textContent = urls.wmts;
    document.getElementById("wfs-code").textContent = urls.wfs;
    document.getElementById("tile-subtitle").textContent =
        "Choose which export URL to copy.";
    document.getElementById("tile-modal").style.display = "flex";
}

function hideTileDialog() {
    document.getElementById("tile-modal").style.display = "none";
}

document.getElementById("ex-geojson").onclick = function() {
    if (!sessionId) return;
    let a = document.createElement("a");
    a.href = getWfsExportUrl({geomfield: currentTileConfig.geomField});
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

document.getElementById("ex-tile").onclick = function() {
    const urls = buildTileExportUrls();
    console.log(urls);
    if (!urls) return;
    showTileDialog(urls);
}

document.getElementById("tile-close").onclick = function() {
    hideTileDialog();
}

document.querySelectorAll(".tile-copy").forEach(function(button) {
    button.onclick = async function() {
        const targetId = button.getAttribute("data-copy-target");
        const url = document.getElementById(targetId).textContent;

        try {
            await navigator.clipboard.writeText(url);
            button.textContent = "Copied";
            setTimeout(function() {
                button.textContent = "Copy";
            }, 1200);
        } catch (e) {
            document.getElementById("tile-subtitle").textContent = 
                "Copy failed - please copy manually.";
        }
    };
});
