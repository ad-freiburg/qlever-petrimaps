var sessionId;

var map = L.map('m', {
    renderer: L.canvas(),
    maxZoom: 19
}).setView([47.9965, 7.8469], 13);

L.tileLayer('https://stamen-tiles-{s}.a.ssl.fastly.net/toner-lite/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a rel="noreferrer" target="_blank" href="#">OpenStreetMap</a>'
}).addTo(map);

function openPopup(data) {
    if (data.length > 0) {
        var content = "";


        for (var i in data[0]["attrs"]) {
            content += "<br>" + data[0]["attrs"][i][1].replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
        }

        L.popup()
            .setLatLng(data[0]["ll"])
            .setContent(content)
            .openOn(map);
    }
}

function loadMap(id, bounds) {
    console.log("Loading session " + id);
    var ll = L.Projection.SphericalMercator.unproject({"x": bounds[0][0], "y":bounds[0][1]});
    var ur =  L.Projection.SphericalMercator.unproject({"x": bounds[1][0], "y":bounds[1][1]});
    var bounds = [[ll.lat, ll.lng], [ur.lat, ur.lng]];
    map.fitBounds(bounds);
    sessionId = id;
    L.nonTiledLayer.wms('heatmap', {
        maxZoom: 19,
        minZoom: 0,
        opacity: 0.8,
        layers: id,
        format: 'image/png',
        transparent: true,
    }).addTo(map);

    map.on('click', function(e) {
        var ll= e.latlng;
        var pos = L.Projection.SphericalMercator.project(ll);

        fetch('/pos?x=' + pos.x + "&y=" + pos.y + "&id=" + id)
          .then(response => response.json())
          .then(data => openPopup(data));
        });
}

console.log("Loading data from QLever...");
fetch('/query' + window.location.search)
  .then(response => response.json())
  .then(data => loadMap(data["qid"], data["bounds"]));
