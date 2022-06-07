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
        var content = "<table>";


        for (var i in data[0]["attrs"]) {
            content += "<tr>";
            content += "<td style='white-space:nowrap;'>" + data[0]["attrs"][i][0] + "</td><td>" + trimStr(data[0]["attrs"][i][1].replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')) + "</td>";
            content += "</tr>";
        }

        content += "</table>";

        L.popup({"maxWidth" : 600})
            .setLatLng(data[0]["ll"])
            .setContent(content)
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
