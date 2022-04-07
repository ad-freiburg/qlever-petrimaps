/*
 (c) 2014, Vladimir Agafonkin
 simpleheat, a tiny JavaScript library for drawing heatmaps with Canvas
 https://github.com/mourner/simpleheat
*/
!function(){"use strict";function t(i){return this instanceof t?(this._canvas=i="string"==typeof i?document.getElementById(i):i,this._ctx=i.getContext("2d"),this._width=i.width,this._height=i.height,this._max=1,void this.clear()):new t(i)}t.prototype={defaultRadius:25,defaultGradient:{.4:"blue",.6:"cyan",.7:"lime",.8:"yellow",1:"red"},data:function(t,i){return this._data=t,this},max:function(t){return this._max=t,this},add:function(t){return this._data.push(t),this},clear:function(){return this._data=[],this},radius:function(t,i){i=i||15;var a=this._circle=document.createElement("canvas"),s=a.getContext("2d"),e=this._r=t+i;return a.width=a.height=2*e,s.shadowOffsetX=s.shadowOffsetY=200,s.shadowBlur=i,s.shadowColor="black",s.beginPath(),s.arc(e-200,e-200,t,0,2*Math.PI,!0),s.closePath(),s.fill(),this},gradient:function(t){var i=document.createElement("canvas"),a=i.getContext("2d"),s=a.createLinearGradient(0,0,0,256);i.width=1,i.height=256;for(var e in t)s.addColorStop(e,t[e]);return a.fillStyle=s,a.fillRect(0,0,1,256),this._grad=a.getImageData(0,0,1,256).data,this},draw:function(t){this._circle||this.radius(this.defaultRadius),this._grad||this.gradient(this.defaultGradient);var i=this._ctx;i.clearRect(0,0,this._width,this._height);for(var a,s=0,e=this._data.length;e>s;s++)a=this._data[s],i.globalAlpha=Math.max(a[2]/this._max,t||.05),i.drawImage(this._circle,a[0]-this._r,a[1]-this._r);var n=i.getImageData(0,0,this._width,this._height);return this._colorize(n.data,this._grad),i.putImageData(n,0,0),this},_colorize:function(t,i){for(var a,s=3,e=t.length;e>s;s+=4)a=4*t[s],a&&(t[s-3]=i[a],t[s-2]=i[a+1],t[s-1]=i[a+2])}},window.simpleheat=t}(),/*
 (c) 2014, Vladimir Agafonkin
 Leaflet.heat, a tiny and fast heatmap plugin for Leaflet.
 https://github.com/Leaflet/Leaflet.heat
*/
L.HeatLayer=(L.Layer?L.Layer:L.Class).extend({initialize:function(t,i){this._latlngs=t,L.setOptions(this,i)},setLatLngs:function(t){return this._latlngs=t,this.redraw()},addLatLng:function(t){return this._latlngs.push(t),this.redraw()},setOptions:function(t){return L.setOptions(this,t),this._heat&&this._updateOptions(),this.redraw()},redraw:function(){return!this._heat||this._frame||this._map._animating||(this._frame=L.Util.requestAnimFrame(this._redraw,this)),this},onAdd:function(t){this._map=t,this._canvas||this._initCanvas(),t._panes.overlayPane.appendChild(this._canvas),t.on("moveend",this._reset,this),t.options.zoomAnimation&&L.Browser.any3d&&t.on("zoomanim",this._animateZoom,this),this._reset()},onRemove:function(t){t.getPanes().overlayPane.removeChild(this._canvas),t.off("moveend",this._reset,this),t.options.zoomAnimation&&t.off("zoomanim",this._animateZoom,this)},addTo:function(t){return t.addLayer(this),this},_initCanvas:function(){var t=this._canvas=L.DomUtil.create("canvas","leaflet-heatmap-layer leaflet-layer"),i=L.DomUtil.testProp(["transformOrigin","WebkitTransformOrigin","msTransformOrigin"]);t.style[i]="50% 50%";var a=this._map.getSize();t.width=a.x,t.height=a.y;var s=this._map.options.zoomAnimation&&L.Browser.any3d;L.DomUtil.addClass(t,"leaflet-zoom-"+(s?"animated":"hide")),this._heat=simpleheat(t),this._updateOptions()},_updateOptions:function(){this._heat.radius(this.options.radius||this._heat.defaultRadius,this.options.blur),this.options.gradient&&this._heat.gradient(this.options.gradient),this.options.max&&this._heat.max(this.options.max)},_reset:function(){var t=this._map.containerPointToLayerPoint([0,0]);L.DomUtil.setPosition(this._canvas,t);var i=this._map.getSize();this._heat._width!==i.x&&(this._canvas.width=this._heat._width=i.x),this._heat._height!==i.y&&(this._canvas.height=this._heat._height=i.y),this._redraw()},_redraw:function(){var t,i,a,s,e,n,h,o,r,d=[],_=this._heat._r,l=this._map.getSize(),m=new L.Bounds(L.point([-_,-_]),l.add([_,_])),c=void 0===this.options.max?1:this.options.max,u=void 0===this.options.maxZoom?this._map.getMaxZoom():this.options.maxZoom,f=1/Math.pow(2,Math.max(0,Math.min(u-this._map.getZoom(),12))),g=_/2,p=[],v=this._map._getMapPanePos(),w=v.x%g,y=v.y%g;for(t=0,i=this._latlngs.length;i>t;t++)if(a=this._map.latLngToContainerPoint(this._latlngs[t]),m.contains(a)){e=Math.floor((a.x-w)/g)+2,n=Math.floor((a.y-y)/g)+2;var x=void 0!==this._latlngs[t].alt?this._latlngs[t].alt:void 0!==this._latlngs[t][2]?+this._latlngs[t][2]:1;r=x*f,p[n]=p[n]||[],s=p[n][e],s?(s[0]=(s[0]*s[2]+a.x*r)/(s[2]+r),s[1]=(s[1]*s[2]+a.y*r)/(s[2]+r),s[2]+=r):p[n][e]=[a.x,a.y,r]}for(t=0,i=p.length;i>t;t++)if(p[t])for(h=0,o=p[t].length;o>h;h++)s=p[t][h],s&&d.push([Math.round(s[0]),Math.round(s[1]),Math.min(s[2],c)]);this._heat.data(d).draw(this.options.minOpacity),this._frame=null},_animateZoom:function(t){var i=this._map.getZoomScale(t.zoom),a=this._map._getCenterOffset(t.center)._multiplyBy(-i).subtract(this._map._getMapPanePos());L.DomUtil.setTransform?L.DomUtil.setTransform(this._canvas,a,i):this._canvas.style[L.DomUtil.TRANSFORM]=L.DomUtil.getTranslateString(a)+" scale("+i+")"}}),L.heatLayer=function(t,i){return new L.HeatLayer(t,i)};var widths = [12, 13, 13, 13, 13, 13, 13, 13];
var opas = [0.8, 0.6, 0.5, 0.5, 0.4];
var mwidths = [1, 1, 1, 1.5, 2, 3, 5, 6, 6, 4, 3, 2];
var stCols = ['#78f378', '#0000c3', 'red'];

var osmUrl = "//www.openstreetmap.org/";

var grIdx, stIdx, selectedRes, prevSearch, delayTimer;

var reqs = {};

var openedGr = -1;
var openedSt = -1;

var sgMvOrNew = "Move into a <span class='grouplink' onmouseover='grHl(${tid})' onmouseout='grUnHl(${tid})'>new relation</span> <tt>public_transport=stop_area</tt>.";
var sgMvOrEx = "Move into relation <a onmouseover='grHl(${tid})' onmouseout='grUnHl(${tid})' href=\"" + osmUrl + "relation/${toid}\" target=\"_blank\">${toid}</a>.";
var sgMvRelNew = "Move from relation <a onmouseover='grHl(${oid})' onmouseout='grUnHl(${oid})' href=\"" + osmUrl + "relation/${ooid}\" target=\"_blank\">${ooid}</a> into a <span class='grouplink' onmouseover='grHl(${tid})' onmouseout='grUnHl(${tid})'>new relation</span> <tt>public_transport=stop_area</tt>.";
var sgMvRelRel = "Move from relation <a onmouseover='grHl(${oid})' onmouseout='grUnHl(${oid})' href=\"" + osmUrl + "relation/${ooid}\" target=\"_blank\">${ooid}</a> into relation <a onmouseover='grHl(${tid})' onmouseout='grUnHl(${tid})' href=\"" + osmUrl + "relation/${toid}\" target=\"_blank\">${toid}</a>.";
var sgMvOutRel = "Move out of relation <a onmouseover='grHl(${oid})' onmouseout='grUnHl(${oid})' href=\"" + osmUrl + "relation/${ooid}\" target=\"_blank\">${ooid}</a>";
var sgFixAttr = "Fix attribute <tt>${attr}</tt>.";
var sgAddName = "Consider adding a <tt><a target='_blank' href='https://wiki.openstreetmap.org/wiki/Key:name'>name</a></tt> attribute.";
var sgAttrTr = "Attribute <tt>${attr}</tt> seems to be a track number. Use <tt>ref</tt> for this and set <tt>${attr}</tt> to the station name.";

var suggsMsg = [sgMvOrNew, sgMvOrEx, sgMvRelNew, sgMvRelRel, sgMvOutRel, sgFixAttr, sgAddName, sgAttrTr];

function $(a){return a[0] == "#" ? document.getElementById(a.substr(1)) : a[0] == "." ? document.getElementsByClassName(a.substr(1)) : document.getElementsByTagName(a)}
function $$(t){return document.createElement(t) }
function ll(g){return {"lat" : g[0], "lng" : g[1]}}
function hasCl(e, c){return e.className.split(" ").indexOf(c) != -1}
function addCl(e, c){if (!hasCl(e, c)) e.className += " " + c;e.className = e.className.trim()}
function delCl(e, c){var a = e.className.split(" "); delete a[a.indexOf(c)]; e.className = a.join(" ").trim()}
function stCol(s){return s.e ? stCols[2] : s.s ? stCols[1] : stCols[0]}
function tmpl(s, r){for (p in r) s = s.replace(new RegExp("\\${" + p + "}", "g"), r[p]); return s}
function req(id, u, cb) {
    if (reqs[id]) reqs[id].abort();
    reqs[id] = new XMLHttpRequest();
    reqs[id].onreadystatechange = function() { if (this.readyState == 4 && this.status == 200 && this == reqs[id]) cb(JSON.parse(this.responseText))};
    reqs[id].open("GET", u, 1);
    reqs[id].send();
}

function marker(stat, z) {
    if (stat.g.length == 1) {
        if (z > 15) {
            return L.circle(
                stat.g[0], {
                    color: '#000',
                    fillColor: stCol(stat),
                    radius: mwidths[23 - z],
                    fillOpacity: 1,
                    weight: z > 17 ? 1.5 : 1,
                    id: stat.i
                }
            );
        } else {
            return L.polyline(
                [stat.g[0], stat.g[0]], {
                    color: stCol(stat),
                    fillColor: stCol(stat),
                    weight: widths[15 - z],
                    opacity: opas[15 - z],
                    id: stat.i
                }
            );
        }
    } else {
        return L.polygon(
            stat.g, {
                color: z > 15 ? '#000': stCol(stat),
                fillColor: stCol(stat),
                smoothFactor: 0,
                fillOpacity: 0.75,
                weight: z > 17 ? 1.5 : 1,
                id: stat.i
            }
        );
    }
}

function poly(group, z) {
    var col = group.e ? 'red' : group.s ? '#0000c3' : '#85f385';
    var style = {
        color: col,
        fillColor: col,
        smoothFactor: 0.4,
        fillOpacity: 0.2,
        id: group.i
    };
    if (z < 16) {
        style.weight = 11;
        style.opacity = 0.5;
        style.fillOpacity = 0.5;
    }
    return L.polygon(group.g, style)
}

function sugArr(sug, z) {
    return L.polyline(sug.a, {
        id: sug.i,
        color: '#0000c3',
        smoothFactor: 0.1,
        weight: 4,
        opacity: 0.5
    });
}

function rndrSt(stat) {
    openedSt = stat.id;
    stHl(stat.id);
    var attrrows = {};

    var way = stat.osmid < 0;
    var osmid = Math.abs(stat.osmid);
    var ident = way ? "Way" : "Node";

    var con = $$('div');
    con.setAttribute("id", "nav")

    var suggD = $$('div');
    suggD.setAttribute("id", "sugg")

    con.innerHTML = ident + " <a target='_blank' href='" + osmUrl + ident.toLowerCase()+"/" + osmid + "'>" + osmid + "</a>";

    if (stat.attrs.name) con.innerHTML += " (<b>\"" + stat.attrs.name + "\"</b>)";

    con.innerHTML += "<a class='ebut' target='_blank' href='" + osmUrl + ident.toLowerCase() + "=" + osmid +"'>&#9998;</a>";

    var attrTbl = $$('table');
    attrTbl.setAttribute("id", "attr-tbl")
    con.appendChild(attrTbl);
    con.appendChild(suggD);

    var tbody = $$('tbody');
    attrTbl.appendChild(tbody);

    for (var key in stat.attrs) {
        var row = $$('tr');
        var col1 = $$('td');
        var col2 = $$('td');
        addCl(col2, "err-wrap");
        tbody.appendChild(row);
        row.appendChild(col1);
        row.appendChild(col2);
        col1.innerHTML = "<a href=\"https://wiki.openstreetmap.org/wiki/Key:" + key + "\" target=\"_blank\"><tt>" + key + "</tt></a>";
        for (var i = 0; i < stat.attrs[key].length; i++) col2.innerHTML += "<span class='attrval'>" + stat.attrs[key][i] + "</span>" + "<br>";
        attrrows[key] = row;
    }

    for (var i = 0; i < stat.attrerrs.length; i++) {
        var err = stat.attrerrs[i];
        var row = attrrows[err.attr[0]];
        addCl(row, "err-" + Math.round(err.conf * 10));

        var info = $$('div');

        if (err.other_grp) {
            // the mismatch was with a group name
            if (err.other_osmid > 1) info.innerHTML = "Does not match <tt>" + err.other_attr[0] + "</tt> in relation <a onmouseover='grHl( " + err.other_grp + ")' onmouseout='grUnHl( " + err.other_grp + ")' target=\"_blank\" href=\"" + osmUrl + "relation/" + Math.abs(err.other_osmid) + "\">" + Math.abs(err.other_osmid) + "</a>";
            else info.innerHTML = "Does not match <tt>" + err.other_attr[0] + "</tt> in relation <span onmouseover='grHl( " + err.other_grp + ")' onmouseout='grUnHl( " + err.other_grp + ")'>" + Math.abs(err.other_osmid) + "</span>";
        } else {
            // the mismatch was with another station
            if (err.other_osmid != stat.osmid) {
                var ident = err.other_osmid < 0 ? "way" : "node";
                info.innerHTML = "Does not match <tt>" + err.other_attr[0] + "</tt> in " + ident + " <a onmouseover='stHl( " + err.other + ")' onmouseout='stUnHl( " + err.other + ")' target=\"_blank\" href=\"" + osmUrl + ident+"/" + Math.abs(err.other_osmid) + "\">" + Math.abs(err.other_osmid) + "</a>";
            } else {
                info.innerHTML = "Does not match <tt>" + err.other_attr[0] + "</tt> = '" + err.other_attr[1] + "'";
            }
        }
        addCl(info, 'attr-err-info');
        row.childNodes[1].appendChild(info);
    }

    var suggList = $$('ul');

    if (stat.su.length) {
        var a = $$('span');
        addCl(a, "sugtit");
        a.innerHTML = "Suggestions";
        suggD.appendChild(a);
    }

    suggD.appendChild(suggList);

    for (var i = 0; i < stat.su.length; i++) {
        var sg = stat.su[i];
        var sgDiv = $$('li');        
        sgDiv.innerHTML = tmpl(suggsMsg[sg.type - 1], {"attr" : sg.attr, "tid" : sg.target_gid, "ooid" : sg.orig_osm_rel_id, "toid" : sg.target_osm_rel_id, "oid" : sg.orig_gid});
        suggList.appendChild(sgDiv);
    }

    L.popup({opacity: 0.8})
        .setLatLng(stat)
        .setContent(con)
        .openOn(map)
        .on('remove', function() {openedSt = -1; stUnHl(stat.id);});
}

function openSt(id) {req("s", "/stat?id=" + id, function(c) {rndrSt(c)});}

function rndrGr(grp, ll) {
    openedGr = grp.id;
    var attrrows = {};
    grHl(grp.id);

    var con = $$('div');
    con.setAttribute("id", "nav");

    var newMembers = $$('div');
    newMembers.setAttribute("id", "group-stations-new")
    newMembers.innerHTML = "<span class='newmemberstit'>New Members</span>";

    var oldMembers = $$('div');
    oldMembers.setAttribute("id", "group-stations-old")
    oldMembers.innerHTML = "<span class='oldmemberstit'>Existing Members</span>";

    if (grp.osmid == 1) {
        con.innerHTML = "<span class='grouplink'>New relation</span> <tt>public_transport=stop_area</tt>";
    } else {
        con.innerHTML = "OSM relation <a target='_blank' href='https://www.openstreetmap.org/relation/" + grp.osmid + "'>" + grp.osmid + "</a>";

        if (grp.attrs.name) con.innerHTML += " (<b>\"" + grp.attrs.name + "\"</b>)";

        con.innerHTML += "<a class='ebut' target='_blank' href='https://www.openstreetmap.org/edit?relation=" + grp.osmid +"'>&#9998;</a>";
    }

    var attrTbl = $$('table');
    attrTbl.setAttribute("id", "attr-tbl")
    con.appendChild(attrTbl);

    var tbody = $$('tbody');
    attrTbl.appendChild(tbody);

    var suggD = $$('div');
    suggD.setAttribute("id", "sugg")

    for (var key in grp.attrs) {
        var row = $$('tr');
        var col1 = $$('td');
        var col2 = $$('td');
        addCl(col2, "err-wrap");
        tbody.appendChild(row);
        row.appendChild(col1);
        row.appendChild(col2);
        col1.innerHTML = "<a href=\"https://wiki.openstreetmap.org/wiki/Key:" + key + "\" target=\"_blank\"><tt>" + key + "</tt></a>";
        for (var i = 0; i < grp.attrs[key].length; i++) col2.innerHTML += "<span class='attrval'>" + grp.attrs[key][i] + "</span>" + "<br>";
        attrrows[key] = row;
    }

    for (var i = 0; i < grp.attrerrs.length; i++) {
        var err = grp.attrerrs[i];
        var row = attrrows[err.attr[0]];
        addCl(row, "err-" + Math.round(err.conf * 10));

        var info = $$('div');

        if (err.other_grp) {
            // the mismatch was with a group name
            if (err.other_osmid != grp.osmid) {
                if (err.other_osmid > 1) info.innerHTML = "Does not match <tt>" + err.other_attr[0] + "</tt> in relation <a onmouseover='grHl( " + err.other_grp + ")' onmouseout='grUnHl( " + err.other_grp + ")' target=\"_blank\" href=\"" + osmUrl + "relation/" + Math.abs(err.other_osmid) + "\">" + Math.abs(err.other_osmid) + "</a>";
                else info.innerHTML = "Does not match <tt>" + err.other_attr[0] + "</tt> in relation <span onmouseover='grHl( " + err.other_grp + ")' onmouseout='grUnHl( " + err.other_grp + ")'>" + Math.abs(err.other_osmid) + "</span>";
            } else info.innerHTML = "Does not match <tt>" + err.other_attr[0] + "</tt> = '" + err.other_attr[1] + "'";
        } else {
            // the mismatch was with another station
            var ident = err.other_osmid < 0 ? "way" : "node";
            info.innerHTML = "Does not match <tt>" + err.other_attr[0] + "</tt> in " + ident + " <a onmouseover='stHl( " + err.other + ")' onmouseout='stUnHl( " + err.other + ")' target=\"_blank\" href=\"" + osmUrl + ident+"/" + Math.abs(err.other_osmid) + "\">" + Math.abs(err.other_osmid) + "</a>";
        }

        addCl(info, 'attr-err-info');
        row.childNodes[1].appendChild(info);
    }

    con.appendChild(newMembers);
    if (grp.osmid != 1) con.appendChild(oldMembers);

    for (var key in grp.stations) {
        var stat = grp.stations[key];
        var row = $$('div');
        var ident = stat.osmid < 0 ? "Way" : "Node";

        row.innerHTML = ident + " <a onmouseover='stHl( " + stat.id + ")' onmouseout='stUnHl( " + stat.id + ")' target='_blank' href='" + osmUrl + ident.toLowerCase() + "/" + Math.abs(stat.osmid) + "'>" + Math.abs(stat.osmid) + "</a>";

        if (stat.attrs.name) row.innerHTML += " (<b>\"" + stat.attrs.name + "\"</b>)";

        row.style.backgroundColor = stat.e ? '#f58d8d' : stat.s ? '#b6b6e4' : '#c0f7c0';

        if (grp.osmid == 1 || stat.orig_group != grp.id) newMembers.appendChild(row);
        else {
            oldMembers.appendChild(row);
            if (stat.group != grp.id) addCl(row, "del-stat");
        }
    }

    var suggList = $$('ul');

    if (grp.su.length) {
        var a = $$('span');
        addCl(a, "sugtit");
        a.innerHTML = "Suggestions";
        suggD.appendChild(a);
    }

    suggD.appendChild(suggList);

    for (var i = 0; i < grp.su.length; i++) {
        var sugg = grp.su[i];
        var suggDiv = $$('li');

        if (sugg.type == 6) suggDiv.innerHTML = "Fix attribute <tt>" + sugg.attr + "</tt>.";
        else if (sugg.type == 7) suggDiv.innerHTML = "Consider adding a <tt><a target='_blank' href='https://wiki.openstreetmap.org/wiki/Key:name'>name</a></tt> attribute.";
        else if (sugg.type == 8) suggDiv.innerHTML = "Attribute <tt>" + sugg.attr + "</tt> seems to be a track number. Use <tt>ref</tt> for this and set <tt>" + sugg.attr + "</tt> to the station name.";

        suggList.appendChild(suggDiv);
    }

    con.appendChild(suggD);

    L.popup({opacity: 0.8})
        .setLatLng(ll)
        .setContent(con)
        .openOn(map)
        .on('remove', function() {openedGr = -1; grUnHl(grp.id)});
}

function openGr(id, ll) {
    req("g", "/group?id=" + id, function(c) {rndrGr(c, ll)});
}

function grHl(id) {
    !grIdx[id] || grIdx[id].setStyle({'weight': 6, 'color': "#eecc00"});
}

function grUnHl(id) {
    !grIdx[id] || grIdx[id].setStyle({
        'weight': 3,
        'color': grIdx[id].options["fillColor"]
    });
}

function stHl(id) {
    if (!stIdx[id]) return;

    if (map.getZoom() > 15) {
        stIdx[id].setStyle({
            'weight': 5,
            'color': "#eecc00"
        });
    } else {
        stIdx[id].setStyle({
            'color': "#eecc00"
        });   
    }
}

function stUnHl(id) {
    if (!stIdx[id]) return;

    if (map.getZoom() > 15) {
        stIdx[id].setStyle({
            'weight': map.getZoom() > 17 ? 1.5 : 1,
            'color': "black"
        });
    } else {
        stIdx[id].setStyle({
            'color': stIdx[id].options["fillColor"]
        });   
    }
}

var map = L.map('map', {renderer: L.canvas(), attributionControl: false}).setView([47.9965, 7.8469], 13);

map.addControl(L.control.attribution({
    position: 'bottomright',
    prefix: '&copy; <a href="http://ad.cs.uni-freiburg.de">University of Freiburg, Chair of Algorithms and Data Structures</a>'
}));

map.on('popupopen', function(e) {
    var px = map.project(e.target._popup._latlng);
    px.y -= e.target._popup._container.clientHeight/2;
    map.panTo(map.unproject(px),{animate: true});
    search();
});

L.tileLayer('http://{s}.tile.stamen.com/toner-lite/{z}/{x}/{y}.png', {
    maxZoom: 20,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    opacity: 0.8
}).addTo(map);

var l = L.featureGroup().addTo(map);

map.on("moveend", function() {render();});
map.on("click", function() {search()});

function render() {
    if (map.getZoom() < 11) {
        req("m", "/heatmap?z=" + map.getZoom() + "&bbox=" + [map.getBounds().getSouthWest().lat, map.getBounds().getSouthWest().lng, map.getBounds().getNorthEast().lat, map.getBounds().getNorthEast().lng].join(","),
            function(re) {
                l.clearLayers();

                var blur = 22 - map.getZoom();
                var rad = 25 - map.getZoom();

                l.addLayer(L.heatLayer(re.ok, {
                    max: 500,
                    gradient: {
                        0: '#cbf7cb',
                        0.5: '#78f378',
                        1: '#29c329'
                    },
                    minOpacity: 0.65,
                    blur: blur,
                    radius: rad
                }));
                l.addLayer(L.heatLayer(re.sugg, {
                    max: 500,
                    gradient: {
                        0: '#7f7fbd',
                        0.5: '#4444b3',
                        1: '#0606c1'
                    },
                    minOpacity: 0.65,
                    blur: blur - 3,
                    radius: Math.min(12, rad - 3)
                }));
                l.addLayer(L.heatLayer(re.err, {
                    max: 500,
                    gradient: {
                        0: '#f39191',
                        0.5: '#ff5656',
                        1: '#ff0000'
                    },
                    minOpacity: 0.75,
                    blur: blur - 3,
                    radius: Math.min(10, rad - 3),
                    maxZoom: 15
                }));
            }
        )
    } else {
        req("m", "/map?z=" + map.getZoom() + "&bbox=" + [map.getBounds().getSouthWest().lat, map.getBounds().getSouthWest().lng, map.getBounds().getNorthEast().lat, map.getBounds().getNorthEast().lng].join(","),
            function(re) {
                l.clearLayers();
                grIdx = {};
                stIdx = {};

                var stats = [];
                for (var i = 0; i < re.stats.length; i++) {
                    stIdx[re.stats[i].i] = stats[stats.push(marker(re.stats[i], map.getZoom())) - 1];
                }

                var groups = [];
                for (var i = 0; i < re.groups.length; i++) {
                    grIdx[re.groups[i].i] = groups[groups.push(poly(re.groups[i], map.getZoom())) - 1];;
                }

                var suggs = [];
                for (var i = 0; i < re.su.length; i++) {
                    suggs.push(sugArr(re.su[i], map.getZoom()));
                }

                if (map.getZoom() > 13) {
                    l.addLayer(L.featureGroup(groups).on('click', function(a) {
                        openGr(a.layer.options.id, a.layer.getBounds().getCenter());
                    }));
                }

                l.addLayer(L.featureGroup(stats).on('click', function(a) {
                    openSt(a.layer.options.id);
                }));

                if (map.getZoom() > 15) {
                    l.addLayer(L.featureGroup(suggs).on('click', function(a) {
                        openSt(a.layer.options.id);
                    }));
                }

                grHl(openedGr);
                stHl(openedSt);
            }
        )
    };
}

function rowClick(row) {    
    if (!isSearchOpen()) return;
    if (row.stat) openSt(row.stat.i, ll(row.stat.g[0]));
    else openGr(row.group.i, ll(row.group.g[0]));
}

function select(row) {    
    if (!row) return;
    if (!isSearchOpen()) return;
    unselect(selectedRes);
    selectedRes = row;
    addCl(row, "selres");
    if (row.stat) stHl(row.stat.i);
    if (row.group) grHl(row.group.i);
}

function unselect(row) {    
    selectedRes = undefined;
    if (!row) return;
    delCl(row, "selres");
    if (row.stat) stUnHl(row.stat.i);
    if (row.group) grUnHl(row.group.i);
}

function isSearchOpen() {
    return $("#sres").className == "res-open";
}

function search(q) {
    var delay = 0;
    if (q == prevSearch) return;
    clearTimeout(delayTimer);
    prevSearch = q;
    unselect(selectedRes);
    if (!q) {
        $('#searchinput').value = "";
        $("#sres").className = "";
        return;
    }

    delayTimer = setTimeout(function() {
        req("sr", "/search?q=" + q, function(c) {
                var res = $("#sres");
                addCl(res, "res-open");
                res.innerHTML = "";
                for (var i = 0; i < c.length; i++) {
                    var e = c[i];
                    var row = $$('span');
                    addCl(row, "sres");
                    row.innerHTML = e.n;
                    if (e.w) addCl(row, "res-way");
                    if (e.s) {
                        row.stat = e.s;                    
                        addCl(row, "res-stat");
                        if (e.s.s) addCl(row, "res-sugg");                    
                        if (e.s.e) addCl(row, "res-err");
                    } else {
                        row.group = e.g;
                        addCl(row, "res-group");                       
                        if (e.g.s) addCl(row, "res-sugg");                    
                        if (e.g.e) addCl(row, "res-err");
                    }

                    row.onmouseover = function(){select(this)};
                    row.onclick = function(){rowClick(this)};

                    if (e.v && e.v != e.name) {
                        var via = $$('span');
                        addCl(via, "via");
                        via.innerHTML = e.v;
                        row.appendChild(via);
                    }
                    res.appendChild(row);
                }
            }
        )}, delay);
}

function keypress(e) {
    if (e.keyCode == 40) {
        var sels = $('.selres')
        if (sels.length) select(sels[0].nextSibling);
        else select($('.sres')[0]);
        e.preventDefault();
    } else if (e.keyCode == 38) {
        var sels = $('.selres')
        if (sels.length) {
            if (sels[0].previousSibling) select(sels[0].previousSibling);
            else unselect(sels[0]);
            e.preventDefault();
        }
    }

    if (e.keyCode == 13) {
        var sels = $('.selres');
        if (sels.length) rowClick(sels[0]);
    }
}

$('#del').onclick = function() {search();}

render();