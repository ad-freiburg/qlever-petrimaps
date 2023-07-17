# QLever petrimaps

Visualization of geospatial SPARQL query results for QLever on a map, either as individual objeckts or as a heatmap. Can handle hundreds of millions of result rows, tested with over 200 million results (all streets in OSM). Implemented as a middle-end / middleware.
This is still very much hacked-together, use with care.

Examples: [All railway lines in OSM](https://qlever.cs.uni-freiburg.de/mapui-petri/?query=PREFIX%20osm%3A%20%3Chttps%3A%2F%2Fwww.openstreetmap.org%2F%3E%0APREFIX%20rdf%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23%3E%0APREFIX%20geo%3A%20%3Chttp%3A%2F%2Fwww.opengis.net%2Font%2Fgeosparql%23%3E%0APREFIX%20osmkey%3A%20%3Chttps%3A%2F%2Fwww.openstreetmap.org%2Fwiki%2FKey%3A%3E%0ASELECT%20%3Fosm_id%20%3Fgeometry%20WHERE%20%7B%0A%20%20%3Fosm_id%20osmkey%3Arailway%20%3Frail%20.%0A%20%20%3Fosm_id%20rdf%3Atype%20osm%3Away%20.%0A%20%20%3Fosm_id%20geo%3AhasGeometry%20%3Fgeometry%20.%0A%7D&backend=https%3A%2F%2Fqlever.cs.uni-freiburg.de%2Fapi%2Fosm-planet), [All buildings in Germany in OSM](https://qlever.cs.uni-freiburg.de/mapui-petri/?query=PREFIX%20osm2rdf%3A%20%3Chttps%3A%2F%2Fosm2rdf.cs.uni-freiburg.de%2Frdf%23%3EPREFIX%20geo%3A%20%3Chttp%3A%2F%2Fwww.opengis.net%2Font%2Fgeosparql%23%3E%20PREFIX%20osmkey%3A%20%3Chttps%3A%2F%2Fwww.openstreetmap.org%2Fwiki%2FKey%3A%3E%20PREFIX%20ogc%3A%20%3Chttp%3A%2F%2Fwww.opengis.net%2Frdf%23%3E%20PREFIX%20osmrel%3A%20%3Chttps%3A%2F%2Fwww.openstreetmap.org%2Frelation%2F%3E%20SELECT%20%3Fosm_id%20%3Fhasgeometry%20WHERE%20%7B%20%7B%20osmrel%3A51477%20osm2rdf%3Acontains_area%2B%20%3Fqlm_i%20.%20%3Fqlm_i%20osm2rdf%3Acontains_nonarea%20%3Fosm_id%20.%20%3Fosm_id%20geo%3AhasGeometry%20%3Fhasgeometry%20.%20%3Fosm_id%20osmkey%3Abuilding%20%3Fbuilding%20%7D%20UNION%20%7B%20%7B%20osmrel%3A51477%20osm2rdf%3Acontains_area%2B%20%3Fosm_id%20.%20%3Fosm_id%20geo%3AhasGeometry%20%3Fhasgeometry%20.%20%3Fosm_id%20osmkey%3Abuilding%20%3Fbuilding%20%7D%20UNION%20%7B%20osmrel%3A51477%20osm2rdf%3Acontains_nonarea%20%3Fosm_id%20.%20%3Fosm_id%20geo%3AhasGeometry%20%3Fhasgeometry%20.%20%3Fosm_id%20osmkey%3Abuilding%20%3Fbuilding%20%7D%20%7D%20%7D&backend=https%3A%2F%2Fqlever.cs.uni-freiburg.de%2Fapi%2Fosm-planet)

## Requirements
* gcc > 5.0 || clang > 3.9
* libcurl
* libpng (for PNG rendering)
* Java Runtime Environment (for compiling the JS of the web frontend)

## Optional Requirements
* zlib (for gzip compression)
* OpenMP

## Installation

Compile yourself:

    $ git clone https://github.com/ad-freiburg/qlever-petrimaps
    $ cd qlever-petrimaps
    $ mkdir -p build && cd build
    $ cmake ..
    $ make

via Docker:

    $ docker build -t petrimaps .

## Usage

To start:

    $ petrimaps [-p <port=9090>] [-m <memory limit] [-c <cache dir>]

Requests can be send via the `?query` get parameter.
The QLever backend to use must be specified via the `?backend` get parameter.

**IMPORTANT**: the tool currently expects the geometry to be the last selected column.

    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX osmkey: <https://www.openstreetmap.org/wiki/Key:>
    SELECT ?osm_id ?geometry WHERE {
      ?osm_id osmkey:building ?building .
      ?osm_id geo:hasGeometry ?geometry .
    }

## Cache + Memory Management

The tool caches query results and memory usage will thus slowly build up. There is a primitive memory limit which can be set via the `-m` parameter (in GB). By default, 90% of the available system memory are used.

If a query runs out of memory, you can clear all existing caches by requesting

    /clearsession

`/clearsessions` will also work. Optionally, you can specify the session id via `?id=<SESSIONID>'.

## Disk Cache

If `-c` specifies a serialization cache directory, the complete geometries downloaded from a QLever backend will be serialized to disk and re-used on later startups. This significantly speeds up the loading times.
