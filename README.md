[![Build](https://github.com/ad-freiburg/qlever-petrimaps/actions/workflows/build.yml/badge.svg)](https://github.com/ad-freiburg/qlever-petrimaps/actions/workflows/build.yml)
# QLever Petrimaps

Interactive visualization of SPARQL query results with geospatial information on a map, either as individual objects or as a heatmap. Petrimaps can display **hundreds of millions of objects** interactively, for example, a heatmap of all the streets in a given country, or even in the whole world. Petrimaps is implemented as a middleware. In the [QLever UI](https://github.com/ad-freiburg/qlever-ui), whenever a SPARQL result has WKT literals in its last column, a "Map view" button is displayed, which leads to an instance of Petrimaps.

Here are two example queries: [All railway lines in OSM](https://qlever.cs.uni-freiburg.de/petrimaps/?query=PREFIX+rdf%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23%3E+PREFIX+geo%3A+%3Chttp%3A%2F%2Fwww.opengis.net%2Font%2Fgeosparql%23%3E+PREFIX+osmkey%3A+%3Chttps%3A%2F%2Fwww.openstreetmap.org%2Fwiki%2FKey%3A%3E+PREFIX+osm%3A+%3Chttps%3A%2F%2Fwww.openstreetmap.org%2F%3E+PREFIX+rdf%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23%3E+SELECT+%3Fosm_id+%3Fgeometry+WHERE+%7B+%3Fosm_id+osmkey%3Arailway+%3Frail+.+%3Fosm_id+rdf%3Atype+osm%3Away+.+%3Fosm_id+geo%3AhasGeometry%2Fgeo%3AasWKT+%3Fgeometry+%7D&backend=https%3A%2F%2Fqlever.cs.uni-freiburg.de%2Fapi%2Fosm-planet), [All streets in Germany in OSM](https://qlever.cs.uni-freiburg.de/petrimaps/?query=PREFIX+ogc%3A+%3Chttp%3A%2F%2Fwww.opengis.net%2Frdf%23%3E+PREFIX+osmrel%3A+%3Chttps%3A%2F%2Fwww.openstreetmap.org%2Frelation%2F%3E+PREFIX+geo%3A+%3Chttp%3A%2F%2Fwww.opengis.net%2Font%2Fgeosparql%23%3E+PREFIX+osm%3A+%3Chttps%3A%2F%2Fwww.openstreetmap.org%2F%3E+PREFIX+rdf%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23%3E+PREFIX+osmkey%3A+%3Chttps%3A%2F%2Fwww.openstreetmap.org%2Fwiki%2FKey%3A%3E+SELECT+%3Fosm_id+%3Fshape+WHERE+%7B+osmrel%3A51477+ogc%3AsfContains+%3Fosm_id+.+%3Fosm_id+osmkey%3Ahighway+%3Fhighway+.+%3Fosm_id+rdf%3Atype+osm%3Away+.+%3Fosm_id+geo%3AhasGeometry%2Fgeo%3AasWKT+%3Fshape+%7D&backend=https%3A%2F%2Fqlever.cs.uni-freiburg.de%2Fapi%2Fosm-planet).

## Requirements
* gcc > 5.0 || clang > 3.9
* xxd
* libcurl
* libpng (for PNG rendering)
* Java Runtime Environment (for compiling the JS of the web frontend)

## Optional Requirements
* zlib (for gzip compression)
* OpenMP

## Installation

Compile yourself:

    $ git clone --recurse-submodules https://github.com/ad-freiburg/qlever-petrimaps
    $ cd qlever-petrimaps
    $ mkdir -p build && cd build
    $ cmake ..
    $ make

via Docker:

    $ docker build -t petrimaps .

## Usage

To start:

    $ qlever-petrimaps [-p <port=9090>] [-m <memory limit] [-c <cache dir>] [-x <access token>]

Requests can be send via the `?query` get parameter.
The QLever backend to use must be specified via the `?backend` get parameter.
If an access token was given, and if no cache existed for the requested backend, the backend must first be created by calling

    /touch?backend=<backend>

with the value of the `Authorization` header set to `<access token>`. Optionally, a parameter `cfg` can be set to a JSON dict containing a backend config. Currently, the only config field supported is `fillQuery`, which can be set to the query used to fill the geometry cache for that backend. The query must return a list of all geometries. If no config is given, a default fill query will be used.

**IMPORTANT**: by default, the tool currently expects the geometry to be the last selected column.

    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX osmkey: <https://www.openstreetmap.org/wiki/Key:>
    SELECT ?osm_id ?geometry WHERE {
      ?osm_id osmkey:building ?building .
      ?osm_id geo:hasGeometry ?geometry .
    }

To use other columns, they must be configured during query time. See the example integration page below for how to use different layers.

## Example integration page

After start, an example integration page can be found at `/example`

## Cache + Memory Management

The tool caches query results and memory usage will thus slowly build up. There is a primitive memory limit which can be set via the `-m` parameter (in GB). By default, 90% of the available system memory are used.

Query results are evicted from the cache after `-t` minutes (default: 360).

If a query still runs out of memory, you can clear all existing caches by requesting

    /clearsession

`/clearsessions` will also work. Optionally, you can specify the session id via `?id=<SESSIONID>'.

## Disk Cache

If `-c` specifies a serialization cache directory, the complete geometries downloaded from a QLever backend will be serialized to disk and re-used on later startups. This significantly speeds up the loading times.
