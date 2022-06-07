# QLever petrimaps

Visualization of geospatial SPARQL query results for QLever as a heatmap. Can handle millions of result rows, tested with up to 60 million results (all buildings in Germany). Implemented as a middle-end / middleware.
This is still very much hacked-together, use with care.

## Requirements
* gcc > 5.0 || clang > 3.9
* zlib (for gzip compression)
* libcurl (curl *must* use GnuTLS, not OpenSSL, as the latter is not thread safe per default)
* libpng (for PNG rendering)

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
