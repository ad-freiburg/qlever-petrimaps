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

    $ petrimaps [-p <port=9090>]

Requests can be send via the `?query` get parameter.
The QLever backend to use must be specified via the `?backend` get parameter.

**IMPORTANT**: the tool currently expects the geometry to be the last selected column.

    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX osmkey: <https://www.openstreetmap.org/wiki/Key:>
    SELECT ?osm_id ?geometry WHERE {
      ?osm_id osmkey:building ?building .
      ?osm_id geo:hasGeometry ?geometry .
    }
