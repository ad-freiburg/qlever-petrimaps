// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <curl/curl.h>

#include <cstring>
#include <iostream>
#include <parallel/algorithm>
#include <sstream>

#include "qlever-petrimaps/GeomCache.h"
#include "qlever-petrimaps/Misc.h"
#include "qlever-petrimaps/server/Requestor.h"
#include "util/Misc.h"
#include "util/geo/Geo.h"
#include "util/geo/PolyLine.h"
#include "util/log/Log.h"

using petrimaps::GeomCache;
using util::geo::FPoint;
using util::geo::latLngToWebMerc;

const static char* QUERY =
    "SELECT ?geometry WHERE {"
    " ?osm_id <http://www.opengis.net/ont/geosparql#hasGeometry> ?geometry ."
    "   { ?osm_id <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
    "<https://www.openstreetmap.org/node> }"
    " UNION"
    "     { ?osm_id <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
    "<https://www.openstreetmap.org/way> }"
    " } ORDER BY ?geometry LIMIT "
    "18446744073709551615";
// "100000";

// _____________________________________________________________________________
size_t GeomCache::writeCb(void* contents, size_t size, size_t nmemb,
                          void* userp) {
  size_t realsize = size * nmemb;
  static_cast<GeomCache*>(userp)->parse(static_cast<const char*>(contents),
                                        realsize);
  return realsize;
}

// _____________________________________________________________________________
size_t GeomCache::writeCbIds(void* contents, size_t size, size_t nmemb,
                             void* userp) {
  size_t realsize = size * nmemb;
  static_cast<GeomCache*>(userp)->parseIds(static_cast<const char*>(contents),
                                           realsize);
  return realsize;
}

// _____________________________________________________________________________
void GeomCache::parse(const char* c, size_t size) {
  const char* start = c;
  while (c < start + size) {
    switch (_state) {
      case IN_HEADER:
        if (*c == '\n') {
          _state = IN_ROW;
          c++;
        } else {
          c++;
          continue;
        }
      case IN_ROW:
        if (*c == '\t' || *c == '\n') {
          bool isGeom = util::endsWith(
              _dangling, "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

          auto p = _dangling.rfind("\"POINT(", 0);

          if (isGeom && p != std::string::npos) {
            p += 7;
            auto point = parsePoint(_dangling, p);
            if (pointValid(point)) {
              _points.push_back(point);
              _qleverIdToInternalId.push_back({0, _points.size() - 1});
            } else {
              _qleverIdToInternalId.push_back({0, 2 * I_OFFSET});
            }
          } else if (isGeom && (p = _dangling.rfind("\"LINESTRING(", 0)) !=
                                   std::string::npos) {
            p += 12;
            const auto& line = parseLineString(_dangling, p);
            _lines.push_back(line);
            _lineBoxes.push_back(util::geo::getBoundingBox(line));
            _qleverIdToInternalId.push_back({0, I_OFFSET + _lines.size() - 1});
          } else if (isGeom && (p = _dangling.rfind("\"MULTILINESTRING(", 0)) !=
                                   std::string::npos) {
            p += 17;
            size_t i = 0;
            while ((p = _dangling.find("(", p + 1)) != std::string::npos) {
              const auto& line = parseLineString(_dangling, p + 1);
              _lines.push_back(line);
              _lineBoxes.push_back(util::geo::getBoundingBox(line));
              _qleverIdToInternalId.push_back(
                  {i == 0 ? 0 : 1, I_OFFSET + _lines.size() - 1});
              i++;
            }
          } else if (isGeom && (p = _dangling.rfind("\"POLYGON(", 0)) !=
                                   std::string::npos) {
            p += 9;
            size_t i = 0;
            while ((p = _dangling.find("(", p + 1)) != std::string::npos) {
              const auto& line = parseLineString(_dangling, p + 1);
              _lines.push_back(line);
              _lineBoxes.push_back(util::geo::getBoundingBox(line));
              _qleverIdToInternalId.push_back(
                  {i == 0 ? 0 : 1, I_OFFSET + _lines.size() - 1});
              i++;
            }
          } else if (isGeom && (p = _dangling.rfind("\"MULTIPOLYGON(", 0)) !=
                                   std::string::npos) {
            p += 13;
            size_t i = 0;
            while ((p = _dangling.find("(", p + 1)) != std::string::npos) {
              if (_dangling[p + 1] == '(') p++;
              const auto& line = parseLineString(_dangling, p + 1);
              _lines.push_back(line);
              _lineBoxes.push_back(util::geo::getBoundingBox(line));
              _qleverIdToInternalId.push_back(
                  {i == 0 ? 0 : 1, I_OFFSET + _lines.size() - 1});
              i++;
            }
          } else {
            _qleverIdToInternalId.push_back({0, 2 * I_OFFSET});
          }

          if (*c == '\n') {
            _curRow++;
            if (_curRow % 1000000 == 0) {
              LOG(INFO) << "[GEOMCACHE] "
                        << "@ row " << _curRow;
            }
            _dangling.clear();
            c++;
            continue;
          } else {
            _dangling.clear();
            c++;
            continue;
          }
        }

        _dangling += *c;
        c++;

        break;
      default:
        break;
    }
  }
}

// _____________________________________________________________________________
void GeomCache::parseIds(const char* c, size_t size) {
  for (size_t i = 0; i < size; i++) {
    _curId.bytes[_curByte] = c[i];
    _curByte = (_curByte + 1) % 8;

    if (_curByte == 0) {
      if (_curRow % 1000000 == 0) {
        LOG(INFO) << "[GEOMCACHE] "
                  << "@ row " << _curRow;
      }
      if (_curRow < _qleverIdToInternalId.size() &&
          _qleverIdToInternalId[_curRow].first == 0) {
        _qleverIdToInternalId[_curRow].first = _curId.val;
      } else {
        LOG(WARN) << "The results for the binary IDs are out of sync.";
        LOG(WARN) << "_curRow: " << _curRow
                  << " _qleverIdInt.size: " << _qleverIdToInternalId.size()
                  << " cur val: " << _qleverIdToInternalId[_curRow].first;
      }

      // if a qlever entity contained multiple geometries (MULTILINESTRING,
      // MULTIPOLYGON, MULTIPOIN), they appear consecutively in
      // _qleverIdToInternalId; continuation geometries are marked by a
      // preliminary qlever ID of 1, while the first geometry always has a
      // preliminary id of 0
      while (_curRow < _qleverIdToInternalId.size() - 1 &&
             _qleverIdToInternalId[_curRow + 1].first == 1) {
        _qleverIdToInternalId[++_curRow].first = _curId.val;
      }

      _curRow++;
    }
  }
}

// _____________________________________________________________________________
void GeomCache::request() {
  _state = IN_HEADER;
  _points.clear();
  _lines.clear();
  _curRow = 0;
  _dangling.clear();
  _dangling.reserve(10000);

  LOG(INFO) << "[GEOMCACHE] Query is " << QUERY;

  CURLcode res;
  char errbuf[CURL_ERROR_SIZE];

  if (_curl) {
    auto qUrl = queryUrl(QUERY);

    LOG(INFO) << "[GEOMCACHE] Query URL is " << qUrl;
    curl_easy_setopt(_curl, CURLOPT_URL, qUrl.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, GeomCache::writeCb);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, this);
    curl_easy_setopt(_curl, CURLOPT_ERRORBUFFER, errbuf);

    // set headers
    struct curl_slist* headers;
    headers = curl_slist_append(headers, "Accept: text/tab-separated-values");
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, headers);

    // accept any compression supported
    curl_easy_setopt(_curl, CURLOPT_ACCEPT_ENCODING, "");
    res = curl_easy_perform(_curl);
  } else {
    LOG(ERROR) << "[GEOMCACHE] Failed to perform curl request.";
    return;
  }

  // check if there was an error
  if (res != CURLE_OK) {
    size_t len = strlen(errbuf);
    if (len > 0) {
      LOG(ERROR) << "[GEOMCACHE] " << errbuf;
    } else {
      LOG(ERROR) << "[GEOMCACHE] " << curl_easy_strerror(res);
    }
  } else {
    LOG(INFO) << "[GEOMCACHE] Done";
    LOG(INFO) << "[GEOMCACHE] Received " << _points.size() << " points and "
              << _lines.size() << " lines";
  }
}

// _____________________________________________________________________________
void GeomCache::requestIds() {
  _curByte = 0;
  _curRow = 0;

  LOG(INFO) << "[GEOMCACHE] Query is " << QUERY;

  if (_curl) {
    auto qUrl = queryUrl(QUERY);
    LOG(INFO) << "[GEOMCACHE] Binary ID query URL is " << qUrl;
    curl_easy_setopt(_curl, CURLOPT_URL, qUrl.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, GeomCache::writeCbIds);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, this);

    // set headers
    struct curl_slist* headers = 0;
    headers = curl_slist_append(headers, "Accept: application/octet-stream");
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, headers);

    // accept any compression supported
    curl_easy_setopt(_curl, CURLOPT_ACCEPT_ENCODING, "");
    curl_easy_perform(_curl);
  } else {
    LOG(ERROR) << "[GEOMCACHE] Failed to perform curl request.";
  }

  LOG(INFO) << "[GEOMCACHE] Received " << _curRow << " rows";
  LOG(INFO) << "[GEOMCACHE] Done";

  // sorting by qlever id
  LOG(INFO) << "[GEOMCACHE] Sorting results by qlever ID...";
  __gnu_parallel::sort(_qleverIdToInternalId.begin(),
                       _qleverIdToInternalId.end());
  LOG(INFO) << "[GEOMCACHE] ... done";
}

// _____________________________________________________________________________
std::string GeomCache::queryUrl(std::string query) const {
  std::stringstream ss;

  if (util::toLower(query).find("limit") == std::string::npos) {
    query += " LIMIT " + std::to_string(MAXROWS);
  }

  auto esc = curl_easy_escape(_curl, query.c_str(), query.size());

  ss << _backendUrl << "/?send=" << std::to_string(MAXROWS) << "&query=" << esc;

  return ss.str();
}

// _____________________________________________________________________________
bool GeomCache::pointValid(const FPoint& p) {
  if (p.getY() > std::numeric_limits<float>::max()) return false;
  if (p.getY() < std::numeric_limits<float>::lowest()) return false;
  if (p.getX() > std::numeric_limits<float>::max()) return false;
  if (p.getX() < std::numeric_limits<float>::lowest()) return false;

  return true;
}

// _____________________________________________________________________________
util::geo::FLine GeomCache::parseLineString(const std::string& a,
                                            size_t p) const {
  util::geo::FLine line;
  auto end = memchr(a.c_str() + p, ')', a.size() - p);
  assert(end);

  while (true) {
    auto point = latLngToWebMerc(FPoint(
        util::atof(a.c_str() + p, 10),
        util::atof(
            static_cast<const char*>(memchr(a.c_str() + p, ' ', a.size() - p)) +
                1,
            10)));

    if (!pointValid(point)) continue;
    line.push_back(point);

    auto n = memchr(a.c_str() + p, ',', a.size() - p);
    if (!n || n > end) break;
    p = static_cast<const char*>(n) - a.c_str() + 1;
  }

  return line;
}

// _____________________________________________________________________________
util::geo::FPoint GeomCache::parsePoint(const std::string& a, size_t p) const {
  auto point = latLngToWebMerc(FPoint(
      util::atof(a.c_str() + p, 10),
      util::atof(
          static_cast<const char*>(memchr(a.c_str() + p, ' ', a.size() - p)) +
              1,
          10)));

  return point;
}

// _____________________________________________________________________________
std::vector<std::pair<size_t, size_t>> GeomCache::getRelObjects(
    const std::vector<std::pair<uint64_t, uint64_t>>& ids) const {
  // (geom id, result row)
  std::vector<std::pair<size_t, size_t>> ret;

  // in most cases, the return size will be exactly the size of the ids set
  ret.reserve(ids.size());

  size_t i = 0;
  size_t j = 0;

  while (i < ids.size() && j < _qleverIdToInternalId.size()) {
    if (ids[i].first == _qleverIdToInternalId[j].first) {
      ret.push_back({_qleverIdToInternalId[j].second, ids[i].second});
      j++;
    } else if (ids[i].first < _qleverIdToInternalId[j].first) {
      i++;
    } else {
      j++;
    }
  }

  return ret;
}
