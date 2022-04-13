// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <curl/curl.h>
#include <cstring>
#include <iostream>
#include <regex>
#include <sstream>
#include "qlever-mapui/Misc.h"
#include "qlever-mapui/server/Requestor.h"
#include "util/Misc.h"
#include "util/geo/Geo.h"
#include "util/geo/PolyLine.h"
#include "util/log/Log.h"

using mapui::GeomCache;
using mapui::Requestor;
using mapui::ResObj;

// _____________________________________________________________________________
size_t Requestor::writeCb(void* contents, size_t size, size_t nmemb,
                          void* userp) {
  size_t realsize = size * nmemb;
  static_cast<Requestor*>(userp)->parse(static_cast<const char*>(contents),
                                        realsize);
  return realsize;
}

// _____________________________________________________________________________
size_t Requestor::writeCbIds(void* contents, size_t size, size_t nmemb,
                             void* userp) {
  size_t realsize = size * nmemb;
  static_cast<Requestor*>(userp)->parseIds(static_cast<const char*>(contents),
                                           realsize);
  return realsize;
}

// _____________________________________________________________________________
void Requestor::parseIds(const char* c, size_t size) const {
  for (size_t i = 0; i < size; i++) {
    _curId.bytes[_curByte] = c[i];
    _curByte = ++_curByte % 8;

    if (_curByte == 0) {
      _curIds.push_back({_curId.val, _curIds.size()});
    }
  }
}

// _____________________________________________________________________________
void Requestor::parse(const char* c, size_t size) const {
  const char* start = c;
  const char* tmp;
  while ((c - start) < size) {
    switch (_state) {
      case IN_HEADER:
        if (*c == '\t' || *c == '\n') {
          _colNames.push_back(_dangling);
          _dangling.clear();
        }

        if (*c == '\n') {
          _curRow++;
          _state = IN_ROW;
          c++;
        } else {
          _dangling += *c;
          c++;
          continue;
        }
      case IN_ROW:
        if (*c == '\t' || *c == '\n') {
          // bool isGeom = util::endsWith(
          // _dangling, "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

          _cols.push_back({_colNames[_curCol], _dangling});

          if (*c == '\n') {
            _curRow++;
            _curCol = 0;
            _dangling = "";
            c++;
            continue;
          } else {
            _curCol++;
            _dangling = "";
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
void Requestor::request(const std::string& qry) const {
  _query = qry;
  _curRow = 0;
  _curByte = 0;
  _points.clear();
  _lines.clear();
  _received = 0;
  CURLcode res;

  if (_curl) {
    auto qUrl = queryUrl(qry);
    LOG(INFO) << "Query URL is " << qUrl;
    curl_easy_setopt(_curl, CURLOPT_URL, qUrl.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, Requestor::writeCbIds);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, this);

    // set headers
    struct curl_slist* headers = 0;
    headers = curl_slist_append(headers, "Accept: application/octet-stream");
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, headers);

    // accept any compression supported
    curl_easy_setopt(_curl, CURLOPT_ACCEPT_ENCODING, "");
    res = curl_easy_perform(_curl);
  }

  LOG(INFO) << "Done, have " << _curIds.size() << " ids in total.";

  // join with geoms from GeomCache

  // sort by qlever id
  LOG(INFO) << "Sorting results by qlever ID...";
  std::sort(_curIds.begin(), _curIds.end());
  LOG(INFO) << "... done";

  LOG(INFO) << "Retrieving geoms from cache...";
  // (geom id, result row)
  const auto& geoms = _cache->getRelObjects(_curIds);
  LOG(INFO) << "... done, got " << geoms.size() << " geoms.";

  LOG(INFO) << "Calculating bounding box of result...";

  util::geo::FBox bbox;
  for (auto p : geoms) {
    auto geomId = p.first;
    auto rowId = p.second;

    // TODO: precalculate boxes in geomcache, add getBox()

    if (geomId < I_OFFSET) {
      auto pId = geomId;
      _points.push_back({pId, rowId});
      bbox = util::geo::extendBox(_cache->getPointBBox(pId), bbox);
    } else if (geomId < 2 * I_OFFSET) {
      auto lId = geomId - I_OFFSET;
      _lines.push_back({lId, rowId});
      bbox = util::geo::extendBox(_cache->getLineBBox(lId), bbox);
    }
  }

  LOG(INFO) << "... done";

  LOG(INFO) << "BBox: " << util::geo::getWKT(bbox);
  LOG(INFO) << "Starting building grid...";

  _pgrid = mapui::Grid<size_t, util::geo::Point, float>(100000, 100000, bbox);
  _lgrid = mapui::Grid<size_t, util::geo::Line, float>(100000, 100000, bbox);

  LOG(INFO) << "...done init ...";

#pragma omp parallel sections
  {
#pragma omp section
    {
      size_t i = 0;
      for (const auto& p : _points) {
        _pgrid.add(_cache->getPoints()[p.first], _cache->getPointBBox(p.first),
                   i);
        i++;
      }
    }
#pragma omp section
    {
      size_t i = 0;
      for (const auto& l : _lines) {
        _lgrid.add(_cache->getLines()[l.first], _cache->getLineBBox(l.first),
                   i);
        i++;
      }
    }
  }

  LOG(INFO) << "...done";
}

// _____________________________________________________________________________
std::vector<std::pair<std::string, std::string>> Requestor::requestRow(
    uint64_t row) const {
  _state = IN_HEADER;
  _curRow = 0;
  _curCol = 0;
  _cols.clear();
  _colNames.clear();
  _points.clear();
  _lines.clear();
  _dangling = "";
  _received = 0;
  CURLcode res;

  if (_curl) {
    auto qUrl = queryUrlRow(_query, row);
    LOG(INFO) << "Query URL is " << qUrl;
    curl_easy_setopt(_curl, CURLOPT_URL, qUrl.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, Requestor::writeCb);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, this);

    // set headers
    struct curl_slist* headers = 0;
    headers = curl_slist_append(headers, "Accept: text/tab-separated-values");
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, headers);

    // accept any compression supported
    curl_easy_setopt(_curl, CURLOPT_ACCEPT_ENCODING, "");
    res = curl_easy_perform(_curl);
  }

  LOG(INFO) << "Done, received " << _received << " rows in total.";
  LOG(INFO) << "...done";

  return _cols;
}

// _____________________________________________________________________________
std::string Requestor::queryUrl(std::string query) const {
  std::stringstream ss;

  // only use last column
  std::regex expr("select\\s*(\\?[A-Z0-9_\\-+]*\\s*)+\\s*where\\s*\\{",
                  std::regex_constants::icase);
  query = std::regex_replace(query, expr, "SELECT $1 WHERE {");

  if (util::toLower(query).find("limit") == std::string::npos) {
    query += " LIMIT 18446744073709551615";
  }

  auto esc = curl_easy_escape(_curl, query.c_str(), query.size());

  ss << _cache->getBackendURL() << "/?send=18446744073709551615"
     << "&query=" << esc;

  return ss.str();
}

// _____________________________________________________________________________
std::string Requestor::queryUrlRow(std::string query, uint64_t row) const {
  std::stringstream ss;

  if (util::toLower(query).find("limit") == std::string::npos) {
    query += " LIMIT 18446744073709551615";
  }

  query += " OFFSET " + std::to_string(row) + " LIMIT 1";

  auto esc = curl_easy_escape(_curl, query.c_str(), query.size());

  ss << _cache->getBackendURL() << "/?send=18446744073709551615"
     << "&query=" << esc;

  return ss.str();
}

// _____________________________________________________________________________
const ResObj Requestor::getNearest(util::geo::FPoint rp, double rad) const {
  // points

  auto box = pad(getBoundingBox(rp), rad);

  std::unordered_set<size_t> ret;
  _pgrid.get(box, &ret);

  size_t nearest = 0;
  double dBest = std::numeric_limits<double>::max();

  for (const auto& i : ret) {
    auto p = _cache->getPoints()[_points[i].first];

    double d = util::geo::dist(p, rp);

    if (d < dBest) {
      nearest = i;
      dBest = d;
    }
  }

  // lines
  size_t nearestL = 0;
  double dBestL = std::numeric_limits<double>::max();

  std::unordered_set<size_t> retL;
  _lgrid.get(box, &retL);

  for (const auto& i : retL) {
    auto l = _cache->getLines()[_lines[i].first];

    double d = util::geo::dist(l, rp);

    if (d < dBestL) {
      nearestL = i;
      dBestL = d;
    }
  }

  if (dBest < rad && dBest < dBestL) {
    return {true, _cache->getPoints()[_points[nearest].first],
            requestRow(_points[nearest].second)};
  }

  if (dBestL < rad && dBestL < dBest) {
    return {
        true,
        util::geo::PolyLine<float>(_cache->getLines()[_lines[nearestL].first])
            .projectOn(rp)
            .p,
        requestRow(_lines[nearestL].second)};
  }

  return {false, {0, 0}, {}};
}
