// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

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
using mapui::RequestReader;


// _____________________________________________________________________________
void Requestor::request(const std::string& qry) {
  _query = qry;
  _points.clear();
  _lines.clear();
  CURLcode res;

  RequestReader reader(_cache->getBackendURL());
  _query = qry;

  reader.requestIds(prepQuery(qry));

  LOG(INFO) << "Done, have " << reader.ids.size() << " ids in total.";

  // join with geoms from GeomCache

  // sort by qlever id
  LOG(INFO) << "Sorting results by qlever ID...";
  std::sort(reader.ids.begin(), reader.ids.end());
  LOG(INFO) << "... done";

  LOG(INFO) << "Retrieving geoms from cache...";
  // (geom id, result row)
  const auto& geoms = _cache->getRelObjects(reader.ids);
  LOG(INFO) << "... done, got " << geoms.size() << " geoms.";

  LOG(INFO) << "Calculating bounding box of result...";

  util::geo::FBox bbox;
  for (auto p : geoms) {
    auto geomId = p.first;
    auto rowId = p.second;

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

  double GRID_SIZE = 50000;

  _pgrid =
      mapui::Grid<size_t, util::geo::Point, float>(GRID_SIZE, GRID_SIZE, bbox);
  _lgrid =
      mapui::Grid<size_t, util::geo::Line, float>(GRID_SIZE, GRID_SIZE, bbox);
  _lpgrid = mapui::Grid<util::geo::FPoint, util::geo::Point, float>(
      GRID_SIZE, GRID_SIZE, bbox);

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
#pragma omp section
    {
      for (const auto& l : _lines) {
        for (const auto& p : _cache->getLines()[l.first]) {
          _lpgrid.add(p, util::geo::getBoundingBox(p), p);
        }
      }
    }
  }

  LOG(INFO) << "...done";
}

// _____________________________________________________________________________
std::vector<std::pair<std::string, std::string>> Requestor::requestRow(
    uint64_t row) const {
  RequestReader reader(_cache->getBackendURL());
  auto query = prepQueryRow(_query, row);

  reader.requestRows(query);

  return reader.cols;
}

// _____________________________________________________________________________
std::string Requestor::prepQuery(std::string query) const {
  // only use last column
  std::regex expr("select\\s*(\\?[A-Z0-9_\\-+]*\\s*)+\\s*where\\s*\\{",
                  std::regex_constants::icase);
  query = std::regex_replace(query, expr, "SELECT $1 WHERE {");

  if (util::toLower(query).find("limit") == std::string::npos) {
    query += " LIMIT 18446744073709551615";
  }

  return query;
}

// _____________________________________________________________________________
std::string Requestor::prepQueryRow(std::string query, uint64_t row) const {
  if (util::toLower(query).find("limit") == std::string::npos) {
    query += " LIMIT 18446744073709551615";
  }

  query += " OFFSET " + std::to_string(row) + " LIMIT 1";

  return query;
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
    if (!util::geo::contains(p, box)) continue;

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
    auto lBox = _cache->getLineBBox(_lines[i].first);
    if (!util::geo::intersects(lBox, box)) continue;

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
