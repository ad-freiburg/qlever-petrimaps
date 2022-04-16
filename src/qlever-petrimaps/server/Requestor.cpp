// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <cstring>
#include <iostream>
#include <parallel/algorithm>
#include <regex>
#include <sstream>

#include "qlever-petrimaps/Misc.h"
#include "qlever-petrimaps/server/Requestor.h"
#include "util/Misc.h"
#include "util/geo/Geo.h"
#include "util/geo/PolyLine.h"
#include "util/log/Log.h"

using petrimaps::GeomCache;
using petrimaps::Requestor;
using petrimaps::RequestReader;
using petrimaps::ResObj;

// _____________________________________________________________________________
void Requestor::request(const std::string& qry) {
  _query = qry;
  _points.clear();
  _lines.clear();
  _objects.clear();

  RequestReader reader(_cache->getBackendURL());
  _query = qry;

  LOG(INFO) << "[REQUESTOR] Requesting IDs for query " << qry;
  reader.requestIds(prepQuery(qry));

  LOG(INFO) << "[REQUESTOR] Done, have " << reader.ids.size()
            << " ids in total.";

  // join with geoms from GeomCache

  // sort by qlever id
  LOG(INFO) << "[REQUESTOR] Sorting results by qlever ID...";
  __gnu_parallel::sort(reader.ids.begin(), reader.ids.end());
  LOG(INFO) << "[REQUESTOR] ... done";

  LOG(INFO) << "[REQUESTOR] Retrieving geoms from cache...";
  // (geom id, result row)
  _objects = _cache->getRelObjects(reader.ids);
  LOG(INFO) << "[REQUESTOR] ... done, got " << _objects.size() << " objects.";

  LOG(INFO) << "[REQUESTOR] Calculating bounding box of result...";
  size_t NUM_THREADS = 24;
  std::vector<util::geo::FBox> bboxes(NUM_THREADS);
  util::geo::FBox bbox;

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
  for (size_t i = 0; i < _objects.size(); i++) {
    auto p = _objects[i];
    auto geomId = p.first;
    auto rowId = p.second;

    if (geomId < I_OFFSET) {
      auto pId = geomId;
      bboxes[omp_get_thread_num()] = util::geo::extendBox(
          _cache->getPointBBox(pId), bboxes[omp_get_thread_num()]);
    } else if (geomId < 2 * I_OFFSET) {
      auto lId = geomId;
      bboxes[omp_get_thread_num()] = util::geo::extendBox(
          _cache->getLineBBox(lId), bboxes[omp_get_thread_num()]);
    }
  }

  for (const auto& box : bboxes) {
    bbox = util::geo::extendBox(box, bbox);
  }

  LOG(INFO) << "[REQUESTOR] ... done";

  LOG(INFO) << "[REQUESTOR] BBox: " << util::geo::getWKT(bbox);
  LOG(INFO) << "[REQUESTOR] Building grid...";

  double GRID_SIZE = 100000;

  _pgrid = petrimaps::Grid<size_t, util::geo::Point, float>(GRID_SIZE,
                                                            GRID_SIZE, bbox);
  _lgrid = petrimaps::Grid<size_t, util::geo::Line, float>(GRID_SIZE, GRID_SIZE,
                                                           bbox);
  _lpgrid = petrimaps::Grid<util::geo::FPoint, util::geo::Point, float>(
      GRID_SIZE, GRID_SIZE, bbox);

#pragma omp parallel sections
  {
#pragma omp section
    {
      size_t i = 0;
      for (const auto& p : _objects) {
        auto geomId = p.first;
        if (geomId < I_OFFSET) {
          _pgrid.add(_cache->getPoints()[geomId], _cache->getPointBBox(geomId),
                     i);
        }
        i++;
      }
    }
#pragma omp section
    {
      size_t i = 0;
      for (const auto& l : _objects) {
        if (l.first >= I_OFFSET) {
          auto geomId = l.first - I_OFFSET;
          _lgrid.add(_cache->getLines()[geomId], _cache->getLineBBox(geomId), i);
          for (const auto& p : _cache->getLines()[geomId]) {
            _lpgrid.add(p, util::geo::getBoundingBox(p), p);
          }
        }
        i++;
      }
    }
  }

  LOG(INFO) << "[REQUESTOR] ...done";
}

// _____________________________________________________________________________
std::vector<std::pair<std::string, std::string>> Requestor::requestRow(
    uint64_t row) const {
  RequestReader reader(_cache->getBackendURL());
  LOG(INFO) << "[REQUESTOR] Requesting single row " << row << " for query "
            << _query;
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
  auto box = pad(getBoundingBox(rp), rad);

  size_t nearest = 0;
  double dBest = std::numeric_limits<double>::max();
  size_t nearestL = 0;
  double dBestL = std::numeric_limits<double>::max();
#pragma omp parallel sections
  {
#pragma omp section
    {
      // points

      std::unordered_set<size_t> ret;
      _pgrid.get(box, &ret);

      for (const auto& i : ret) {
        auto p = _cache->getPoints()[_objects[i].first];
        if (!util::geo::contains(p, box)) continue;

        double d = util::geo::dist(p, rp);

        if (d < dBest) {
          nearest = i;
          dBest = d;
        }
      }
    }

#pragma omp section
    {
      // lines
      std::unordered_set<size_t> retL;
      _lgrid.get(box, &retL);

      for (const auto& i : retL) {
        auto lBox = _cache->getLineBBox(_objects[i].first - I_OFFSET);
        if (!util::geo::intersects(lBox, box)) continue;

        auto l = _cache->getLines()[_objects[i].first - I_OFFSET];

        double d = util::geo::dist(l, rp);

        if (d < dBestL) {
          nearestL = i;
          dBestL = d;
        }
      }
    }
  }

    if (dBest < rad && dBest < dBestL) {
      return {true, _cache->getPoints()[_objects[nearest].first],
              requestRow(_objects[nearest].second)};
    }

    if (dBestL < rad && dBestL < dBest) {
      return {true,
              util::geo::PolyLine<float>(
                  _cache->getLines()[_objects[nearestL].first - I_OFFSET])
                  .projectOn(rp)
                  .p,
              requestRow(_objects[nearestL].second)};
    }

    return {false, {0, 0}, {}};
  }
