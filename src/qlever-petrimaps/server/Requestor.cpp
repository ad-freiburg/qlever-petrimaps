// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <regex>
#include <sstream>

#include "qlever-petrimaps/Misc.h"
#include "qlever-petrimaps/server/Requestor.h"
#include "util/Misc.h"
#include "util/geo/Geo.h"
#include "util/geo/PolyLine.h"
#include "util/log/Log.h"
#ifdef _OPENMP
#include <omp.h>
#else
#define omp_get_thread_num() 0
#endif

using petrimaps::GeomCache;
using petrimaps::Requestor;
using petrimaps::RequestReader;
using petrimaps::ResObj;
using util::LogLevel::ERROR;
using util::LogLevel::INFO;
using util::LogLevel::WARN;

// _____________________________________________________________________________
void Requestor::request(const std::string& remoteAddr) {
  std::lock_guard<std::mutex> guard(_m);

  if (_ready) return;

  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }

  _ready = false;
  _objects.clear();
  _clusterObjects.clear();

  RequestReader reader(_cache->getConfig().backend, _maxMemory,
                       _geomColumns.size(), _valueFlds.size(),
                       _rasterMetaFlds.size());

  _sortColumn = "";

  if (_geomColumns.size()) {
    _sortColumn = _geomColumns.front();

    auto wantCols = _geomColumns;

    // value columns come after geom columns
    wantCols.insert(wantCols.end(), _valueColumns.begin(), _valueColumns.end());

    // raster columns come after value columns
    wantCols.insert(wantCols.end(), _rasterMetaColumns.begin(),
                    _rasterMetaColumns.end());

    std::string prepedGeomQuery = prepQuery(_rcfg.query, wantCols, _sortColumn);

    LOG(INFO) << "[REQUESTOR] Requesting IDs/weights for query " << _rcfg.query;
    LOG(INFO) << "[REQUESTOR] Prepped query: " << prepedGeomQuery;

    reader.requestIds(prepedGeomQuery, remoteAddr);

    size_t totNumIds = 0;
    for (size_t i = 0; i < _geomColumns.size(); i++)
      totNumIds += reader._ids[i].size();
  }

  // join with geoms from GeomCache

  // sort by qlever id
  for (size_t i = 0; i < _geomColumns.size(); i++) {
    LOG(INFO) << "[REQUESTOR] Sorting " << reader._ids[i].size()
              << " results for column " << _geomColumns[i]
              << " by qlever ID...";
    std::sort(reader._ids[i].begin(), reader._ids[i].end());
  }
  LOG(INFO) << "[REQUESTOR] ... done";

  _objects.resize(_geomColumns.size());
  _vals.resize(_geomColumns.size());
  _valsMax.resize(_geomColumns.size(), 0);
  _valsMin.resize(_geomColumns.size(), 1);
  _rasterMetas.resize(_geomColumns.size());
  _dynamicPoints.resize(_geomColumns.size());
  _pgrid.resize(_geomColumns.size());
  _lgrid.resize(_geomColumns.size());
  _agrid.resize(_geomColumns.size());
  _lpgrid.resize(_geomColumns.size());
  _numObjects.resize(_geomColumns.size());
  _clusterObjects.resize(_geomColumns.size());

  for (size_t geomColId = 0; geomColId < _geomColumns.size(); geomColId++) {
    std::string fieldName = _geomColumns[geomColId];
    LOG(INFO) << "[REQUESTOR] Retrieving geoms from cache for field "
              << fieldName << "...";
    // (geom id, result row)
    const auto& ret = _cache->getRelObjects(reader._ids[geomColId]);
    _objects[geomColId] = ret.first;
    _numObjects[geomColId] = ret.second;

    if (_valueFlds.count(geomColId)) {
      _vals[geomColId] = std::move(reader._vals[_valueFlds[geomColId]]);

      _valsMin[geomColId] = std::numeric_limits<double>::max();
      _valsMax[geomColId] = std::numeric_limits<double>::lowest();

      for (auto v : _vals[geomColId]) {
        if (v < _valsMin[geomColId]) _valsMin[geomColId] = v;
        if (v > _valsMax[geomColId]) _valsMax[geomColId] = v;
      }
    }

    if (_rasterMetaFlds.count(geomColId)) {
      _rasterMetas[geomColId] =
          std::move(reader._rasterMetas[_rasterMetaFlds[geomColId]]);
    }

    LOG(INFO) << "[REQUESTOR] ... done, got " << _objects[geomColId].size()
              << " objects.";

    LOG(INFO) << "[REQUESTOR] Retrieving points dynamically from query...";

    // dynamic points present in query
    _dynamicPoints[geomColId] = getDynamicPoints(reader._ids[geomColId]);
    _numObjects[geomColId] += _dynamicPoints[geomColId].size();

    LOG(INFO) << "[REQUESTOR] ... done, got "
              << _dynamicPoints[geomColId].size() << " points.";

    LOG(INFO) << "[REQUESTOR] Calculating bounding box of result...";

    size_t NUM_THREADS = std::thread::hardware_concurrency();

    std::vector<util::geo::FBox> pointBoxes(NUM_THREADS);
    std::vector<util::geo::DBox> lineBoxes(NUM_THREADS);
    std::vector<size_t> numLines(NUM_THREADS, 0);
    util::geo::FBox pointBbox;
    util::geo::DBox lineBbox;
    size_t batch =
        ceil(static_cast<double>(_objects[geomColId].size()) / NUM_THREADS);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
    for (size_t t = 0; t < NUM_THREADS; t++) {
      for (size_t i = batch * t;
           i < batch * (t + 1) && i < _objects[geomColId].size(); i++) {
        auto geomId = _objects[geomColId][i].first;

        if (geomId < I_OFFSET) {
          auto pId = geomId;
          pointBoxes[t] =
              util::geo::extendBox(_cache->getPoints()[pId], pointBoxes[t]);
        } else if (geomId < std::numeric_limits<ID_TYPE>::max()) {
          auto lId = geomId - I_OFFSET;

          lineBoxes[t] =
              util::geo::extendBox(_cache->getLineBBox(lId), lineBoxes[t]);
          numLines[t]++;
        }
      }
    }

    batch = ceil(static_cast<double>(_dynamicPoints[geomColId].size()) /
                 NUM_THREADS);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
    for (size_t t = 0; t < NUM_THREADS; t++) {
      for (size_t i = batch * t;
           i < batch * (t + 1) && i < _dynamicPoints[geomColId].size(); i++) {
        auto geom = _dynamicPoints[geomColId][i].first;

        pointBoxes[t] = util::geo::extendBox(geom, pointBoxes[t]);
      }
    }

    for (const auto& box : pointBoxes) {
      pointBbox = util::geo::extendBox(box, pointBbox);
    }

    for (const auto& box : lineBoxes) {
      lineBbox = util::geo::extendBox(box, lineBbox);
    }

    // to avoid zero area boxes if only one point is requested
    pointBbox = util::geo::pad(pointBbox, 1);
    lineBbox = util::geo::pad(lineBbox, 1);

    LOG(INFO) << "[REQUESTOR] ... done";

    if (pointBbox.getLowerLeft().getX() > pointBbox.getUpperRight().getX()) {
      LOG(INFO) << "[REQUESTOR] Point BBox: <none>";
    } else {
      LOG(INFO) << "[REQUESTOR] Point BBox: " << util::geo::getWKT(pointBbox);
    }
    if (lineBbox.getLowerLeft().getX() > lineBbox.getUpperRight().getX()) {
      LOG(INFO) << "[REQUESTOR] Line BBox: <none>";
    } else {
      LOG(INFO) << "[REQUESTOR] Line BBox: " << util::geo::getWKT(lineBbox);
    }
    LOG(INFO) << "[REQUESTOR] Building grid...";

    double GRID_SIZE = 65536;

    double pw =
        pointBbox.getUpperRight().getX() - pointBbox.getLowerLeft().getX();
    double ph =
        pointBbox.getUpperRight().getY() - pointBbox.getLowerLeft().getY();

    // estimate memory consumption of empty grid
    double pxWidth = fmax(0, ceil(pw / GRID_SIZE));
    double pyHeight = fmax(0, ceil(ph / GRID_SIZE));

    double lw =
        lineBbox.getUpperRight().getX() - lineBbox.getLowerLeft().getX();
    double lh =
        lineBbox.getUpperRight().getY() - lineBbox.getLowerLeft().getY();

    // estimate memory consumption of empty grid
    double lxWidth = fmax(0, ceil(lw / GRID_SIZE));
    double lyHeight = fmax(0, ceil(lh / GRID_SIZE));

    LOG(INFO) << "[REQUESTOR] (" << pxWidth << "x" << pyHeight
              << " cell point grid)";
    LOG(INFO) << "[REQUESTOR] (" << lxWidth << "x" << lyHeight
              << " cell line grid)";

    checkMem(8 * (pxWidth * pyHeight), _maxMemory);
    checkMem(8 * (lxWidth * lyHeight), _maxMemory);
    checkMem(8 * (lxWidth * lyHeight), _maxMemory);
    // checkMem(8 * (lxWidth * lyHeight), _maxMemory);

    util::geo::FBox fLineBbox = {
        {lineBbox.getLowerLeft().getX(), lineBbox.getLowerLeft().getY()},
        {lineBbox.getUpperRight().getX(), lineBbox.getUpperRight().getY()}};

    _pgrid[geomColId] =
        petrimaps::Grid<ID_TYPE, float, float>(GRID_SIZE, GRID_SIZE, pointBbox);
    _lgrid[geomColId] =
        petrimaps::Grid<ID_TYPE, float, float>(GRID_SIZE, GRID_SIZE, fLineBbox);
    _agrid[geomColId] =
        petrimaps::Grid<ID_TYPE, float, float>(GRID_SIZE, GRID_SIZE, fLineBbox);
    _lpgrid[geomColId] =
        petrimaps::Grid<util::geo::Point<uint8_t>, float, float>(
            GRID_SIZE, GRID_SIZE, fLineBbox);

    std::exception_ptr ePtr1, ePtr2, ePtr3, ePtr4;

#pragma omp parallel sections
    {
#pragma omp section
      {
        size_t j =
            _objects[geomColId].size() + _dynamicPoints[geomColId].size();

        for (size_t oid = 0; oid < _objects[geomColId].size(); oid++) {
          const auto& p = _objects[geomColId][oid];
          auto geomId = p.first;
          if (geomId >= I_OFFSET) continue;

          size_t clusterI = 0;
          // cluster if they have same geometry, don't do for multigeoms
          while (oid < _objects[geomColId].size() - 1 &&
                 geomId == _objects[geomColId][oid + 1].first) {
            clusterI++;
            oid++;
          }

          if (clusterI > 0) {
            for (size_t m = 0; m < clusterI; m++) {
              const auto& p = _objects[geomColId][oid - m];
              _pgrid[geomColId].add(_cache->getPoints()[p.first],
                                    getVal(geomColId, oid - m), j);
              _clusterObjects[geomColId].push_back({oid - m, {m, clusterI}});
              j++;
            }
          } else {
            _pgrid[geomColId].add(_cache->getPoints()[geomId],
                                  getVal(geomColId, oid), oid);
          }

          // every 100000 objects, check memory...
          if (oid % 100000 == 0) {
            try {
              checkMem(1, _maxMemory);
            } catch (...) {
              ePtr1 = std::current_exception();
              break;
            }
          }
        }

        for (size_t i = 0; i < _dynamicPoints[geomColId].size(); i++) {
          const auto& p = _dynamicPoints[geomColId][i];
          auto geom = p.first;

          size_t clusterI = 0;
          // cluster if they have same geometry, don't do for multigeoms
          while (i < _dynamicPoints[geomColId].size() - 1 &&
                 geom == _dynamicPoints[geomColId][i + 1].first) {
            clusterI++;
            i++;
          }

          if (clusterI > 0) {
            for (size_t m = 0; m < clusterI; m++) {
              const auto& p = _dynamicPoints[geomColId][i - m];
              auto geom = p.first;
              _pgrid[geomColId].add(geom, getVal(geomColId, j), j);
              _clusterObjects[geomColId].push_back(
                  {i - m + _objects[geomColId].size(), {m, clusterI}});
              j++;
            }
          } else {
            _pgrid[geomColId].add(
                geom, getVal(geomColId, i + _objects[geomColId].size()),
                i + _objects[geomColId].size());
          }

          // every 100000 objects, check memory...
          if (i % 100000 == 0) {
            try {
              checkMem(1, _maxMemory);
            } catch (...) {
              ePtr2 = std::current_exception();
              break;
            }
          }
        }
      }

#pragma omp section
      {
        size_t i = 0;
        for (const auto& l : _objects[geomColId]) {
          if (l.first >= I_OFFSET &&
              l.first < std::numeric_limits<ID_TYPE>::max()) {
            auto geomId = l.first - I_OFFSET;
            auto box = _cache->getLineBBox(geomId);
            util::geo::FBox fbox = {
                {box.getLowerLeft().getX(), box.getLowerLeft().getY()},
                {box.getUpperRight().getX(), box.getUpperRight().getY()}};
            _lgrid[geomColId].add(fbox, getVal(geomColId, i), i);
          }
          i++;

          // every 100000 objects, check memory...
          if (i % 100000 == 0) {
            try {
              checkMem(1, _maxMemory);
            } catch (...) {
              ePtr3 = std::current_exception();
              break;
            }
          }
        }
      }

#pragma omp section
      {
        size_t i = 0;
        for (const auto& l : _objects[geomColId]) {
          if (l.first >= I_OFFSET &&
              l.first < std::numeric_limits<ID_TYPE>::max()) {
            auto geomId = l.first - I_OFFSET;
            bool lineIsArea = isArea(geomId);

            util::geo::FPolygon poly;

            size_t start = _cache->getLine(geomId);
            size_t end = _cache->getLineEnd(geomId);

            double mainX = 0;
            double mainY = 0;

            size_t gi = 0;

            int lastX = 0;
            int lastY = 0;

            for (size_t li = start; li < end; li++) {
              const auto& cur = _cache->getLinePoints()[li];

              if (isMCoord(cur.getX())) {
                mainX = rmCoord(cur.getX());
                mainY = rmCoord(cur.getY());
                continue;
              }

              // skip bounding box at beginning
              if (++gi < 3) continue;

              // extract real geometry
              util::geo::FPoint curP(
                  (mainX * M_COORD_GRANULARITY + cur.getX()) / 10.0,
                  (mainY * M_COORD_GRANULARITY + cur.getY()) / 10.0);

              if (lineIsArea) poly.getOuter().push_back(curP);

              size_t cellX = _lpgrid[geomColId].getCellXFromX(curP.getX());
              size_t cellY = _lpgrid[geomColId].getCellYFromY(curP.getY());

              uint8_t sX = (curP.getX() -
                            _lpgrid[geomColId].getBBox().getLowerLeft().getX() +
                            cellX * _lpgrid[geomColId].getCellWidth()) /
                           256;
              uint8_t sY = (curP.getY() -
                            _lpgrid[geomColId].getBBox().getLowerLeft().getY() +
                            cellY * _lpgrid[geomColId].getCellHeight()) /
                           256;

              const auto& cellBox = _lpgrid[geomColId].getBox(cellX, cellY);

              int fullX = cellBox.getLowerLeft().getX() + sX * 256;
              int fullY = cellBox.getLowerLeft().getY() + sY * 256;

              if (gi == 3 || lastX != fullX || lastY != fullY) {
                _lpgrid[geomColId].add(cellX, cellY, getVal(geomColId, i),
                                       {sX, sY});
                lastX = fullX;
                lastY = fullY;
              }
            }

            if (lineIsArea && util::geo::area(poly) > (2000.0 * 2000.0)) {
              auto fbox = util::geo::getBoundingBox(poly);
              _agrid[geomColId].add(fbox, getVal(geomColId, i), i);
            }
          }
          i++;

          // every 100000 objects, check memory...
          if (i % 100000 == 0) {
            try {
              checkMem(1, _maxMemory);
            } catch (...) {
              ePtr4 = std::current_exception();
              break;
            }
          }
        }
      }
    }

    // unroll exceptions
    if (ePtr1) std::rethrow_exception(ePtr1);
    if (ePtr2) std::rethrow_exception(ePtr2);
    if (ePtr3) std::rethrow_exception(ePtr3);
    if (ePtr4) std::rethrow_exception(ePtr4);
  }

  _ready = true;

  LOG(INFO) << "[REQUESTOR] ...done";
}

// _____________________________________________________________________________
std::vector<std::pair<std::string, std::string>> Requestor::requestRow(
    uint64_t row, const std::string& remoteAddr) const {
  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }
  RequestReader reader(_cache->getConfig().backend, _maxMemory, 0, 0, 0);
  LOG(INFO) << "[REQUESTOR] Requesting single row " << row << " for query "
            << _rcfg.query;
  auto query = prepQueryRow(_rcfg.query, row);

  LOG(INFO) << "[REQUESTOR] Row query is " << query;

  reader.requestRows(query, remoteAddr);

  if (reader.rows.size() == 0) return {};

  return reader.rows[0];
}

// _____________________________________________________________________________
void Requestor::requestRows(
    std::function<
        void(std::vector<std::vector<std::pair<std::string, std::string>>>)>
        cb,
    const std::string& remoteAddr) const {
  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }
  RequestReader reader(_cache->getConfig().backend, _maxMemory, 0, 0, 0);
  LOG(INFO) << "[REQUESTOR] Requesting rows for query " << _rcfg.query;

  reader.requestRows(
      _rcfg.query,
      [&reader, &cb](const char* c, size_t n) {
        // parse this block of rows and give them to the callback
        reader.rows = {};
        reader.parse(c, n);
        cb(reader.rows);
      },
      remoteAddr);
}

// _____________________________________________________________________________
std::vector<std::string> Requestor::getColumns(std::string query) const {
  std::regex expr("select[^{]*(\\*|[\\?$][A-Z0-9_\\-+]*)+[^{]*\\s*\\{",
                  std::regex_constants::icase);

  query = std::regex_replace(query, expr, "SELECT * WHERE {$&",
                             std::regex_constants::format_first_only) +
          "}";

  query += " LIMIT 0";

  RequestReader reader(_cache->getConfig().backend, _maxMemory, 0, 0, 0);
  return reader.requestColumns(query);
}

// _____________________________________________________________________________
std::string Requestor::prepQuery(std::string query,
                                 std::vector<std::string> columns,
                                 std::string sortBy) const {
  std::vector<std::string> rawCols;
  for (const auto& col : columns) rawCols.push_back(util::split(col, ':')[0]);

  std::regex expr("select[^{]*(\\*|[\\?$][A-Z0-9_\\-+]*)+[^{]*\\s*\\{",
                  std::regex_constants::icase);

  query =
      std::regex_replace(query, expr,
                         "SELECT " + util::implode(rawCols, " ") + " WHERE {$&",
                         std::regex_constants::format_first_only) +
      "}";

  if (sortBy.size()) query += " INTERNAL SORT BY " + sortBy;
  query += " LIMIT 18446744073709551615";

  return query;
}

// _____________________________________________________________________________
std::string Requestor::prepQueryRow(std::string query, uint64_t row) const {
  // replace first select
  std::regex expr("select[^{]*(\\*|[\\?$][A-Z0-9_\\-+]*)+[^{]*\\s*\\{",
                  std::regex_constants::icase);

  query = std::regex_replace(query, expr, "SELECT * {$&",
                             std::regex_constants::format_first_only) +
          "}";
  if (_sortColumn.size()) query += " INTERNAL SORT BY " + _sortColumn;
  query += " OFFSET " + std::to_string(row) + " LIMIT 1";
  return query;
}

// _____________________________________________________________________________
const ResObj Requestor::getNearest(util::geo::DPoint rp, double rad, double res,
                                   util::geo::FBox fullbox,
                                   const std::string& remoteAddr) const {
  for (size_t lid = 0; lid < getNumFields(); lid++) {
    auto r = getNearest(lid, rp, rad, res, fullbox, remoteAddr);
    if (r.has) return r;
  }

  return {false, 0, 0, {0, 0}, {}, {}, {}, {}};
}

// _____________________________________________________________________________
const ResObj Requestor::getNearest(size_t fieldId, util::geo::DPoint rp,
                                   double rad, double res,
                                   util::geo::FBox fullbox,
                                   const std::string& remoteAddr) const {
  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }
  auto box = pad(getBoundingBox(rp), rad);
  auto fbox = pad(getBoundingBox(util::geo::FPoint(rp.getX(), rp.getY())), rad);

  auto frp = util::geo::FPoint{rp.getX(), rp.getY()};

  size_t NUM_THREADS = std::thread::hardware_concurrency();

  size_t nearest = 0;
  double dBest = std::numeric_limits<double>::max();
  std::vector<size_t> nearestVec(NUM_THREADS, 0);
  std::vector<double> dBestVec(NUM_THREADS, std::numeric_limits<double>::max());

  std::vector<size_t> nearestLVec(NUM_THREADS, 0);
  std::vector<double> dBestLVec(NUM_THREADS,
                                std::numeric_limits<double>::max());
  size_t nearestL = 0;
  double dBestL = std::numeric_limits<double>::max();
#pragma omp parallel sections
  {
#pragma omp section
    {
      // points

      std::vector<ID_TYPE> ret;

      if (res > 0)
        _pgrid[fieldId].get(fullbox, &ret);
      else
        _pgrid[fieldId].get(fbox, &ret);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
      for (size_t idx = 0; idx < ret.size(); idx++) {
        auto oid = ret[idx];
        util::geo::FPoint p;
        if (isCluster(fieldId, oid)) {
          auto dp = clusterGeom(fieldId, oid, res);
          p = {dp.getX(), dp.getY()};
        } else {
          p = getPoint(fieldId, oid);
        }

        if (!util::geo::contains(p, fbox)) continue;

        double d = util::geo::dist(p, frp);

        if (d < dBestVec[omp_get_thread_num()]) {
          nearestVec[omp_get_thread_num()] = oid;
          dBestVec[omp_get_thread_num()] = d;
        }
      }
    }

#pragma omp section
    {
      // lines
      std::vector<ID_TYPE> retL;
      _lgrid[fieldId].get(fbox, &retL);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
      for (size_t idx = 0; idx < retL.size(); idx++) {
        const auto& oid = retL[idx];
        auto lBox =
            _cache->getLineBBox(_objects[fieldId][oid].first - I_OFFSET);
        if (!util::geo::intersects(lBox, box)) continue;

        size_t start = _cache->getLine(_objects[fieldId][oid].first - I_OFFSET);
        size_t end =
            _cache->getLineEnd(_objects[fieldId][oid].first - I_OFFSET);

        // TODO _____________________ own function
        double d = std::numeric_limits<double>::infinity();

        util::geo::DPoint curPa, curPb;
        int s = 0;

        size_t gi = 0;

        double mainX = 0;
        double mainY = 0;

        bool isArea =
            Requestor::isArea(_objects[fieldId][oid].first - I_OFFSET);

        util::geo::DLine areaBorder;

        for (size_t i = start; i < end; i++) {
          // extract real geom
          const auto& cur = _cache->getLinePoints()[i];

          if (isMCoord(cur.getX())) {
            mainX = rmCoord(cur.getX());
            mainY = rmCoord(cur.getY());
            continue;
          }

          // skip bounding box at beginning
          gi++;
          if (gi < 3) continue;

          // extract real geometry
          util::geo::DPoint curP(
              (mainX * M_COORD_GRANULARITY + cur.getX()) / 10.0,
              (mainY * M_COORD_GRANULARITY + cur.getY()) / 10.0);

          if (isArea) areaBorder.push_back(curP);

          if (s == 0) {
            curPa = curP;
            s++;
          } else if (s == 1) {
            curPb = curP;
            s++;
          }

          if (s == 2) {
            s = 1;
            double dTmp = util::geo::distToSegment(curPa, curPb, rp);
            if (dTmp < 0.0001) {
              d = 0;
              break;
            }
            curPa = curPb;
            if (dTmp < d) d = dTmp;
          }
        }
        // TODO _____________________ own function

        if (isArea) {
          if (util::geo::contains(rp, util::geo::DPolygon(areaBorder))) {
            // set it to rad/4 - this allows selecting smaller objects
            // inside the polgon
            d = rad / 4;
          }
        }

        if (d < dBestLVec[omp_get_thread_num()]) {
          nearestLVec[omp_get_thread_num()] = oid;
          dBestLVec[omp_get_thread_num()] = d;
        }
      }
    }
  }

  // join threads
  for (size_t i = 0; i < NUM_THREADS; i++) {
    if (dBestVec[i] < dBest) {
      dBest = dBestVec[i];
      nearest = nearestVec[i];
    }

    if (dBestLVec[i] < dBestL) {
      dBestL = dBestLVec[i];
      nearestL = nearestLVec[i];
    }
  }

  if (dBest < rad && dBest <= dBestL) {
    size_t row = getRow(fieldId, nearest);
    auto points = geomPointGeoms(fieldId, nearest, res);

    return {true,
            nearest,
            fieldId,
            points.size() == 1 ? points[0] : util::geo::centroid(points),
            requestRow(row, remoteAddr),
            points,
            geomLineGeoms(fieldId, nearest, rad / 10),
            geomPolyGeoms(fieldId, nearest, rad / 10)};
  }

  if (dBestL < rad && dBestL <= dBest) {
    size_t lineId = _objects[fieldId][nearestL].first - I_OFFSET;
    const auto& dline = extractLineGeom(lineId);

    if (Requestor::isArea(lineId) &&
        util::geo::contains(rp, util::geo::DPolygon(dline))) {
      return {true,
              nearestL,
              fieldId,
              {frp.getX(), frp.getY()},
              requestRow(_objects[fieldId][nearestL].second, remoteAddr),
              geomPointGeoms(fieldId, nearestL, res),
              geomLineGeoms(fieldId, nearestL, rad / 10),
              geomPolyGeoms(fieldId, nearestL, rad / 10)};
    } else {
      auto p = util::geo::PolyLine<double>(dline).projectOn(rp).p;
      auto fp = util::geo::DPoint(p.getX(), p.getY());
      return {true,
              nearestL,
              fieldId,
              fp,
              requestRow(_objects[fieldId][nearestL].second, remoteAddr),
              geomPointGeoms(fieldId, nearestL, res),
              geomLineGeoms(fieldId, nearestL, rad / 10),
              geomPolyGeoms(fieldId, nearestL, rad / 10)};
    }
  }

  return {false, 0, 0, {0, 0}, {}, {}, {}, {}};
}

// _____________________________________________________________________________
const ResObj Requestor::getGeom(size_t fieldId, size_t id, double rad) const {
  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }

  return {true,
          id,
          fieldId,
          {0, 0},
          {},
          geomPointGeoms(fieldId, id, rad / 10),
          geomLineGeoms(fieldId, id, rad / 10),
          geomPolyGeoms(fieldId, id, rad / 10)};
}

// _____________________________________________________________________________
util::geo::DLine Requestor::extractLineGeom(size_t lineId, double minD) const {
  util::geo::DLine dline;

  size_t start = _cache->getLine(lineId);
  size_t end = _cache->getLineEnd(lineId);

  double mainX = 0;
  double mainY = 0;

  size_t gi = 0;

  for (size_t i = start; i < end; i++) {
    // extract real geom
    const auto& cur = _cache->getLinePoints()[i];

    if (isMCoord(cur.getX())) {
      mainX = rmCoord(cur.getX());
      mainY = rmCoord(cur.getY());
      continue;
    }

    // skip bounding box at beginning
    gi++;
    if (gi < 3) continue;

    util::geo::DPoint curP((mainX * M_COORD_GRANULARITY + cur.getX()) / 10.0,
                           (mainY * M_COORD_GRANULARITY + cur.getY()) / 10.0);

    if (dline.size() && minD > 0 && i < end - 1 &&
        util::geo::dist(dline.back(), curP) < minD)
      continue;
    dline.push_back(curP);
  }

  return dline;
}

// _____________________________________________________________________________
bool Requestor::isArea(size_t lineId) const {
  size_t end = _cache->getLineEnd(lineId);

  if (end == 0) return false;

  return isMCoord(_cache->getLinePoints()[end - 1].getX());
}

// _____________________________________________________________________________
bool Requestor::isInnerArea(size_t lineId) const {
  size_t end = _cache->getLineEnd(lineId);

  if (end == 0) return false;

  return isMCoord(_cache->getLinePoints()[end - 1].getX()) &&
         rmCoord(_cache->getLinePoints()[end - 1].getX()) == 1;
}

// _____________________________________________________________________________
util::geo::MultiLine<double> Requestor::geomLineGeoms(size_t fieldId,
                                                      size_t oid,
                                                      double eps) const {
  std::vector<util::geo::DLine> polys;

  // catch multigeometries
  for (size_t i = oid;
       i < _objects[fieldId].size() &&
       _objects[fieldId][i].second == _objects[fieldId][oid].second;
       i++) {
    if (_objects[fieldId][i].first < I_OFFSET ||
        Requestor::isArea(_objects[fieldId][i].first - I_OFFSET))
      continue;
    const auto& fline = extractLineGeom(_objects[fieldId][i].first - I_OFFSET);
    polys.push_back(util::geo::simplify(fline, eps));
  }

  if (oid > 0) {
    for (size_t i = oid - 1;
         i < _objects[fieldId].size() &&
         _objects[fieldId][i].second == _objects[fieldId][oid].second;
         i--) {
      if (_objects[fieldId][i].first < I_OFFSET ||
          Requestor::isArea(_objects[fieldId][i].first - I_OFFSET))
        continue;
      const auto& fline =
          extractLineGeom(_objects[fieldId][i].first - I_OFFSET);
      polys.push_back(util::geo::simplify(fline, eps));
    }
  }

  return polys;
}

// _____________________________________________________________________________
util::geo::MultiPoint<double> Requestor::geomPointGeoms(size_t fieldId,
                                                        size_t oid) const {
  return geomPointGeoms(fieldId, oid, -1);
}

// _____________________________________________________________________________
util::geo::MultiPoint<double> Requestor::geomPointGeoms(size_t fieldId,
                                                        size_t oid,
                                                        double res) const {
  std::vector<util::geo::DPoint> points;

  if (!(res < 0) && isCluster(fieldId, oid)) {
    return {clusterGeom(fieldId, oid, res)};
  }

  if (isCluster(fieldId, oid)) {
    oid = getCluster(fieldId, oid).first;
  }

  if (oid >= _objects[fieldId].size()) {
    points.push_back(
        {_dynamicPoints[fieldId][oid - _objects[fieldId].size()].first.getX(),
         _dynamicPoints[fieldId][oid - _objects[fieldId].size()].first.getY()});
    return points;
  }

  // catch multigeometries, not relevant for dynamic points
  for (size_t i = oid;
       i < _objects[fieldId].size() &&
       _objects[fieldId][i].second == _objects[fieldId][oid].second;
       i++) {
    if (_objects[fieldId][i].first >= I_OFFSET) continue;
    auto p = _cache->getPoints()[_objects[fieldId][i].first];
    points.push_back({p.getX(), p.getY()});
  }

  if (oid > 0) {
    for (size_t i = oid - 1;
         i < _objects[fieldId].size() &&
         _objects[fieldId][i].second == _objects[fieldId][oid].second;
         i--) {
      if (_objects[fieldId][i].first >= I_OFFSET) continue;
      auto p = _cache->getPoints()[_objects[fieldId][i].first];
      points.push_back({p.getX(), p.getY()});
    }
  }

  return points;
}

// _____________________________________________________________________________
util::geo::MultiPolygon<double> Requestor::geomPolyGeoms(size_t fieldId,
                                                         size_t oid,
                                                         double eps) const {
  std::vector<util::geo::DPolygon> polys;

  // catch multigeometries
  for (size_t i = oid;
       i < _objects[fieldId].size() &&
       _objects[fieldId][i].second == _objects[fieldId][oid].second;
       i++) {
    if (_objects[fieldId][i].first < I_OFFSET ||
        !Requestor::isArea(_objects[fieldId][i].first - I_OFFSET))
      continue;
    const auto& dline = extractLineGeom(_objects[fieldId][i].first - I_OFFSET);
    polys.push_back(util::geo::DPolygon(util::geo::simplify(dline, eps)));
  }

  if (oid > 0) {
    for (size_t i = oid - 1;
         i < _objects[fieldId].size() &&
         _objects[fieldId][i].second == _objects[fieldId][oid].second;
         i--) {
      if (_objects[fieldId][i].first < I_OFFSET ||
          !Requestor::isArea(_objects[fieldId][i].first - I_OFFSET))
        continue;
      const auto& dline =
          extractLineGeom(_objects[fieldId][i].first - I_OFFSET);
      polys.push_back(util::geo::DPolygon(util::geo::simplify(dline, eps)));
    }
  }

  return polys;
}

// _____________________________________________________________________________
std::vector<std::pair<util::geo::FPoint, ID_TYPE>> Requestor::getDynamicPoints(
    const std::vector<IdMapping>& ids) const {
  std::vector<std::pair<util::geo::FPoint, ID_TYPE>> ret;

  size_t count = 0;

  for (const auto& p : ids) {
    uint8_t type = (p.qid & (uint64_t(15) << 60)) >> 60;
    if (type == 8) count++;  // 8 = Geopoint in Qlever
  }

  checkMem(sizeof(std::pair<util::geo::FPoint, ID_TYPE>) * count, _maxMemory);
  ret.reserve(count);

  for (const auto& p : ids) {
    uint8_t type = (p.qid & (uint64_t(15) << 60)) >> 60;
    if (type != 8) continue;  // 8 = Geopoint in Qlever

    uint64_t maskLng = 1073741823;
    uint64_t maskLat = static_cast<uint64_t>(1073741823) << 30;

    auto lng =
        ((static_cast<double>((p.qid & maskLng)) / maskLng) * 2 * 180.0) -
        180.0;
    auto lat =
        ((static_cast<double>((p.qid & maskLat) >> 30) / maskLng) * 2 * 90.0) -
        90.0;

    ret.push_back(
        {util::geo::latLngToWebMerc(util::geo::FPoint{lng, lat}), p.id});
  }

  return ret;
}

// _____________________________________________________________________________
util::geo::DPoint Requestor::clusterGeom(size_t fieldId, size_t oid,
                                         double res) const {
  size_t cid =
      oid - getObjects(fieldId).size() - getDynamicPoints(fieldId).size();
  size_t refOid = _clusterObjects[fieldId][cid].first;

  util::geo::FPoint pp = getPoint(fieldId, refOid);

  if (res < 0) return {pp.getX(), pp.getY()};

  size_t num = _clusterObjects[fieldId][cid].second.first;
  size_t tot = _clusterObjects[fieldId][cid].second.second;

  double a = 25;
  double b = 6;

  if (tot > a) {
    double rad = 2 * a;

    int row = ((-a - b / 2.0) + sqrt((a + b / 2.0) * (a + b / 2.0) +
                                     2.0 * b * (std::max(0.0, num - a + 2)))) /
              b;

    double g = b * ((row * row + row) / 2.0);

    double relpos = num - (a * row + (g - row * b));
    double tot = a + row * b;

    double x = pp.getX() + (rad + row * 13.0) * res *
                               sin(relpos * (2.0 * 3.14159265359 / tot));
    double y = pp.getY() + (rad + row * 13.0) * res *
                               cos(relpos * (2.0 * 3.14159265359 / tot));

    return util::geo::DPoint{x, y};
  } else {
    float rad = 2 * tot;

    float x = pp.getX() + rad * res * sin(num * (2 * 3.14159265359 / tot));
    float y = pp.getY() + rad * res * cos(num * (2 * 3.14159265359 / tot));

    return util::geo::DPoint{x, y};
  }
}

// _____________________________________________________________________________
bool Requestor::lineIntersects(size_t lineId,
                               const util::geo::DBox& bbox) const {
  const auto& lbox = getLineBBox(lineId - I_OFFSET);
  if (!util::geo::intersects(lbox, bbox)) return false;
  size_t start = getLine(lineId - I_OFFSET);
  size_t end = getLineEnd(lineId - I_OFFSET);

  util::geo::DPoint curPa, curPb;
  int s = 0;
  size_t gi = 0;

  double mainX = 0;
  double mainY = 0;
  for (size_t i = start; i < end; i++) {
    // extract real geom
    const auto& cur = getLinePoints()[i];

    if (isMCoord(cur.getX())) {
      mainX = rmCoord(cur.getX());
      mainY = rmCoord(cur.getY());
      continue;
    }

    // skip bounding box at beginning
    gi++;
    if (gi < 3) continue;

    // extract real geometry
    const util::geo::DPoint curP(
        (mainX * M_COORD_GRANULARITY + cur.getX()) / 10.0,
        (mainY * M_COORD_GRANULARITY + cur.getY()) / 10.0);
    if (s == 0) {
      curPa = curP;
      s++;
    } else if (s == 1) {
      curPb = curP;
      s++;
    }

    if (s == 2) {
      s = 1;
      if (util::geo::intersects(util::geo::LineSegment<double>(curPa, curPb),
                                bbox)) {
        return true;
      }
      curPa = curPb;
    }
  }
  return false;
}

// _____________________________________________________________________________
std::pair<double, double> Requestor::getValRange(size_t fid) const {
  if (_valsMin[fid] >= _valsMax[fid]) return {0, 0};
  return {_valsMin[fid], _valsMax[fid]};
}

// _____________________________________________________________________________
std::pair<double, double> Requestor::getRasterMetas(
    size_t fieldId, size_t oid, std::pair<double, double> def) const {
  if (oid < _objects[fieldId].size()) {
    if (_objects[fieldId][oid].second >= _rasterMetas[fieldId].size())
      return def;
    size_t did = _rasterMetas[fieldId][_objects[fieldId][oid].second];
    return _cache->getRasterMeta(did);
  }
  if (oid >= _objects[fieldId].size()) {
    if (_dynamicPoints[fieldId][oid - _objects[fieldId].size()].second >=
        _rasterMetas[fieldId].size())
      return def;
    size_t did =
        _rasterMetas[fieldId]
                    [_dynamicPoints[fieldId][oid - _objects[fieldId].size()]
                         .second];
    return _cache->getRasterMeta(did);
  }

  return def;
}

// _____________________________________________________________________________
double Requestor::getVal(size_t fieldId, size_t oid) const {
  // shortcut
  if (_vals[fieldId].size() == 0) return 1;

  if (oid < _objects[fieldId].size()) {
    if (_objects[fieldId][oid].second >= _vals[fieldId].size()) return 1;
    return _vals[fieldId][_objects[fieldId][oid].second];
  }
  if (oid >= _objects[fieldId].size()) {
    if (_dynamicPoints[fieldId][oid - _objects[fieldId].size()].second >=
        _vals[fieldId].size())
      return 1;
    return _vals[fieldId]
                [_dynamicPoints[fieldId][oid - _objects[fieldId].size()]
                     .second];
  }

  return 1;
}
