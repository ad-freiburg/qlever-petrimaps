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
                       _geomColumns.size(), _valueColumns.size(),
                       _rasterMetaColumns.size());

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
  }

  // join with geoms from GeomCache

  // sort by qlever id
  for (size_t gid = 0; gid < _geomColumns.size(); gid++) {
    LOG(INFO) << "[REQUESTOR] Sorting " << reader._ids[gid].size()
              << " results for column " << _geomColumns[gid]
              << " by qlever ID...";
    std::sort(reader._ids[gid].begin(), reader._ids[gid].end());
  }
  LOG(INFO) << "[REQUESTOR] ... done";

  _objects.resize(_geomColumns.size());
  _dynamicPoints.resize(_geomColumns.size());
  _clusterObjects.resize(_geomColumns.size());
  _numObjects.resize(_geomColumns.size());

  _vals.resize(_valueColumns.size());
  _valsMax.resize(_valueColumns.size(), 0);
  _valsMin.resize(_valueColumns.size(), 1);

  _rasterMetas.resize(_rasterMetaColumns.size());

  _pgrid.resize(_gridSets.size());
  _lgrid.resize(_gridSets.size());
  _agrid.resize(_gridSets.size());
  _lpgrid.resize(_gridSets.size());

  // move the values, and calculate their range
  for (size_t vid = 0; vid < _valueColumns.size(); vid++) {
    _vals[vid] = std::move(reader._vals[vid]);

    _valsMin[vid] = std::numeric_limits<double>::max();
    _valsMax[vid] = std::numeric_limits<double>::lowest();

    for (auto v : _vals[vid]) {
      if (v < _valsMin[vid]) _valsMin[vid] = v;
      if (v > _valsMax[vid]) _valsMax[vid] = v;
    }
  }

  // move the raster metas
  for (size_t rid = 0; rid < _rasterMetaColumns.size(); rid++) {
    _rasterMetas[rid] = std::move(reader._rasterMetas[rid]);
  }

  // bounding boxes of the geometries, per distinct geo column
  std::vector<util::geo::FBox> pointBboxes(_geomColumns.size());
  std::vector<util::geo::FBox> lineBboxes(_geomColumns.size());

  for (size_t gid = 0; gid < _geomColumns.size(); gid++) {
    const std::string& fieldName = _geomColumns[gid];
    LOG(INFO) << "[REQUESTOR] Retrieving geoms from cache for field "
              << fieldName << "...";
    // (geom id, result row)
    const auto& ret = _cache->getRelObjects(reader._ids[gid]);
    _objects[gid] = ret.first;
    _numObjects[gid] = ret.second;

    LOG(INFO) << "[REQUESTOR] ... done, got " << _objects[gid].size()
              << " objects.";

    LOG(INFO) << "[REQUESTOR] Retrieving points dynamically from query...";

    // dynamic points present in query
    _dynamicPoints[gid] = getDynamicPoints(reader._ids[gid]);
    _numObjects[gid] += _dynamicPoints[gid].size();

    LOG(INFO) << "[REQUESTOR] ... done, got " << _dynamicPoints[gid].size()
              << " points.";

    LOG(INFO) << "[REQUESTOR] Calculating bounding box of result...";

    size_t NUM_THREADS = std::thread::hardware_concurrency();

    std::vector<util::geo::FBox> pointBoxes(NUM_THREADS);
    std::vector<util::geo::DBox> lineBoxes(NUM_THREADS);
    std::vector<size_t> numLines(NUM_THREADS, 0);
    util::geo::FBox pointBbox;
    util::geo::DBox lineBbox;
    size_t batch =
        ceil(static_cast<double>(_objects[gid].size()) / NUM_THREADS);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
    for (size_t t = 0; t < NUM_THREADS; t++) {
      for (size_t i = batch * t;
           i < batch * (t + 1) && i < _objects[gid].size(); i++) {
        auto geomId = _objects[gid][i].first;

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

    batch = ceil(static_cast<double>(_dynamicPoints[gid].size()) / NUM_THREADS);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
    for (size_t t = 0; t < NUM_THREADS; t++) {
      for (size_t i = batch * t;
           i < batch * (t + 1) && i < _dynamicPoints[gid].size(); i++) {
        auto geom = _dynamicPoints[gid][i].first;

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

    pointBboxes[gid] = pointBbox;
    lineBboxes[gid] = util::geo::FBox{
        {lineBbox.getLowerLeft().getX(), lineBbox.getLowerLeft().getY()},
        {lineBbox.getUpperRight().getX(), lineBbox.getUpperRight().getY()}};
  }

  // the clusters only depend on the geometries, they are built togethr with
  // the first grid set of a geom column, no need to do that multiple times
  std::vector<bool> clustersDone(_geomColumns.size(), false);

  for (size_t gsid = 0; gsid < _gridSets.size(); gsid++) {
    const size_t gid = _gridSets[gsid].first;
    const size_t vid = _gridSets[gsid].second;

    const bool buildClusters = !clustersDone[gid];
    clustersDone[gid] = true;

    LOG(INFO) << "[REQUESTOR] Building grid for geom column "
              << _geomColumns[gid] << " weighted by "
              << (vid == NO_COL ? "-" : _valueColumns[vid]) << "...";

    const util::geo::FBox& pointBbox = pointBboxes[gid];
    const util::geo::FBox& fLineBbox = lineBboxes[gid];

    double GRID_SIZE = 65536;

    double pw =
        pointBbox.getUpperRight().getX() - pointBbox.getLowerLeft().getX();
    double ph =
        pointBbox.getUpperRight().getY() - pointBbox.getLowerLeft().getY();

    // estimate memory consumption of empty grid
    double pxWidth = fmax(0, ceil(pw / GRID_SIZE));
    double pyHeight = fmax(0, ceil(ph / GRID_SIZE));

    double lw =
        fLineBbox.getUpperRight().getX() - fLineBbox.getLowerLeft().getX();
    double lh =
        fLineBbox.getUpperRight().getY() - fLineBbox.getLowerLeft().getY();

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

    _pgrid[gsid] =
        petrimaps::Grid<ID_TYPE, float, float>(GRID_SIZE, GRID_SIZE, pointBbox);
    _lgrid[gsid] =
        petrimaps::Grid<ID_TYPE, float, float>(GRID_SIZE, GRID_SIZE, fLineBbox);
    _agrid[gsid] =
        petrimaps::Grid<ID_TYPE, float, float>(GRID_SIZE, GRID_SIZE, fLineBbox);
    _lpgrid[gsid] = petrimaps::Grid<util::geo::Point<uint8_t>, float, float>(
        GRID_SIZE, GRID_SIZE, fLineBbox);

    std::exception_ptr ePtr1, ePtr2, ePtr3, ePtr4;

#pragma omp parallel sections
    {
#pragma omp section
      {
        size_t j = _objects[gid].size() + _dynamicPoints[gid].size();

        for (size_t oid = 0; oid < _objects[gid].size(); oid++) {
          const auto& p = _objects[gid][oid];
          auto geomId = p.first;
          if (geomId >= I_OFFSET) continue;

          size_t clusterI = 0;
          // cluster if they have same geometry, don't do for multigeoms
          while (oid < _objects[gid].size() - 1 &&
                 geomId == _objects[gid][oid + 1].first) {
            clusterI++;
            oid++;
          }

          if (clusterI > 0) {
            for (size_t m = 0; m < clusterI; m++) {
              const auto& p = _objects[gid][oid - m];
              _pgrid[gsid].add(_cache->getPoints()[p.first],
                               getValFor(gid, vid, oid - m), j);
              if (buildClusters)
                _clusterObjects[gid].push_back({oid - m, {m, clusterI}});
              j++;
            }
          } else {
            _pgrid[gsid].add(_cache->getPoints()[geomId],
                             getValFor(gid, vid, oid), oid);
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

        for (size_t i = 0; i < _dynamicPoints[gid].size(); i++) {
          const auto& p = _dynamicPoints[gid][i];
          auto geom = p.first;

          size_t clusterI = 0;
          // cluster if they have same geometry, don't do for multigeoms
          while (i < _dynamicPoints[gid].size() - 1 &&
                 geom == _dynamicPoints[gid][i + 1].first) {
            clusterI++;
            i++;
          }

          if (clusterI > 0) {
            for (size_t m = 0; m < clusterI; m++) {
              const auto& p = _dynamicPoints[gid][i - m];
              auto geom = p.first;
              _pgrid[gsid].add(
                  geom, getValFor(gid, vid, i - m + _objects[gid].size()), j);
              if (buildClusters)
                _clusterObjects[gid].push_back(
                    {i - m + _objects[gid].size(), {m, clusterI}});
              j++;
            }
          } else {
            _pgrid[gsid].add(geom,
                             getValFor(gid, vid, i + _objects[gid].size()),
                             i + _objects[gid].size());
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
        for (const auto& l : _objects[gid]) {
          if (l.first >= I_OFFSET &&
              l.first < std::numeric_limits<ID_TYPE>::max()) {
            auto geomId = l.first - I_OFFSET;
            auto box = _cache->getLineBBox(geomId);
            util::geo::FBox fbox = {
                {box.getLowerLeft().getX(), box.getLowerLeft().getY()},
                {box.getUpperRight().getX(), box.getUpperRight().getY()}};
            _lgrid[gsid].add(fbox, getValFor(gid, vid, i), i);
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
        for (const auto& l : _objects[gid]) {
          if (l.first >= I_OFFSET &&
              l.first < std::numeric_limits<ID_TYPE>::max()) {
            auto geomId = l.first - I_OFFSET;
            bool lineIsArea = isArea(geomId);

            size_t start = _cache->getLine(geomId);
            size_t end = _cache->getLineEnd(geomId);

            double mainX = 0;
            double mainY = 0;

            size_t gi = 0;

            int lastX = 0;
            int lastY = 0;

            double val = getValFor(gid, vid, i);

            double area = 0;
            util::geo::FBox fbox;
            util::geo::FPoint lastP;

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

              if (lineIsArea && gi != 3) {
                area +=
                    (lastP.getX() + curP.getX()) * (lastP.getY() - curP.getY());
                fbox = extendBox(curP, fbox);
              }

              lastP = curP;

              size_t cellX = _lpgrid[gsid].getCellXFromX(curP.getX());
              size_t cellY = _lpgrid[gsid].getCellYFromY(curP.getY());

              uint8_t sX =
                  (curP.getX() - _lpgrid[gsid].getBBox().getLowerLeft().getX() +
                   cellX * _lpgrid[gsid].getCellWidth()) /
                  256;
              uint8_t sY =
                  (curP.getY() - _lpgrid[gsid].getBBox().getLowerLeft().getY() +
                   cellY * _lpgrid[gsid].getCellHeight()) /
                  256;

              const auto& cellBox = _lpgrid[gsid].getBox(cellX, cellY);

              int fullX = cellBox.getLowerLeft().getX() + sX * 256;
              int fullY = cellBox.getLowerLeft().getY() + sY * 256;

              if (gi == 3 || lastX != fullX || lastY != fullY) {
                _lpgrid[gsid].add(cellX, cellY, val, {sX, sY});
                lastX = fullX;
                lastY = fullY;
              }
            }

            if (lineIsArea && fabs(area) > (2000.0 * 2000.0)) {
              _agrid[gsid].add(fbox, val, i);
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
std::vector<std::string> Requestor::getColumns(const std::string& backend,
                                               std::string query) {
  std::regex expr("select[^{]*(\\*|[\\?$][A-Z0-9_\\-+]*)+[^{]*\\s*\\{",
                  std::regex_constants::icase);

  query = std::regex_replace(query, expr, "SELECT * WHERE {$&",
                             std::regex_constants::format_first_only) +
          "}";

  query += " LIMIT 0";

  RequestReader reader(backend, -1, 0, 0, 0);
  return reader.requestColumns(query);
}

// _____________________________________________________________________________
std::string Requestor::prepQuery(std::string query,
                                 std::vector<std::string> columns,
                                 std::string sortBy) const {
  std::regex expr("select[^{]*(\\*|[\\?$][A-Z0-9_\\-+]*)+[^{]*\\s*\\{",
                  std::regex_constants::icase);

  query =
      std::regex_replace(query, expr,
                         "SELECT " + util::implode(columns, " ") + " WHERE {$&",
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
  for (size_t lid = 0; lid < getNumLayers(); lid++) {
    auto r = getNearest(lid, rp, rad, res, fullbox, remoteAddr);
    if (r.has) return r;
  }

  return {false, 0, 0, {0, 0}, {}, {}, {}, {}};
}

// _____________________________________________________________________________
const ResObj Requestor::getNearest(size_t lid, util::geo::DPoint rp, double rad,
                                   double res, util::geo::FBox fullbox,
                                   const std::string& remoteAddr) const {
  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }

  const size_t gid = _lidToObject[lid];
  const size_t gsid = _lidToGrid[lid];

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
        _pgrid[gsid].get(fullbox, &ret);
      else
        _pgrid[gsid].get(fbox, &ret);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
      for (size_t idx = 0; idx < ret.size(); idx++) {
        auto oid = ret[idx];
        util::geo::FPoint p;
        if (isCluster(lid, oid)) {
          auto dp = clusterGeom(lid, oid, res);
          p = {dp.getX(), dp.getY()};
        } else {
          p = getPoint(lid, oid);
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
      _lgrid[gsid].get(fbox, &retL);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
      for (size_t idx = 0; idx < retL.size(); idx++) {
        const auto& oid = retL[idx];
        auto lBox = _cache->getLineBBox(_objects[gid][oid].first - I_OFFSET);
        if (!util::geo::intersects(lBox, box)) continue;

        size_t start = _cache->getLine(_objects[gid][oid].first - I_OFFSET);
        size_t end = _cache->getLineEnd(_objects[gid][oid].first - I_OFFSET);

        // TODO _____________________ own function
        double d = std::numeric_limits<double>::infinity();

        util::geo::DPoint curPa, curPb;
        int s = 0;

        size_t gi = 0;

        double mainX = 0;
        double mainY = 0;

        bool isArea = Requestor::isArea(_objects[gid][oid].first - I_OFFSET);

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
    size_t row = getRow(lid, nearest);
    auto points = geomPointGeoms(lid, nearest, res);

    return {true,
            nearest,
            lid,
            points.size() == 1 ? points[0] : util::geo::centroid(points),
            requestRow(row, remoteAddr),
            points,
            geomLineGeoms(lid, nearest, rad / 10),
            geomPolyGeoms(lid, nearest, rad / 10)};
  }

  if (dBestL < rad && dBestL <= dBest) {
    size_t lineId = _objects[gid][nearestL].first - I_OFFSET;
    const auto& dline = extractLineGeom(lineId);

    if (Requestor::isArea(lineId) &&
        util::geo::contains(rp, util::geo::DPolygon(dline))) {
      return {true,
              nearestL,
              lid,
              {frp.getX(), frp.getY()},
              requestRow(_objects[gid][nearestL].second, remoteAddr),
              geomPointGeoms(lid, nearestL, res),
              geomLineGeoms(lid, nearestL, rad / 10),
              geomPolyGeoms(lid, nearestL, rad / 10)};
    } else {
      auto p = util::geo::PolyLine<double>(dline).projectOn(rp).p;
      auto fp = util::geo::DPoint(p.getX(), p.getY());
      return {true,
              nearestL,
              lid,
              fp,
              requestRow(_objects[gid][nearestL].second, remoteAddr),
              geomPointGeoms(lid, nearestL, res),
              geomLineGeoms(lid, nearestL, rad / 10),
              geomPolyGeoms(lid, nearestL, rad / 10)};
    }
  }

  return {false, 0, 0, {0, 0}, {}, {}, {}, {}};
}

// _____________________________________________________________________________
const ResObj Requestor::getGeom(size_t lid, size_t id, double rad) const {
  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }

  return {true,
          id,
          lid,
          {0, 0},
          {},
          geomPointGeoms(lid, id, rad / 10),
          geomLineGeoms(lid, id, rad / 10),
          geomPolyGeoms(lid, id, rad / 10)};
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
util::geo::MultiLine<double> Requestor::geomLineGeoms(size_t lid, size_t oid,
                                                      double eps) const {
  const size_t gid = _lidToObject[lid];
  std::vector<util::geo::DLine> polys;

  // catch multigeometries
  for (size_t i = oid; i < _objects[gid].size() &&
                       _objects[gid][i].second == _objects[gid][oid].second;
       i++) {
    if (_objects[gid][i].first < I_OFFSET ||
        Requestor::isArea(_objects[gid][i].first - I_OFFSET))
      continue;
    const auto& fline = extractLineGeom(_objects[gid][i].first - I_OFFSET);
    polys.push_back(util::geo::simplify(fline, eps));
  }

  if (oid > 0) {
    for (size_t i = oid - 1;
         i < _objects[gid].size() &&
         _objects[gid][i].second == _objects[gid][oid].second;
         i--) {
      if (_objects[gid][i].first < I_OFFSET ||
          Requestor::isArea(_objects[gid][i].first - I_OFFSET))
        continue;
      const auto& fline = extractLineGeom(_objects[gid][i].first - I_OFFSET);
      polys.push_back(util::geo::simplify(fline, eps));
    }
  }

  return polys;
}

// _____________________________________________________________________________
util::geo::MultiPoint<double> Requestor::geomPointGeoms(size_t lid,
                                                        size_t oid) const {
  return geomPointGeoms(lid, oid, -1);
}

// _____________________________________________________________________________
util::geo::MultiPoint<double> Requestor::geomPointGeoms(size_t lid, size_t oid,
                                                        double res) const {
  const size_t gid = _lidToObject[lid];
  std::vector<util::geo::DPoint> points;

  if (!(res < 0) && isCluster(lid, oid)) {
    return {clusterGeom(lid, oid, res)};
  }

  if (isCluster(lid, oid)) {
    oid = getCluster(lid, oid).first;
  }

  if (oid >= _objects[gid].size()) {
    points.push_back(
        {_dynamicPoints[gid][oid - _objects[gid].size()].first.getX(),
         _dynamicPoints[gid][oid - _objects[gid].size()].first.getY()});
    return points;
  }

  // catch multigeometries, not relevant for dynamic points
  for (size_t i = oid; i < _objects[gid].size() &&
                       _objects[gid][i].second == _objects[gid][oid].second;
       i++) {
    if (_objects[gid][i].first >= I_OFFSET) continue;
    auto p = _cache->getPoints()[_objects[gid][i].first];
    points.push_back({p.getX(), p.getY()});
  }

  if (oid > 0) {
    for (size_t i = oid - 1;
         i < _objects[gid].size() &&
         _objects[gid][i].second == _objects[gid][oid].second;
         i--) {
      if (_objects[gid][i].first >= I_OFFSET) continue;
      auto p = _cache->getPoints()[_objects[gid][i].first];
      points.push_back({p.getX(), p.getY()});
    }
  }

  return points;
}

// _____________________________________________________________________________
util::geo::MultiPolygon<double> Requestor::geomPolyGeoms(size_t lid, size_t oid,
                                                         double eps) const {
  const size_t gid = _lidToObject[lid];
  std::vector<util::geo::DPolygon> polys;

  // catch multigeometries
  for (size_t i = oid; i < _objects[gid].size() &&
                       _objects[gid][i].second == _objects[gid][oid].second;
       i++) {
    if (_objects[gid][i].first < I_OFFSET ||
        !Requestor::isArea(_objects[gid][i].first - I_OFFSET))
      continue;
    const auto& dline = extractLineGeom(_objects[gid][i].first - I_OFFSET);
    polys.push_back(util::geo::DPolygon(util::geo::simplify(dline, eps)));
  }

  if (oid > 0) {
    for (size_t i = oid - 1;
         i < _objects[gid].size() &&
         _objects[gid][i].second == _objects[gid][oid].second;
         i--) {
      if (_objects[gid][i].first < I_OFFSET ||
          !Requestor::isArea(_objects[gid][i].first - I_OFFSET))
        continue;
      const auto& dline = extractLineGeom(_objects[gid][i].first - I_OFFSET);
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
util::geo::DPoint Requestor::clusterGeom(size_t lid, size_t oid,
                                         double res) const {
  const size_t gid = _lidToObject[lid];
  size_t cid = oid - getObjects(lid).size() - getDynamicPoints(lid).size();
  size_t refOid = _clusterObjects[gid][cid].first;

  util::geo::FPoint pp = getPoint(lid, refOid);

  if (res < 0) return {pp.getX(), pp.getY()};

  size_t num = _clusterObjects[gid][cid].second.first;
  size_t tot = _clusterObjects[gid][cid].second.second;

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
std::pair<double, double> Requestor::getValRange(size_t lid) const {
  const size_t vid = _lidToValue[lid];
  if (vid == NO_COL) return {0, 0};
  if (_valsMin[vid] >= _valsMax[vid]) return {0, 0};
  return {_valsMin[vid], _valsMax[vid]};
}

// _____________________________________________________________________________
std::pair<double, double> Requestor::getRasterMetas(
    size_t lid, size_t oid, std::pair<double, double> def) const {
  const size_t gid = _lidToObject[lid];
  const size_t rid = _lidToRaster[lid];

  if (rid == NO_COL) return def;

  const auto& rasterMetas = _rasterMetas[rid];

  if (oid < _objects[gid].size()) {
    if (_objects[gid][oid].second >= rasterMetas.size()) return def;
    return _cache->getRasterMeta(rasterMetas[_objects[gid][oid].second]);
  }

  // dynamic points
  const size_t did = oid - _objects[gid].size();
  if (did >= _dynamicPoints[gid].size()) return def;
  if (_dynamicPoints[gid][did].second >= rasterMetas.size()) return def;
  return _cache->getRasterMeta(rasterMetas[_dynamicPoints[gid][did].second]);
}

// _____________________________________________________________________________
double Requestor::getVal(size_t lid, size_t oid) const {
  return getValFor(_lidToObject[lid], _lidToValue[lid], oid);
}

// _____________________________________________________________________________
double Requestor::getValFor(size_t gid, size_t vid, size_t oid) const {
  // shortcut
  if (vid == NO_COL) return 1;

  const auto& vals = _vals[vid];

  // shortcut
  if (vals.size() == 0) return 1;

  if (oid < _objects[gid].size()) {
    if (_objects[gid][oid].second >= vals.size()) return 1;
    return vals[_objects[gid][oid].second];
  }

  // dynamic points
  const size_t did = oid - _objects[gid].size();
  if (did >= _dynamicPoints[gid].size()) return 1;
  if (_dynamicPoints[gid][did].second >= vals.size()) return 1;
  return vals[_dynamicPoints[gid][did].second];
}
