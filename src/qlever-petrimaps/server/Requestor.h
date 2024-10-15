// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_SERVER_REQUESTOR_H_
#define PETRIMAPS_SERVER_REQUESTOR_H_

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "qlever-petrimaps/GeomCache.h"
#include "qlever-petrimaps/Grid.h"
#include "qlever-petrimaps/Misc.h"
#include "util/geo/Geo.h"

namespace petrimaps {

struct ResObj {
  bool has;
  size_t id;
  util::geo::DPoint pos;
  std::vector<std::pair<std::string, std::string>> cols;

  // the geometry
  util::geo::MultiPoint<double> point;
  util::geo::MultiLine<double> line;
  util::geo::MultiPolygon<double> poly;
};

struct ReaderCbPair {
  RequestReader* reader;
  std::function<void(
      std::vector<std::vector<std::pair<std::string, std::string>>>)>
      cb;
};

class Requestor {
 public:
  Requestor() : _maxMemory(-1) {}
  Requestor(std::shared_ptr<const GeomCache> cache, size_t maxMemory)
      : _cache(cache),
        _maxMemory(maxMemory),
        _createdAt(std::chrono::system_clock::now()) {}

  void request(const std::string& query);

  std::vector<std::pair<std::string, std::string>> requestRow(
      uint64_t row) const;

  void requestRows(
      std::function<
          void(std::vector<std::vector<std::pair<std::string, std::string>>>)>
          cb) const;

  const petrimaps::Grid<ID_TYPE, float>& getPointGrid() const { return _pgrid; }

  const petrimaps::Grid<ID_TYPE, float>& getLineGrid() const { return _lgrid; }

  const petrimaps::Grid<util::geo::Point<uint8_t>, float>& getLinePointGrid()
      const {
    return _lpgrid;
  }

  const std::vector<std::pair<ID_TYPE, ID_TYPE>>& getObjects() const {
    return _objects;
  }

  const std::vector<std::pair<util::geo::FPoint, ID_TYPE>>& getDynamicPoints() const {
    return _dynamicPoints;
  }

  const std::vector<std::pair<ID_TYPE, std::pair<size_t, size_t>>>&
  getClusters() const {
    return _clusterObjects;
  }

  const util::geo::FPoint& getPoint(ID_TYPE id) const {
    return _cache->getPoints()[id];
  }

  const util::geo::FPoint& getDPoint(ID_TYPE id) const {
    return _dynamicPoints[id].first;
  }

  size_t getLine(ID_TYPE id) const { return _cache->getLine(id); }

  size_t getLineEnd(ID_TYPE id) const { return _cache->getLineEnd(id); }

  const std::vector<util::geo::Point<int16_t>>& getLinePoints() const {
    return _cache->getLinePoints();
  }

  util::geo::DBox getLineBBox(ID_TYPE id) const {
    return _cache->getLineBBox(id);
  }

  const ResObj getNearest(util::geo::DPoint p, double rad, double res,
                          util::geo::FBox box) const;

  const ResObj getGeom(size_t id, double rad) const;

  util::geo::MultiPolygon<double> geomPolyGeoms(size_t oid, double eps) const;
  util::geo::MultiLine<double> geomLineGeoms(size_t oid, double eps) const;
  util::geo::MultiPoint<double> geomPointGeoms(size_t oid, double res) const;
  util::geo::MultiPoint<double> geomPointGeoms(size_t oid) const;

  util::geo::DLine extractLineGeom(size_t lineId) const;
  bool isArea(size_t lineId) const;

  size_t getNumObjects() const { return _numObjects; }
  util::geo::DPoint clusterGeom(size_t cid, double res) const;

  std::chrono::time_point<std::chrono::system_clock> createdAt() const {
    return _createdAt;
  }

  bool ready() const {
    _m.lock();
    bool ready = _ready;
    _m.unlock();
    return ready;
  }

 private:
  std::string _backendUrl;

  std::shared_ptr<const GeomCache> _cache;

  size_t _maxMemory;

  std::string prepQuery(std::string query) const;
  std::string prepQueryRow(std::string query, uint64_t row) const;

  std::vector<std::pair<util::geo::FPoint, ID_TYPE>> getDynamicPoints(
      const std::vector<IdMapping>& ids) const;

  std::string _query;

  mutable std::mutex _m;

  std::vector<std::pair<ID_TYPE, ID_TYPE>> _objects;
  std::vector<std::pair<util::geo::FPoint, ID_TYPE>> _dynamicPoints;
  std::vector<std::pair<ID_TYPE, std::pair<size_t, size_t>>> _clusterObjects;
  size_t _numObjects = 0;

  petrimaps::Grid<ID_TYPE, float> _pgrid;
  petrimaps::Grid<ID_TYPE, float> _lgrid;
  petrimaps::Grid<util::geo::Point<uint8_t>, float> _lpgrid;

  bool _ready = false;

  std::chrono::time_point<std::chrono::system_clock> _createdAt;
};
}  // namespace petrimaps

#endif  // MAPUI_SERVER_REQUESTOR_H_
