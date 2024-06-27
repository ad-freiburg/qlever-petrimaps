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
  std::vector<util::geo::FPoint> pos;
  std::vector<std::pair<std::string, std::string>> cols;

  // the geometry
  std::vector<util::geo::DLine> line;
  std::vector<util::geo::DPolygon> poly;
};

class Requestor {
 public:
  virtual ~Requestor(){};
  virtual void request(){};
  virtual void request(const std::string&){};
  virtual std::vector<std::pair<std::string, std::string>> requestRow(
      uint64_t) const = 0;
  virtual void requestRows(
      std::function<
          void(std::vector<std::vector<std::pair<std::string, std::string>>>)>)
      const = 0;

  void createBboxes(util::geo::FBox& pointBbox, util::geo::DBox& lineBbox);
  void createGrid(util::geo::FBox pointBbox, util::geo::DBox lineBbox);

  const petrimaps::Grid<ID_TYPE, float>& getPointGrid() const { return _pgrid; }
  const petrimaps::Grid<ID_TYPE, float>& getLineGrid() const { return _lgrid; }

  const petrimaps::Grid<util::geo::Point<uint8_t>, float>& getLinePointGrid()
      const {
    return _lpgrid;
  }

  const std::vector<std::pair<ID_TYPE, ID_TYPE>>& getObjects() const {
    return _objects;
  }

  const std::vector<std::pair<ID_TYPE, std::pair<size_t, size_t>>>&
  getClusters() const {
    return _clusterObjects;
  }

  const util::geo::FPoint& getPoint(ID_TYPE id) const {
    return _cache->getPoint(id);
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
  util::geo::MultiPoint<float> geomPointGeoms(size_t oid, double res) const;
  util::geo::MultiPoint<float> geomPointGeoms(size_t oid) const;

  util::geo::DLine extractLineGeom(size_t lineId) const;
  bool isArea(size_t lineId) const;

  size_t getNumObjects() const { return _numObjects; }
  util::geo::FPoint clusterGeom(size_t cid, double res) const;

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
  petrimaps::Grid<ID_TYPE, float> _pgrid;
  petrimaps::Grid<ID_TYPE, float> _lgrid;
  petrimaps::Grid<util::geo::Point<uint8_t>, float> _lpgrid;

 protected:
  std::shared_ptr<const GeomCache> _cache;
  mutable std::mutex _m;
  size_t _maxMemory;
  std::chrono::time_point<std::chrono::system_clock> _createdAt;
  bool _ready = false;

  std::vector<std::pair<ID_TYPE, ID_TYPE>> _objects;
  std::vector<std::pair<ID_TYPE, std::pair<size_t, size_t>>> _clusterObjects;
  size_t _numObjects = 0;
};
}  // namespace petrimaps

#endif  // MAPUI_SERVER_REQUESTOR_H_
