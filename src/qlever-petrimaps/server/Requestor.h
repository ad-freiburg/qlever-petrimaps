// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_SERVER_REQUESTOR_H_
#define PETRIMAPS_SERVER_REQUESTOR_H_

#include <map>
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
  util::geo::FPoint pos;
  std::vector<std::pair<std::string, std::string>> cols;
};

class Requestor {
 public:
  Requestor() : _maxMemory(-1) {}
  Requestor(const GeomCache* cache, size_t maxMemory)
      : _cache(cache), _maxMemory(maxMemory) {}

  std::vector<std::pair<std::string, std::string>> requestRow(
      uint64_t row) const;

  void request(const std::string& query);

  const petrimaps::Grid<ID_TYPE, float>& getPointGrid()
      const {
    return _pgrid;
  }

  const petrimaps::Grid<ID_TYPE, float>& getLineGrid() const {
    return _lgrid;
  }

  const petrimaps::Grid<util::geo::Point<uint16_t>, float>&
  getLinePointGrid() const {
    return _lpgrid;
  }

  const std::vector<std::pair<ID_TYPE, ID_TYPE>>& getObjects() const {
    return _objects;
  }

  const util::geo::FPoint& getPoint(ID_TYPE id) const {
    return _cache->getPoints()[id];
  }

  size_t getLine(ID_TYPE id) const { return _cache->getLine(id); }

  size_t getLineEnd(ID_TYPE id) const { return _cache->getLineEnd(id); }

  const std::vector<util::geo::Point<int16_t>>& getLinePoints() const {
    return _cache->getLinePoints();
  }

  util::geo::FBox getLineBBox(ID_TYPE id) const {
    return _cache->getLineBBox(id);
  }

  const ResObj getNearest(util::geo::FPoint p, double rad) const;

  std::mutex& getMutex() const { return _m; }

  bool ready() const { return _ready; }

 private:
  std::string _backendUrl;

  const GeomCache* _cache;

  size_t _maxMemory;

  std::string prepQuery(std::string query) const;
  std::string prepQueryRow(std::string query, uint64_t row) const;

  std::string _query;

  mutable std::mutex _m;

  std::vector<std::pair<ID_TYPE, ID_TYPE>> _objects;

  petrimaps::Grid<ID_TYPE, float> _pgrid;
  petrimaps::Grid<ID_TYPE, float> _lgrid;
  petrimaps::Grid<util::geo::Point<uint16_t>, float> _lpgrid;

  bool _ready = false;
};
}  // namespace petrimaps

#endif  // MAPUI_SERVER_REQUESTOR_H_
