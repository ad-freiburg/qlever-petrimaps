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
  Requestor() {}
  Requestor(const GeomCache* cache) : _cache(cache) {}

  std::vector<std::pair<std::string, std::string>> requestRow(
      uint64_t row) const;

  void request(const std::string& query);

  size_t size() const { return _points.size(); }

  const petrimaps::Grid<size_t, util::geo::Point, float>& getPointGrid() const {
    return _pgrid;
  }

  const petrimaps::Grid<size_t, util::geo::Line, float>& getLineGrid() const {
    return _lgrid;
  }

  const petrimaps::Grid<util::geo::FPoint, util::geo::Point, float>&
  getLinePointGrid() const {
    return _lpgrid;
  }

  const std::vector<std::pair<uint64_t, uint64_t>>& getPoints() const {
    return _points;
  }

  const std::vector<std::pair<uint64_t, uint64_t>>& getLines() const {
    return _lines;
  }

  const std::vector<std::pair<uint64_t, uint64_t>>& getObjects() const {
    return _objects;
  }

  const util::geo::FPoint& getPoint(uint64_t id) const {
    return _cache->getPoints()[id];
  }

  const util::geo::FLine& getLine(uint64_t id) const {
    return _cache->getLines()[id];
  }

  const util::geo::FBox& getLineBBox(uint64_t id) const {
    return _cache->getLineBBox(id);
  }

  const ResObj getNearest(util::geo::FPoint p, double rad) const;

  std::mutex& getMutex() const { return _m; }

 private:
  std::string _backendUrl;

  const GeomCache* _cache;

  std::string prepQuery(std::string query) const;
  std::string prepQueryRow(std::string query, uint64_t row) const;

  std::string _query;

  mutable std::mutex _m;

  std::vector<std::pair<uint64_t, uint64_t>> _points;
  std::vector<std::pair<uint64_t, uint64_t>> _lines;
  std::vector<std::pair<uint64_t, uint64_t>> _objects;

  petrimaps::Grid<size_t, util::geo::Point, float> _pgrid;
  petrimaps::Grid<size_t, util::geo::Line, float> _lgrid;
  petrimaps::Grid<util::geo::FPoint, util::geo::Point, float> _lpgrid;
};
}  // namespace petrimaps

#endif  // MAPUI_SERVER_REQUESTOR_H_
