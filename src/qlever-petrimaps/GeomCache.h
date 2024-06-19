// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_GEOMCACHE_H_
#define PETRIMAPS_GEOMCACHE_H_

#include <curl/curl.h>

#include <atomic>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "qlever-petrimaps/Misc.h"
#include "util/geo/Geo.h"
#include "util/log/Log.h"

using util::geo::DPoint;
using util::geo::FPoint;
using util::geo::latLngToWebMerc;

namespace petrimaps {
class GeomCache {
 public:
  enum SourceType { backend, geoJSON };

  virtual ~GeomCache() {
    if (_curl) curl_easy_cleanup(_curl);
  }

  bool ready() const {
    _m.lock();
    bool ready = _ready;
    _m.unlock();
    return ready;
  }

  const std::vector<util::geo::FPoint>& getPoints() const { return _points; }
  const std::vector<util::geo::Point<int16_t>>& getLinePoints() const {
    return _linePoints;
  }
  const std::vector<size_t>& getLines() const { return _lines; }
  size_t getLine(ID_TYPE id) const { return _lines[id]; }
  size_t getLineEnd(ID_TYPE id) const {
    return id + 1 < _lines.size() ? _lines[id + 1] : _linePoints.size();
  }

  util::geo::FBox getPointBBox(size_t id) const {
    return util::geo::getBoundingBox(_points[id]);
  }
  util::geo::DBox getLineBBox(size_t id) const;

  double getLoadStatusPercent(bool total);
  double getLoadStatusPercent() { return getLoadStatusPercent(false); };
  int getLoadStatusStage();
  size_t getTotalProgress();
  size_t getCurrentProgress();

 protected:
  CURL* _curl;
  enum _LoadStatusStages { Parse = 1, ParseIds };
  _LoadStatusStages _loadStatusStage = Parse;

  size_t _curRow = 0, _curUniqueGeom = 0, _geometryDuplicates = 0;
  size_t _totalSize = 0;
  mutable std::mutex _m;
  bool _ready = false;

  std::vector<util::geo::FPoint> _points;
  std::vector<util::geo::Point<int16_t>> _linePoints;
  std::vector<size_t> _lines;

  static bool pointValid(const util::geo::FPoint& p);
  static bool pointValid(const util::geo::DPoint& p);
};
}  // namespace petrimaps

#endif  // PETRIMAPS_GEOMCACHE_H_
