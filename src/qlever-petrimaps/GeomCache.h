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
#include <atomic>

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

  const util::geo::FPoint& getPoint(ID_TYPE id) const {
    return std::get<0>(_points[id]);
  }
  const std::vector<util::geo::Point<int16_t>>& getLinePoints() const {
    return _linePoints;
  }
  size_t getLine(ID_TYPE id) const {
    return std::get<0>(_lines[id]);
  }
  size_t getLineEnd(ID_TYPE id) const {
    return id + 1 < _lines.size() ? getLine(id + 1) : _linePoints.size();
  }
  
  util::geo::DBox getLineBBox(size_t id) const;

  double getLoadStatusPercent(bool total);
  double getLoadStatusPercent() {
    return getLoadStatusPercent(false);
  };
  int getLoadStatusStage();
  size_t getTotalProgress();
  size_t getCurrentProgress();

 protected:
  CURL* _curl;
  enum _LoadStatusStages { Parse = 1, ParseIds, FromFile};
  _LoadStatusStages _loadStatusStage = Parse;

  std::atomic<size_t> _curRow;
  size_t _curUniqueGeom = 0, _geometryDuplicates = 0;
  size_t _totalSize = 0;
  mutable std::mutex _m;
  bool _ready = false;

  std::vector<std::tuple<util::geo::FPoint, bool>> _points;
  std::vector<std::tuple<size_t, bool>> _lines;
  std::vector<util::geo::Point<int16_t>> _linePoints;

  static bool pointValid(const util::geo::FPoint& p);
  static bool pointValid(const util::geo::DPoint& p);
};
}  // namespace petrimaps

#endif  // PETRIMAPS_GEOMCACHE_H_
