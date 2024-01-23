// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <curl/curl.h>
#include <stdlib.h>

#include <atomic>
#include <cstring>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <sstream>

#include "qlever-petrimaps/Misc.h"
#include "qlever-petrimaps/server/Requestor.h"
#include "util/Misc.h"
#include "util/geo/Geo.h"
#include "util/geo/PolyLine.h"
#include "GeomCache.h"

using petrimaps::GeomCache;

// _____________________________________________________________________________
double GeomCache::getLoadStatusPercent(bool total) {
  /*
  There are 2 loading stages: Parse, afterwards ParseIds.
  Because ParseIds is usually pretty short, we merge the progress of both stages
  to one total progress. Progress is calculated by _curRow / _totalSize, which
  are handled by each stage individually.
  */
  if (_totalSize == 0) {
    return 0.0;
  }
  if (!total) {
    return std::atomic<size_t>(_curRow) / static_cast<double>(_totalSize) *
           100.0;
  }

  double parsePercent = 95.0;
  double parseIdsPercent = 5.0;
  double totalPercent = 0.0;
  switch (_loadStatusStage) {
    case _LoadStatusStages::Parse:
      totalPercent = std::atomic<size_t>(_curRow) /
                     static_cast<double>(_totalSize) * parsePercent;
      break;
    case _LoadStatusStages::ParseIds:
      totalPercent = parsePercent;
      totalPercent += std::atomic<size_t>(_curRow) /
                      static_cast<double>(_totalSize) * parseIdsPercent;
      break;
  }

  return totalPercent;
}

// _____________________________________________________________________________
int GeomCache::getLoadStatusStage() {
  return _loadStatusStage;
}

// _____________________________________________________________________________
bool GeomCache::pointValid(const FPoint& p) {
  if (p.getY() > std::numeric_limits<float>::max()) return false;
  if (p.getY() < std::numeric_limits<float>::lowest()) return false;
  if (p.getX() > std::numeric_limits<float>::max()) return false;
  if (p.getX() < std::numeric_limits<float>::lowest()) return false;

  return true;
}

// _____________________________________________________________________________
bool GeomCache::pointValid(const DPoint& p) {
  if (p.getY() > std::numeric_limits<double>::max()) return false;
  if (p.getY() < std::numeric_limits<double>::lowest()) return false;
  if (p.getX() > std::numeric_limits<double>::max()) return false;
  if (p.getX() < std::numeric_limits<double>::lowest()) return false;

  return true;
}

// _____________________________________________________________________________
util::geo::DBox GeomCache::getLineBBox(size_t lid) const {
  util::geo::DBox ret;
  LOG(INFO) << "[GEOMCACHE] lid: " << lid;
  size_t start = getLine(lid);

  bool s = false;

  double mainX = 0;
  double mainY = 0;
  for (size_t i = start; i < start + 4; i++) {
    // extract real geom
    const auto& cur = _linePoints[i];

    LOG(INFO) << "[GEOMCACHE] _linePoints.size(): " << _linePoints.size();
    if (isMCoord(cur.getX())) {
      mainX = rmCoord(cur.getX());
      mainY = rmCoord(cur.getY());
      continue;
    }

    util::geo::DPoint curP((mainX * M_COORD_GRANULARITY + cur.getX()) / 10.0,
                           (mainY * M_COORD_GRANULARITY + cur.getY()) / 10.0);

    if (!s) {
      ret.setLowerLeft(curP);
      s = true;
    } else {
      ret.setUpperRight(curP);
      return ret;
    }
  }

  return ret;
}
