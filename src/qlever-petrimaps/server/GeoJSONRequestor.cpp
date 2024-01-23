// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include "GeoJSONRequestor.h"

using petrimaps::GeoJSONRequestor;

void GeoJSONRequestor::request() {
  std::lock_guard<std::mutex> guard(_m);

  if (_ready) {
    // nothing to do
    return;
  }
  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }

  _ready = false;
  _objects.clear();
  _clusterObjects.clear();

  _objects = _cache->getRelObjects();
  _numObjects = _objects.size();

  LOG(INFO) << "[REQUESTOR] ... done, got " << _objects.size() << " objects.";

  LOG(INFO) << "[REQUESTOR] Calculating bounding box of result...";

  util::geo::FBox pointBbox;
  util::geo::DBox lineBbox;
  createBboxes(pointBbox, lineBbox);

  LOG(INFO) << "[REQUESTOR] ... done";
  LOG(INFO) << "[REQUESTOR] Point BBox: " << util::geo::getWKT(pointBbox);
  LOG(INFO) << "[REQUESTOR] Line BBox: " << util::geo::getWKT(lineBbox);
  LOG(INFO) << "[REQUESTOR] Building grid...";

  createGrid(pointBbox, lineBbox);

  _ready = true;

  LOG(INFO) << "[REQUESTOR] ...done";
}

// _____________________________________________________________________________
std::vector<std::pair<std::string, std::string>> GeoJSONRequestor::requestRow(uint64_t row) const {
  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }

  std::map<std::string, std::string> attrRow = _cache->getAttrRow(row);
  std::vector<std::pair<std::string, std::string>> res;
  for (auto const& entry : attrRow) {
    std::string key = entry.first;
    std::string val = entry.second;
    std::pair<std::string, std::string> pair{key, val};
    res.push_back(pair);
  }

  return res;
}