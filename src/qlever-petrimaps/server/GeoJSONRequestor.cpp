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
  _rowIdToObjectId.clear();

  _objects = _cache->getRelObjects();
  _numObjects = _objects.size();

  LOG(INFO) << "[REQUESTOR] ... done, got " << _objects.size() << " objects.";

  // Create mapping row_id to object_id for multigeometries
  for (size_t oid = 0; oid < _objects.size(); oid++) {
    std::pair<ID_TYPE, ID_TYPE> object = _objects[oid];
    ID_TYPE object_row_id = object.second;
    _rowIdToObjectId[object_row_id] = oid;
  }

  LOG(INFO) << "[REQUESTOR] Matching size: " << _rowIdToObjectId.size();

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

void GeoJSONRequestor::requestRows(std::function<void(std::vector<std::vector<std::pair<std::string, std::string>>>)> cb) const {
  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }

  std::vector<std::vector<std::pair<std::string, std::string>>> res;
  auto relObjects = _cache->getRelObjects();
  for(auto const& object : relObjects) {
    // vector<pair<geomID, Row>>
    // geomID starts from 0 ascending, Row = geomID
    ID_TYPE row = object.second;
    auto rowAttr = requestRow(row);
    res.push_back(rowAttr);
  }
  cb(res);
}