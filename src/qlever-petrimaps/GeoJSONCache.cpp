// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <3rdparty/nlohmann/json.hpp>

#include "GeoJSONCache.h"

using petrimaps::GeoJSONCache;
using json = nlohmann::json;

// _____________________________________________________________________________
std::vector<std::pair<ID_TYPE, ID_TYPE>> GeoJSONCache::getRelObjects() const {
  // Used for GeoJSON, returns all objects as vector<pair<geomID, Row>>
  // geomID starts from 0 ascending, Row = geomID
  std::vector<std::pair<ID_TYPE, ID_TYPE>> objects;
  objects.reserve(_points.size() + _linePoints.size());

  for (size_t i = 0; i < _points.size(); i++) {
    objects.push_back({i, i});
  }
  for (size_t i = 0; i < _lines.size(); i++) {
    objects.push_back({i + I_OFFSET, i + I_OFFSET});
  }

  return objects;
}

// _____________________________________________________________________________
void GeoJSONCache::insertLine(const util::geo::DLine& l, bool isArea) {
  const auto& bbox = util::geo::getBoundingBox(l);
  int16_t mainX = (bbox.getLowerLeft().getX() * 10.0) / M_COORD_GRANULARITY;
  int16_t mainY = (bbox.getLowerLeft().getY() * 10.0) / M_COORD_GRANULARITY;

  if (mainX != 0 || mainY != 0) {
    util::geo::Point<int16_t> p{mCoord(mainX), mCoord(mainY)};
    _linePoints.push_back(p);
  }

  // add bounding box lower left
  int16_t minorXLoc =
      (bbox.getLowerLeft().getX() * 10.0) - mainX * M_COORD_GRANULARITY;
  int16_t minorYLoc =
      (bbox.getLowerLeft().getY() * 10.0) - mainY * M_COORD_GRANULARITY;
  util::geo::Point<int16_t> p{minorXLoc, minorYLoc};
  _linePoints.push_back(p);

  // add bounding box upper left
  int16_t mainXLoc = (bbox.getUpperRight().getX() * 10.0) / M_COORD_GRANULARITY;
  int16_t mainYLoc = (bbox.getUpperRight().getY() * 10.0) / M_COORD_GRANULARITY;
  minorXLoc =
      (bbox.getUpperRight().getX() * 10.0) - mainXLoc * M_COORD_GRANULARITY;
  minorYLoc =
      (bbox.getUpperRight().getY() * 10.0) - mainYLoc * M_COORD_GRANULARITY;
  if (mainXLoc != mainX || mainYLoc != mainY) {
    mainX = mainXLoc;
    mainY = mainYLoc;
    util::geo::Point<int16_t> p{mCoord(mainX), mCoord(mainY)};
    _linePoints.push_back(p);
  }
  p = util::geo::Point<int16_t>{minorXLoc, minorYLoc};
  _linePoints.push_back(p);

  // add line points
  for (const auto& p : l) {
    mainXLoc = (p.getX() * 10.0) / M_COORD_GRANULARITY;
    mainYLoc = (p.getY() * 10.0) / M_COORD_GRANULARITY;

    if (mainXLoc != mainX || mainYLoc != mainY) {
      mainX = mainXLoc;
      mainY = mainYLoc;
      util::geo::Point<int16_t> p{mCoord(mainX), mCoord(mainY)};
      _linePoints.push_back(p);
    }

    int16_t minorXLoc = (p.getX() * 10.0) - mainXLoc * M_COORD_GRANULARITY;
    int16_t minorYLoc = (p.getY() * 10.0) - mainYLoc * M_COORD_GRANULARITY;
    util::geo::Point<int16_t> pp{minorXLoc, minorYLoc};
    _linePoints.push_back(pp);
  }

  // if we have an area, we end in a major coord (which is not possible for
  // other types)
  if (isArea) {
    util::geo::Point<int16_t> p{mCoord(0), mCoord(0)};
    _linePoints.push_back(p);
  }
}

// _____________________________________________________________________________
void GeoJSONCache::load() {
  _loadStatusStage = _LoadStatusStages::Parse;

  json res = json::parse(_content);

  // Parse json
  if (res["type"] != "FeatureCollection") {
    LOG(INFO) << "GeoJSON content is not a FeatureCollection.";
    return;
  }

  // Parse features
  auto features = res["features"];
  _totalSize = features.size();
  _curRow = 0;
  _curUniqueGeom = 0;
  if (_totalSize == 0) {
    throw std::runtime_error("Number of rows was 0");
  }

  _points.clear();
  _lines.clear();
  _linePoints.clear();

  size_t numPoints = 0;
  size_t numLines = 0;
  for (json feature : features) {
    // Parse type
    if (feature["type"] != "Feature") {
      LOG(INFO) << "[GeomCache] Non-Feature detected. Skipping...";
      continue;
    }
    // Parse geometry
    if (!feature.contains("geometry")) {
      LOG(INFO) << "[GeomCache] Feature has no geometry. Skipping...";
      continue;
    }

    json geom = feature["geometry"];
    std::string type = geom["type"];
    auto coords = geom["coordinates"];
    auto properties = feature["properties"];

    LOG(INFO) << type;

    // PRIMITIVES
    // Point
    if (type == "Point") {
      auto point = latLngToWebMerc(FPoint(coords[0], coords[1]));
      if (!pointValid(point)) {
        LOG(INFO) << "[GeomCache] Invalid point found. Skipping...";
        continue;
      }
      _points.push_back(point);

      _curUniqueGeom++;
      _attr[numPoints] = properties;
      numPoints++;
    
    // LineString
    } else if (type == "LineString") {
      util::geo::DLine line;
      line.reserve(2);

      for (std::vector<float> coord : coords) {
        auto point = latLngToWebMerc(DPoint(coord[0], coord[1]));
        if (!pointValid(point)) {
          LOG(INFO) << "[GeomCache] Invalid point found. Skipping...";
          continue;
        }
        line.push_back(point);
      }
      std::size_t idx = _linePoints.size();
      _lines.push_back(idx);
      line = util::geo::densify(line, 200 * 3);
      insertLine(line, false);

      _curUniqueGeom++;
      _attr[numLines + I_OFFSET] = properties;
      numLines++;
    
    // Polygon
    } else if (type == "Polygon") {
      for (auto args : coords) {
        util::geo::DLine line;
        line.reserve(2);

        for (std::vector<float> coord : args) {
          auto point = latLngToWebMerc(DPoint(coord[0], coord[1]));
          if (!pointValid(point)) {
            LOG(INFO) << "[GeomCache] Invalid point found. Skipping...";
            continue;
          }
          line.push_back(point);
        }
        std::size_t idx = _linePoints.size();
        _lines.push_back(idx);
        line = util::geo::densify(line, 200 * 3);
        insertLine(line, true);

        _curUniqueGeom++;
        _attr[numLines + I_OFFSET] = properties;
        numLines++;
      }

    // MULTIPART
    // MultiPoint
    } else if (type == "MultiPoint") {
      for (std::vector<float> coord : coords) {
        auto point = latLngToWebMerc(FPoint(coord[0], coord[1]));
        if (!pointValid(point)) {
          LOG(INFO) << "[GeomCache] Invalid point found. Skipping...";
          continue;
        }
        _points.push_back(point);

        _curUniqueGeom++;
        _attr[numPoints] = properties;
        numPoints++;
      }

    // MultiLineString
    } else if (type == "MultiLineString") {
      for (auto args : coords) {
        util::geo::DLine line;
        line.reserve(2);

        for (std::vector<float> coord : args) {
          auto point = latLngToWebMerc(DPoint(coord[0], coord[1]));
          if (!pointValid(point)) {
            LOG(INFO) << "[GeomCache] Invalid point found. Skipping...";
            continue;
          }
          line.push_back(point);
        }
        std::size_t idx = _linePoints.size();
        _lines.push_back(idx);
        line = util::geo::densify(line, 200 * 3);
        insertLine(line, false);

        _curUniqueGeom++;
        _attr[numLines + I_OFFSET] = properties;
        numLines++;
      }
    
    // MultiPolygon
    } else if (type == "MultiPolygon") {
      for (auto args1 : coords) {
        for (auto args2 : args1) {
          util::geo::DLine line;
          line.reserve(2);

          for (std::vector<float> coord : args2) {
            auto point = latLngToWebMerc(DPoint(coord[0], coord[1]));
            if (!pointValid(point)) {
              LOG(INFO) << "[GeomCache] Invalid point found. Skipping...";
              continue;
            }
            line.push_back(point);
          }
          std::size_t idx = _linePoints.size();
          _lines.push_back(idx);
          line = util::geo::densify(line, 200 * 3);
          insertLine(line, true);

          _curUniqueGeom++;
          _attr[numLines + I_OFFSET] = properties;
          numLines++;
        }
      }
    }

    _curRow++;
  }

  _ready = true;

  LOG(INFO) << "[GEOMCACHE] Done";
  LOG(INFO) << "[GEOMCACHE] Received " << _curUniqueGeom << " unique geoms";
  LOG(INFO) << "[GEOMCACHE] Received " << _points.size() << " points and "
            << _lines.size() << " lines";
}

void GeoJSONCache::setContent(const std::string& content) {
  _content = content;
}
