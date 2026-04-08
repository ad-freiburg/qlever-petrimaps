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

struct FieldConfig {
  std::string geomField;
  std::string valueField = "";
  double rasterW = 0;
  double rasterH = 0;
  std::string color = "3388ff";
};

struct RequestorConfig {
  std::string query;
  std::vector<FieldConfig> fields;

  std::string getHash() const {
    std::hash<std::string> hashF;
    std::string fieldsStr;
    for (const auto& field : fields)
      fieldsStr += field.geomField + "|" + field.valueField + "|" +
                   std::to_string(field.rasterW) + "|" +
                   std::to_string(field.rasterH) + "|" + field.color;
    return std::to_string(hashF(query + fieldsStr));
  }
};

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
  Requestor(std::shared_ptr<const GeomCache> cache, RequestorConfig rcfg,
            size_t maxMemory)
      : _cache(cache),
        _rcfg(rcfg),
        _maxMemory(maxMemory),
        _createdAt(std::chrono::system_clock::now()) {
    auto columns = getColumns(_rcfg.query);
    for (size_t i = 0; i < columns.size(); i++) _columnsMap[columns[i]] = i;

    if (_rcfg.fields.size() == 0) {
      _geomColumns = {columns.back()};
      _rcfg.fields.push_back({columns.back()});
    } else {
      for (const auto& field : _rcfg.fields) {
        if (!_columnsMap.count(field.geomField)) continue;
        _geomColumns.push_back(field.geomField);
        if (_columnsMap.count(field.valueField)) {
          _valueFlds[_geomColumns.size() - 1] = _columnsMap[field.valueField];
          _valueColumns.push_back(field.valueField);
        }
      }
    }

    for (size_t i = 0; i < _geomColumns.size(); i++) {
      _geoColToLid[_geomColumns[i]] = i;
    }
  }

  void request();

  std::vector<std::pair<std::string, std::string>> requestRow(
      uint64_t row) const;

  void requestRows(
      std::function<
          void(std::vector<std::vector<std::pair<std::string, std::string>>>)>
          cb) const;

  const petrimaps::Grid<ID_TYPE, float>& getPointGrid(size_t layerId) const {
    return _pgrid[layerId];
  }

  const petrimaps::Grid<ID_TYPE, float>& getLineGrid(size_t layerId) const {
    return _lgrid[layerId];
  }

  const petrimaps::Grid<util::geo::Point<uint8_t>, float>& getLinePointGrid(
      size_t layerId) const {
    return _lpgrid[layerId];
  }

  const std::vector<std::pair<ID_TYPE, ID_TYPE>>& getObjects(
      size_t layerId) const {
    return _objects[layerId];
  }

  const std::vector<std::pair<util::geo::FPoint, ID_TYPE>>& getDynamicPoints(
      size_t layerId) const {
    return _dynamicPoints[layerId];
  }

  const std::vector<std::pair<ID_TYPE, std::pair<size_t, size_t>>>& getClusters(
      size_t layerId) const {
    return _clusterObjects[layerId];
  }

  const util::geo::FPoint& getPoint(ID_TYPE id) const {
    return _cache->getPoints()[id];
  }

  const util::geo::FPoint& getDPoint(size_t layerId, ID_TYPE id) const {
    return _dynamicPoints[layerId][id].first;
  }

  size_t getLine(ID_TYPE id) const { return _cache->getLine(id); }

  size_t getLineEnd(ID_TYPE id) const { return _cache->getLineEnd(id); }

  const std::vector<util::geo::Point<int16_t>>& getLinePoints() const {
    return _cache->getLinePoints();
  }

  util::geo::DBox getLineBBox(ID_TYPE id) const {
    return _cache->getLineBBox(id);
  }

  const ResObj getNearest(size_t lid, util::geo::DPoint p, double rad,
                          double res, util::geo::FBox box) const;

  const ResObj getGeom(size_t lid, size_t id, double rad) const;

  util::geo::MultiPolygon<double> geomPolyGeoms(size_t lid, size_t oid,
                                                double eps) const;
  util::geo::MultiLine<double> geomLineGeoms(size_t lid, size_t oid,
                                             double eps) const;
  util::geo::MultiPoint<double> geomPointGeoms(size_t lid, size_t oid,
                                               double res) const;
  util::geo::MultiPoint<double> geomPointGeoms(size_t lid, size_t oid) const;

  util::geo::DLine extractLineGeom(size_t lineId) const;
  bool isArea(size_t lineId) const;

  size_t getNumObjects() const { return _numObjects; }
  util::geo::DPoint clusterGeom(size_t layerId, size_t cid, double res) const;

  std::vector<std::string> getColumns(std::string query) const;

  double getVal(size_t lid, size_t oid) const;

  size_t getLayerId(const std::string& layer) {
    return _geoColToLid.find(layer)->second;
  }
  size_t getNumLayers() const { return _pgrid.size(); }

  const std::vector<FieldConfig> getFields() const { return _rcfg.fields; }
  std::pair<double, double> getValRange() const;

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
  RequestorConfig _rcfg;

  size_t _maxMemory;

  std::string prepQuery(std::string query, std::vector<std::string> columns,
                        std::string sortBy) const;
  std::string prepQueryRow(std::string query, uint64_t row) const;

  std::vector<std::pair<util::geo::FPoint, ID_TYPE>> getDynamicPoints(
      const std::vector<IdMapping>& ids) const;

  std::string _query, _sortColumn;

  mutable std::mutex _m;

  std::vector<std::vector<std::pair<ID_TYPE, ID_TYPE>>> _objects;
  std::vector<std::vector<std::pair<util::geo::FPoint, ID_TYPE>>>
      _dynamicPoints;
  std::vector<std::vector<std::pair<ID_TYPE, std::pair<size_t, size_t>>>>
      _clusterObjects;
  std::vector<double> _vals;
  double _valMax, _valMin;
  size_t _numObjects = 0;

  std::vector<std::string> _geomColumns;
  std::vector<std::string> _valueColumns;
  std::map<std::string, size_t> _columnsMap;
  std::map<std::string, size_t> _geoColToLid;
  std::map<size_t, size_t> _valueFlds;

  std::vector<petrimaps::Grid<ID_TYPE, float>> _pgrid;
  std::vector<petrimaps::Grid<ID_TYPE, float>> _lgrid;
  std::vector<petrimaps::Grid<util::geo::Point<uint8_t>, float>> _lpgrid;

  bool _ready = false;

  std::chrono::time_point<std::chrono::system_clock> _createdAt;
};
}  // namespace petrimaps

#endif  // MAPUI_SERVER_REQUESTOR_H_
