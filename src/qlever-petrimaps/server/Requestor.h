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
#include "util/log/Log.h"

namespace petrimaps {

struct FieldConfig {
  std::string geomField = "";
  std::string id = "";
  std::string name = "";
  std::string valueField = "";
  std::string rasterMetaField = "";
  std::string toggle = "";
  double rasterW = 10;
  double rasterH = 10;
  std::string color = "3388ff";
  std::string colorscheme = "spectralexp";
  std::string style = "auto";

  const std::string geomFieldRaw() const {
    return util::split(geomField, ':')[0];
  }
  static std::string layerFieldId(std::string field) {
    auto parts = util::split(field, ':');
    if (!parts.empty() && !parts[0].empty() &&
        (parts[0][0] == '?' || parts[0][0] == '$')) {
          parts[0].erase(0, 1);
        }
    return util::implode(parts, ":");
  }
  const std::string geomFieldLayerId() const{
    return layerFieldId(geomField);
  }
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
                   std::to_string(field.rasterH) + "|" + field.color + "|" +
                   field.id + "|" + field.name + "|" + field.style + "|" +
                   field.colorscheme + "|" + field.toggle;
    return std::to_string(hashF(query + fieldsStr));
  }
};

struct ResObj {
  bool has;
  size_t id;
  size_t fieldId;
  util::geo::DPoint pos;
  std::vector<std::pair<std::string, std::string>> cols;

  // the geometry
  util::geo::MultiPoint<double> point;
  util::geo::MultiLine<double> line;
  util::geo::MultiPolygon<double> poly;
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
        if (!_columnsMap.count(field.geomFieldRaw())) continue;
        _geomColumns.push_back(field.geomField);
        if (_columnsMap.count(field.valueField)) {
          _valueFlds[_geomColumns.size() - 1] = _valueColumns.size();
          _valueColumns.push_back(field.valueField);
        }
        if (_columnsMap.count(field.rasterMetaField)) {
          _rasterMetaFlds[_geomColumns.size() - 1] = _rasterMetaColumns.size();
          _rasterMetaColumns.push_back(field.rasterMetaField);
        }
      }
    }

    LOG(util::LogLevel::INFO)
        << "[REQUESTOR] " << _geomColumns.size() << " geom columns";

    for (size_t i = 0; i < _geomColumns.size(); i++) {
      _geoColToLid[_geomColumns[i]] = i;
      _geoColToLid[FieldConfig::layerFieldId(_geomColumns[i])] = i;
    }
  }

  void request(const std::string& remoteAddr);

  std::vector<std::pair<std::string, std::string>> requestRow(
      uint64_t row, const std::string& remoteAddr) const;

  void requestRows(
      std::function<
          void(std::vector<std::vector<std::pair<std::string, std::string>>>)>
          cb,
      const std::string& remoteAddr) const;

  const petrimaps::Grid<ID_TYPE, float, float>& getPointGrid(
      size_t fieldId) const {
    return _pgrid[fieldId];
  }

  const petrimaps::Grid<ID_TYPE, float, float>& getLineGrid(
      size_t fieldId) const {
    return _lgrid[fieldId];
  }

  const petrimaps::Grid<util::geo::Point<uint8_t>, float, float>&
  getLinePointGrid(size_t fieldId) const {
    return _lpgrid[fieldId];
  }

  const petrimaps::Grid<ID_TYPE, float, float>& getAreaGrid(
      size_t fieldId) const {
    return _agrid[fieldId];
  }

  const std::vector<std::pair<ID_TYPE, ID_TYPE>>& getObjects(
      size_t fieldId) const {
    return _objects[fieldId];
  }

  const std::vector<std::pair<util::geo::FPoint, ID_TYPE>>& getDynamicPoints(
      size_t fieldId) const {
    return _dynamicPoints[fieldId];
  }

  const std::vector<std::pair<ID_TYPE, std::pair<size_t, size_t>>>& getClusters(
      size_t fieldId) const {
    return _clusterObjects[fieldId];
  }

  const std::pair<ID_TYPE, std::pair<size_t, size_t>>& getCluster(
      size_t fieldId, size_t oid) const {
    size_t cid =
        oid - _objects[fieldId].size() - _dynamicPoints[fieldId].size();
    return _clusterObjects[fieldId][cid];
  }

  const util::geo::FPoint& getCPoint(size_t fieldId, ID_TYPE oid) const {
    return _cache->getPoints()[_objects[fieldId][oid].first];
  }

  const util::geo::FPoint& getDPoint(size_t fieldId, ID_TYPE oid) const {
    return _dynamicPoints[fieldId][oid - _objects[fieldId].size()].first;
  }

  const util::geo::FPoint& getPoint(size_t fieldId, ID_TYPE oid) const {
    if (oid < _objects[fieldId].size()) return getCPoint(fieldId, oid);
    return getDPoint(fieldId, oid);
  }

  size_t getRow(size_t fieldId, ID_TYPE oid) const {
    if (isCluster(fieldId, oid)) oid = getCluster(fieldId, oid).first;
    if (oid >= _objects[fieldId].size())
      return _dynamicPoints[fieldId][oid - _objects[fieldId].size()].second;
    return _objects[fieldId][oid].second;
  }

  bool isCluster(size_t fieldId, ID_TYPE id) const {
    return id >= getObjects(fieldId).size() + getDynamicPoints(fieldId).size();
  }

  size_t getLine(ID_TYPE id) const { return _cache->getLine(id); }

  size_t getLineEnd(ID_TYPE id) const { return _cache->getLineEnd(id); }

  const std::vector<util::geo::Point<int16_t>,
                    util::no_init_allocator<util::geo::Point<int16_t>>>&
  getLinePoints() const {
    return _cache->getLinePoints();
  }

  util::geo::DBox getLineBBox(ID_TYPE id) const {
    return _cache->getLineBBox(id);
  }

  const ResObj getNearest(size_t lid, util::geo::DPoint p, double rad,
                          double res, util::geo::FBox box,
                          const std::string& remoteAddr) const;

  const ResObj getNearest(util::geo::DPoint p, double rad, double res,
                          util::geo::FBox box,
                          const std::string& remoteAddr) const;

  const ResObj getGeom(size_t lid, size_t id, double rad) const;

  util::geo::MultiPolygon<double> geomPolyGeoms(size_t lid, size_t oid,
                                                double eps) const;
  util::geo::MultiLine<double> geomLineGeoms(size_t lid, size_t oid,
                                             double eps) const;
  util::geo::MultiPoint<double> geomPointGeoms(size_t lid, size_t oid,
                                               double res) const;
  util::geo::MultiPoint<double> geomPointGeoms(size_t lid, size_t oid) const;

  util::geo::DLine extractLineGeom(size_t lineId, double minD = 0) const;
  bool isArea(size_t lineId) const;
  bool isInnerArea(size_t lineId) const;

  double getLineDistance(
      size_t lineId,
      const util::geo::DPoint& queryPoint) const;

  double getPolygonDistance(
      size_t polygonId,
      const util::geo::DPoint& queryPoint,
      double radius) const;

  size_t getNumObjects() const {
    size_t ret = 0;
    for (size_t lid = 0; lid < _numObjects.size(); lid++)
      ret += _numObjects[lid];

    return ret;
  }
  size_t getNumObjects(size_t lid) const { return _numObjects[lid]; }
  util::geo::DPoint clusterGeom(size_t fieldId, size_t oid, double res) const;

  std::vector<std::string> getColumns(std::string query) const;

  double getVal(size_t lid, size_t oid) const;
  std::pair<double, double> getRasterMetas(size_t lid, size_t oid,
                                           std::pair<double, double> def) const;

  size_t getFieldId(const std::string& field) {
    auto it = _geoColToLid.find(field);
    std::stringstream ss;
    ss << "Field '" << field << "' not found";
    if (it == _geoColToLid.end()) throw std::runtime_error(ss.str());
    return it->second;
  }
  size_t getNumFields() const { return _pgrid.size(); }
  bool lineIntersects(size_t lid, const util::geo::DBox& bbox) const;

  const std::vector<FieldConfig> getFields() const { return _rcfg.fields; }
  std::pair<double, double> getValRange(size_t fid) const;

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
  std::vector<std::vector<double>> _vals;
  std::vector<double> _valsMax;
  std::vector<double> _valsMin;
  std::vector<std::vector<size_t>> _rasterMetas;
  std::vector<size_t> _numObjects;

  std::vector<std::string> _geomColumns;
  std::vector<std::string> _valueColumns;
  std::vector<std::string> _rasterMetaColumns;
  std::map<std::string, size_t> _columnsMap;
  std::map<std::string, size_t> _geoColToLid;
  std::map<size_t, size_t> _valueFlds;
  std::map<size_t, size_t> _rasterMetaFlds;

  std::vector<petrimaps::Grid<ID_TYPE, float, float>> _pgrid;
  std::vector<petrimaps::Grid<ID_TYPE, float, float>> _lgrid;
  std::vector<petrimaps::Grid<ID_TYPE, float, float>> _agrid;
  std::vector<petrimaps::Grid<util::geo::Point<uint8_t>, float, float>> _lpgrid;

  bool _ready = false;

  std::chrono::time_point<std::chrono::system_clock> _createdAt;
};
}  // namespace petrimaps

#endif  // MAPUI_SERVER_REQUESTOR_H_
