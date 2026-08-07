// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_SERVER_REQUESTOR_H_
#define PETRIMAPS_SERVER_REQUESTOR_H_

#include <chrono>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "qlever-petrimaps/GeomCache.h"
#include "qlever-petrimaps/Grid.h"
#include "qlever-petrimaps/Misc.h"
#include "qlever-petrimaps/server/RenderContext.h"
#include "util/geo/Geo.h"
#include "util/log/Log.h"

namespace petrimaps {

static const size_t NO_COL = std::numeric_limits<size_t>::max();

struct LayerConfig {
  std::string geomField = "";
  std::string id = "";
  std::string name = "";
  std::string group = "";
  std::string valueField = "";
  std::string rasterMetaField = "";
  std::string toggle = "";
  double rasterW = 10;
  double rasterH = 10;
  std::string color = "3388ff";
  std::string colorscheme = "spectralexp";
  std::string style = "auto";
  ObjectStyle objectStyle;

  const std::string geomFieldRaw() const {
    return util::split(geomField, ':')[0];
  }
};

struct RequestorConfig {
  std::string query;
  std::vector<LayerConfig> layers;

  std::string getHash() const {
    std::hash<std::string> hashF;
    std::string layersStr;
    for (const auto& layer : layers)
      layersStr += layer.geomField + "|" + layer.valueField + "|" +
                   layer.group + "|" + std::to_string(layer.rasterW) + "|" +
                   std::to_string(layer.rasterH) + "|" + layer.color + "|" +
                   layer.id + "|" + layer.name + "|" + layer.style + "|" +
                   layer.colorscheme + "|" + layer.toggle;
    return std::to_string(hashF(query + layersStr));
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
    auto columns = getColumns(_cache->getConfig().backend, _rcfg.query);
    for (size_t i = 0; i < columns.size(); i++) _columnsMap[columns[i]] = i;

    // try to make geom cols, value cols and raster cols unique
    std::map<std::string, size_t> geomColToGid, valueColToVid, rasterColToRid;

    // grids can only be unique per combination of (gid, vid)
    std::map<std::pair<size_t, size_t>, size_t> gridSetIdx;

    // logic here: there a user-layers (in _rcfg.layers), but these may
    // share geom columns, layer columns and raster columns. These are made
    // unique, and an additional mapping layer lid -> gid, lid -> vid and lid
    // ->rid is added
    for (const auto& layer : _rcfg.layers) {
      // if the field was not found at all in the column map, simply ignore it
      if (!_columnsMap.count(layer.geomField)) continue;

      size_t gid = uniqueCol(layer.geomField, &_geomColumns, &geomColToGid);
      size_t vid =
          _columnsMap.count(layer.valueField)
              ? uniqueCol(layer.valueField, &_valueColumns, &valueColToVid)
              : NO_COL;
      size_t rid = _columnsMap.count(layer.rasterMetaField)
                       ? uniqueCol(layer.rasterMetaField, &_rasterMetaColumns,
                                   &rasterColToRid)
                       : NO_COL;

      std::pair<size_t, size_t> gridKey{gid, vid};
      auto gridIt = gridSetIdx.find(gridKey);
      if (gridIt == gridSetIdx.end()) {
        _gridSets.push_back(gridKey);
        gridIt = gridSetIdx.insert({gridKey, _gridSets.size() - 1}).first;
      }

      _layers.push_back(layer);
      _layerIdToLid[layer.id] = _layers.size() - 1;
      _geomFieldToLid[layer.geomField].push_back(_layers.size() - 1);

      _lidToObject.push_back(gid);
      _lidToValue.push_back(vid);
      _lidToRaster.push_back(rid);
      _lidToGrid.push_back(gridIt->second);
    }

    LOG(util::LogLevel::INFO)
        << "[REQUESTOR] " << _layers.size() << " layers, "
        << _geomColumns.size() << " unique geom columns, "
        << _valueColumns.size() << " unique value columns, "
        << _rasterMetaColumns.size() << " raster columns, " << _gridSets.size()
        << " unique grid sets";
  }

  void request(const std::string& remoteAddr);

  std::vector<std::pair<std::string, std::string>> requestRow(
      uint64_t row, const std::string& remoteAddr) const;

  void requestRows(
      std::function<
          void(std::vector<std::vector<std::pair<std::string, std::string>>>)>
          cb,
      const std::string& remoteAddr) const;

  const petrimaps::Grid<ID_TYPE, float, float>& getPointGrid(size_t lid) const {
    return _pgrid[_lidToGrid[lid]];
  }

  const petrimaps::Grid<ID_TYPE, float, float>& getLineGrid(size_t lid) const {
    return _lgrid[_lidToGrid[lid]];
  }

  const petrimaps::Grid<util::geo::Point<uint8_t>, float, float>&
  getLinePointGrid(size_t lid) const {
    return _lpgrid[_lidToGrid[lid]];
  }

  const petrimaps::Grid<ID_TYPE, float, float>& getAreaGrid(size_t lid) const {
    return _agrid[_lidToGrid[lid]];
  }

  const std::vector<std::pair<ID_TYPE, ID_TYPE>>& getObjects(size_t lid) const {
    return _objects[_lidToObject[lid]];
  }

  const std::vector<std::pair<util::geo::FPoint, ID_TYPE>>& getDynamicPoints(
      size_t lid) const {
    return _dynamicPoints[_lidToObject[lid]];
  }

  const std::vector<std::pair<ID_TYPE, std::pair<size_t, size_t>>>& getClusters(
      size_t lid) const {
    return _clusterObjects[_lidToObject[lid]];
  }

  const std::pair<ID_TYPE, std::pair<size_t, size_t>>& getCluster(
      size_t lid, size_t oid) const {
    const size_t gid = _lidToObject[lid];
    size_t cid = oid - _objects[gid].size() - _dynamicPoints[gid].size();
    return _clusterObjects[gid][cid];
  }

  const util::geo::FPoint& getCPoint(size_t lid, ID_TYPE oid) const {
    return _cache->getPoints()[_objects[_lidToObject[lid]][oid].first];
  }

  const util::geo::FPoint& getDPoint(size_t lid, ID_TYPE oid) const {
    const size_t gid = _lidToObject[lid];
    return _dynamicPoints[gid][oid - _objects[gid].size()].first;
  }

  const util::geo::FPoint& getPoint(size_t lid, ID_TYPE oid) const {
    if (oid < _objects[_lidToObject[lid]].size()) return getCPoint(lid, oid);
    return getDPoint(lid, oid);
  }

  size_t getRow(size_t lid, ID_TYPE oid) const {
    const size_t gid = _lidToObject[lid];
    if (isCluster(lid, oid)) oid = getCluster(lid, oid).first;
    if (oid >= _objects[gid].size())
      return _dynamicPoints[gid][oid - _objects[gid].size()].second;
    return _objects[gid][oid].second;
  }

  bool isCluster(size_t lid, ID_TYPE id) const {
    return id >= getObjects(lid).size() + getDynamicPoints(lid).size();
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

  // total number of objects over all distinct geometry columns
  size_t getNumObjects() const {
    size_t ret = 0;
    for (size_t gid = 0; gid < _numObjects.size(); gid++)
      ret += _numObjects[gid];

    return ret;
  }
  size_t getNumObjects(size_t lid) const {
    return _numObjects[_lidToObject[lid]];
  }
  util::geo::DPoint clusterGeom(size_t lid, size_t oid, double res) const;

  static std::vector<std::string> getColumns(const std::string& backend,
                                             std::string query);

  double getVal(size_t lid, size_t oid) const;
  std::pair<double, double> getRasterMetas(size_t lid, size_t oid) const;

  size_t getNumLayers() const { return _layers.size(); }
  bool lineIntersects(size_t lid, const util::geo::DBox& bbox) const;

  const std::vector<LayerConfig>& getLayers() const { return _layers; }

  size_t getLidById(const std::string& id) {
    auto it = _layerIdToLid.find(id);
    if (it == _layerIdToLid.end()) {
      std::stringstream ss;
      ss << "Layer '" << id << "' not found";
      throw std::runtime_error(ss.str());
    }
    return it->second;
  }

  size_t getLidByGeomField(const std::string& field) {
    auto it = _geomFieldToLid.find(field);
    if (it == _geomFieldToLid.end()) {
      std::stringstream ss;
      ss << "Geom field '" << field << "' not found";
      throw std::runtime_error(ss.str());
    }
    if (it->second.size() == 0) {
      std::stringstream ss;
      ss << "Geom field '" << field << "' not found";
      throw std::runtime_error(ss.str());
    }
    return it->second[0];
  }

  std::pair<double, double> getValRange(size_t lid) const;

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
  std::vector<LayerConfig> _layers;

  size_t _maxMemory;

  std::string prepQuery(std::string query, std::vector<std::string> columns,
                        std::string sortBy) const;
  std::string prepQueryRow(std::string query, uint64_t row) const;

  std::vector<std::pair<util::geo::FPoint, ID_TYPE>> getDynamicPoints(
      const std::vector<IdMapping>& ids) const;

  static size_t uniqueCol(const std::string& col,
                          std::vector<std::string>* cols,
                          std::map<std::string, size_t>* idx) {
    auto it = idx->find(col);
    if (it != idx->end()) return it->second;

    cols->push_back(col);
    (*idx)[col] = cols->size() - 1;
    return cols->size() - 1;
  }

  // value of object oid of geom column gid, taken from value column vid
  double getValFor(size_t gid, size_t vid, size_t oid) const;

  std::string _query, _sortColumn;

  mutable std::mutex _m;

  // per distinct geometry column
  std::vector<std::vector<std::pair<ID_TYPE, ID_TYPE>>> _objects;
  std::vector<std::vector<std::pair<util::geo::FPoint, ID_TYPE>>>
      _dynamicPoints;
  std::vector<std::vector<std::pair<ID_TYPE, std::pair<size_t, size_t>>>>
      _clusterObjects;
  std::vector<size_t> _numObjects;

  // per vid
  std::vector<std::vector<double>> _vals;
  std::vector<double> _valsMax;
  std::vector<double> _valsMin;

  // per rid
  std::vector<std::vector<size_t>> _rasterMetas;

  std::vector<std::string> _geomColumns;
  std::vector<std::string> _valueColumns;
  std::vector<std::string> _rasterMetaColumns;
  std::map<std::string, size_t> _columnsMap;
  std::map<std::string, size_t> _layerIdToLid;

  // per (gid, vid)
  std::vector<std::pair<size_t, size_t>> _gridSets;

  // mapping in direction  lid -> gid, vid, rad, grid
  std::vector<size_t> _lidToObject;
  std::vector<size_t> _lidToValue;
  std::vector<size_t> _lidToRaster;
  std::vector<size_t> _lidToGrid;

  // mapping geomfield -> lid
  std::map<std::string, std::vector<size_t>> _geomFieldToLid;

  std::vector<petrimaps::Grid<ID_TYPE, float, float>> _pgrid;
  std::vector<petrimaps::Grid<ID_TYPE, float, float>> _lgrid;
  std::vector<petrimaps::Grid<ID_TYPE, float, float>> _agrid;
  std::vector<petrimaps::Grid<util::geo::Point<uint8_t>, float, float>> _lpgrid;

  bool _ready = false;

  std::chrono::time_point<std::chrono::system_clock> _createdAt;
};
}  // namespace petrimaps

#endif  // MAPUI_SERVER_REQUESTOR_H_
