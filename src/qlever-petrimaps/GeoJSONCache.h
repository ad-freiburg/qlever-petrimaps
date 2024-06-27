// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_GEOJSONCACHE_H_
#define PETRIMAPS_GEOJSONCACHE_H_

#include "qlever-petrimaps/GeomCache.h"

namespace petrimaps {
class GeoJSONCache : public GeomCache {
 public:
  GeoJSONCache() {
     _curl = curl_easy_init();
  };

  GeoJSONCache& operator=(GeoJSONCache&& o) {
    _curl = curl_easy_init();
    _lines = std::move(o._lines);
    _linePoints = std::move(o._linePoints);
    _points = std::move(o._points);
    return *this;
  };

  const util::geo::FPoint& getPoint(ID_TYPE id) const {
    return std::get<0>(_points[id]);
  }
  size_t getLine(ID_TYPE id) const {
    return std::get<0>(_lines[id]);
  }
  size_t getLineEnd(ID_TYPE id) const {
    return id + 1 < _lines.size() ? getLine(id + 1) : _linePoints.size();
  }

  void load();
  void setContent(const std::string& content);
  std::vector<std::pair<ID_TYPE, ID_TYPE>> getRelObjects() const;
  std::map<std::string, std::string> getAttrRow(size_t row) const {
    return _attr.at(row);
  }
 
 private:
  std::string _content;

  void insertLine(const util::geo::DLine& l, bool isArea);
  // Map geomID to map<key, value>
  std::map<size_t, std::map<std::string, std::string>> _attr;
  // ------------------------
  std::vector<std::tuple<util::geo::FPoint, bool>> _points;
  std::vector<std::tuple<size_t, bool>> _lines;

 protected:
    
};
} // namespace petrimaps

#endif  // PETRIMAPS_GEOJSONCACHE_H_