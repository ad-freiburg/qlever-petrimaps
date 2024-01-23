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

  void load(const std::string& content);
  std::vector<std::pair<ID_TYPE, ID_TYPE>> getRelObjects() const;
  std::map<std::string, std::string> getAttrRow(size_t row) const {
    return _attr.at(row);
  }
 
 private:
  void insertLine(const util::geo::DLine& l, bool isArea);
  // Map geomID to map<key, value>
  std::map<size_t, std::map<std::string, std::string>> _attr;
  // ------------------------

 protected:
    
};
} // namespace petrimaps

#endif  // PETRIMAPS_GEOJSONCACHE_H_