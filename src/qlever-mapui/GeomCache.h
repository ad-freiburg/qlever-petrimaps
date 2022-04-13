// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef MAPUI_GEOMCACHE_H_
#define MAPUI_GEOMCACHE_H_

#include <curl/curl.h>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "qlever-mapui/Misc.h"
#include "util/geo/Geo.h"

namespace mapui {

class GeomCache {
 public:
  GeomCache() : _backendUrl(""), _curl(0) {}
  explicit GeomCache(const std::string& backendUrl)
      : _backendUrl(backendUrl), _curl(curl_easy_init()) {}

  GeomCache& operator=(GeomCache&& o) {
    _backendUrl = o._backendUrl;
    _curl = curl_easy_init();
    _lines = std::move(o._lines);
    _points = std::move(o._points);
    _dangling = o._dangling;
    _state = o._state;
    return *this;
  };

  ~GeomCache() {
    if (_curl) curl_easy_cleanup(_curl);
  }

  void request();

  void requestIds();

  void parse(const char*, size_t size);
  void parseIds(const char*, size_t size);

  std::vector<std::pair<size_t, size_t>> getRelObjects(
      const std::vector<std::pair<uint64_t, uint64_t>>& id) const;

  const std::string& getBackendURL() const { return _backendUrl; }

  const std::vector<util::geo::FPoint>& getPoints() const { return _points; }

  const std::vector<util::geo::FLine>& getLines() const { return _lines; }

  util::geo::FBox getPointBBox(size_t id) const {
    return util::geo::getBoundingBox(_points[id]);
  }
  const util::geo::FBox& getLineBBox(size_t id) const { return _lineBoxes[id]; }

 private:
  std::string _backendUrl;
  CURL* _curl;

  uint8_t _curByte;
  ID _curId;
  size_t _curRow;

  static size_t writeCb(void* contents, size_t size, size_t nmemb, void* userp);
  static size_t writeCbIds(void* contents, size_t size, size_t nmemb,
                           void* userp);

  std::string queryUrl(std::string query) const;

  util::geo::FLine parseLineString(const std::string& a, size_t p) const;
  util::geo::FPoint parsePoint(const std::string& a, size_t p) const;

  std::vector<util::geo::FPoint> _points;
  std::vector<util::geo::FLine> _lines;
  std::vector<util::geo::FPolygon> _polygons;

  std::vector<util::geo::FBox> _lineBoxes;

  std::vector<std::pair<size_t, size_t>> _qleverIdToInternalId;

  std::string _dangling;
  ParseState _state;
};
}  // namespace mapui

#endif  // MAPUI_GEOMCACHE_H_
