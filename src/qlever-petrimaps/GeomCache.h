// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_GEOMCACHE_H_
#define PETRIMAPS_GEOMCACHE_H_

#include <curl/curl.h>

#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "qlever-petrimaps/Misc.h"
#include "util/geo/Geo.h"

namespace petrimaps {

class GeomCache {
 public:
  GeomCache() : _backendUrl(""), _curl(0) {}
  explicit GeomCache(const std::string& backendUrl)
      : _backendUrl(backendUrl), _curl(curl_easy_init()) {}

  GeomCache& operator=(GeomCache&& o) {
    _backendUrl = o._backendUrl;
    _curl = curl_easy_init();
    _lines = std::move(o._lines);
    _linePoints = std::move(o._linePoints);
    _points = std::move(o._points);
    _dangling = o._dangling;
    _state = o._state;
    return *this;
  };

  ~GeomCache() {
    if (_curl) curl_easy_cleanup(_curl);
  }

  void request();
  void requestPart(size_t offset);

  void requestIds();

  void parse(const char*, size_t size);
  void parseIds(const char*, size_t size);

  std::vector<std::pair<ID_TYPE, ID_TYPE>> getRelObjects(
      const std::vector<IdMapping>& id) const;

  const std::string& getBackendURL() const { return _backendUrl; }

  const std::vector<util::geo::FPoint>& getPoints() const { return _points; }

  const std::vector<util::geo::Point<int16_t>>& getLinePoints() const { return _linePoints; }

  const std::vector<size_t>& getLines() const { return _lines; }

  util::geo::FBox getPointBBox(size_t id) const {
    return util::geo::getBoundingBox(_points[id]);
  }
  util::geo::FBox getLineBBox(size_t id) const;

  size_t getLine(ID_TYPE id) const {
    return _lines[id];
  }

  size_t getLineEnd(ID_TYPE id) const {
    return id + 1 < _lines.size() ? _lines[id + 1] : _linePoints.size();
  }
private:
  std::string _backendUrl;
  CURL* _curl;

  uint8_t _curByte;
  ID _curId;
  QLEVER_ID_TYPE _maxQid;
  size_t _curRow, _curUniqueGeom;

  static size_t writeCb(void* contents, size_t size, size_t nmemb, void* userp);
  static size_t writeCbIds(void* contents, size_t size, size_t nmemb,
                           void* userp);

  std::string queryUrl(std::string query, size_t offset, size_t limit) const;

  util::geo::FLine parseLineString(const std::string& a, size_t p) const;
  util::geo::FPoint parsePoint(const std::string& a, size_t p) const;

  static bool pointValid(const util::geo::FPoint& p);

  void insertLine(const util::geo::FLine& l);

  std::vector<util::geo::FPoint> _points;
  std::vector<util::geo::Point<int16_t>> _linePoints;
  std::vector<size_t> _lines;

  std::vector<IdMapping> _qidToId;

  std::string _dangling, _prev;
  ParseState _state;
};
}  // namespace petrimaps

#endif  // PETRIMAPS_GEOMCACHE_H_
