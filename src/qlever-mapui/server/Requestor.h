// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef MAPUI_SERVER_REQUESTOR_H_
#define MAPUI_SERVER_REQUESTOR_H_

#include <curl/curl.h>
#include <map>
#include <mutex>
#include <string>
#include <vector>
#include "qlever-mapui/GeomCache.h"
#include "qlever-mapui/Misc.h"
#include "util/geo/Geo.h"
#include "qlever-mapui/Grid.h"

namespace mapui {

struct ResObj {
  bool has;
  util::geo::FPoint pos;
  std::vector<std::pair<std::string, std::string>> cols;
};

class Requestor {
 public:
  Requestor() : _curl(0) {}
  Requestor(const GeomCache* cache) : _cache(cache), _curl(curl_easy_init()) {}

  Requestor& operator=(Requestor&& o) {
    _backendUrl = o._backendUrl;
    _curl = curl_easy_init();
    _lgrid = std::move(o._lgrid);
    _lines = std::move(o._lines);
    _pgrid = std::move(o._pgrid);
    _points = std::move(o._points);
    _dangling = o._dangling;
    _state = o._state;
    return *this;
  };

  ~Requestor() {
    if (_curl) curl_easy_cleanup(_curl);
  }

  std::vector<std::pair<std::string, std::string>> requestRow(uint64_t row) const;

  void request(const std::string& query) const;

  void parse(const char*, size_t size) const;
  void parseIds(const char*, size_t size) const;

  size_t size() const { return _points.size(); }

  const mapui::Grid<size_t, util::geo::Point, float>& getPointGrid() const {
    return _pgrid;
  }

  const mapui::Grid<size_t, util::geo::Line, float>& getLineGrid() const {
    return _lgrid;
  }


  const std::vector<std::pair<uint64_t, uint64_t>>& getPoints() const { return _points; }

  const std::vector<std::pair<uint64_t, uint64_t>>& getLines() const { return _lines; }

  const util::geo::FPoint& getPoint(uint64_t id) const { return _cache->getPoints()[id]; }

  const util::geo::FLine& getLine(uint64_t id) const { return _cache->getLines()[id]; }

  const util::geo::FBox& getLineBBox(uint64_t id) const { return _cache->getLineBBox(id); }

  const ResObj getNearest(util::geo::FPoint p, double rad) const;

  std::mutex& getMutex() const { return _m; }

 private:
  std::string _backendUrl;
  CURL* _curl;

  const GeomCache* _cache;

  static size_t writeCb(void* contents, size_t size, size_t nmemb,
                         void* userp);
  static size_t writeCbIds(void* contents, size_t size, size_t nmemb,
                         void* userp);

  mutable std::vector<std::string> _colNames;
  mutable size_t _curCol;

  mutable uint8_t _curByte;
  mutable ID _curId;

  std::string queryUrl(std::string query) const;
  std::string queryUrlRow(std::string query, uint64_t row) const;

  mutable std::string _queryUrl;
  mutable std::string _query;

  mutable std::mutex _m;

  mutable size_t _curRow;

  mutable std::vector<std::pair<uint64_t, uint64_t>> _curIds;

  mutable std::vector<std::pair<uint64_t, uint64_t>> _points;
  mutable std::vector<std::pair<uint64_t, uint64_t>> _lines;

  mutable mapui::Grid<size_t, util::geo::Point, float> _pgrid;
  mutable mapui::Grid<size_t, util::geo::Line, float> _lgrid;

  mutable size_t _received;

  mutable std::string _dangling;
  mutable ParseState _state;

  mutable std::vector<std::pair<std::string, std::string>> _cols;
};
}  // namespace mapui

#endif  // MAPUI_SERVER_REQUESTOR_H_
