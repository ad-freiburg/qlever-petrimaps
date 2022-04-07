// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef MAPUI_SERVER_REQUESTOR_H_
#define MAPUI_SERVER_REQUESTOR_H_

#include <curl/curl.h>
#include <map>
#include <string>
#include <vector>
#include <mutex>
#include "util/geo/Geo.h"
#include "util/geo/Grid.h"

enum ParseState {
  AW_RES,
  AW_RES_DANG,
  IN_RES_TENT,
  IN_RES,
  IN_RES_ROW,
  IN_RES_ROW_VALUE,
  DONE
};

struct ResObj {
  bool has;
  util::geo::FPoint pos;
  std::vector<std::pair<std::string, std::string>> cols;
};

namespace mapui {

typedef std::map<std::string, std::string> Params;

class Requestor {
 public:
  Requestor() : _backendUrl(""), _curl(0) {}
  explicit Requestor(const std::string& backendUrl)
      : _backendUrl(backendUrl), _curl(curl_easy_init()) {}

  Requestor& operator=(Requestor&& o) {
    _backendUrl = o._backendUrl;
    _curl = curl_easy_init();
    _bbox = o._bbox;
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

  void request(const std::string& query) const;

  void parse(const char*, size_t size) const;

  size_t size() const { return _points.size(); }

  const util::geo::Grid<size_t, util::geo::Point, float>& getPointGrid() const {
    return _pgrid;
  }

  const util::geo::Grid<size_t, util::geo::Line, float>& getLineGrid() const {
    return _lgrid;
  }

  const std::vector<util::geo::FPoint>& getPoints() const {
    return _points;
  }

  const std::vector<util::geo::FLine>& getLines() const {
    return _lines;
  }

  const ResObj getNearest(util::geo::FPoint p, double rad) const;

  std::mutex& getMutex() const { return _m; }

 private:
  std::string _backendUrl;
  CURL* _curl;

  std::string queryUrl(std::string query) const;

  mutable std::mutex _m;

  mutable std::vector<util::geo::FPoint> _points;
  mutable std::vector<util::geo::FLine> _lines;

  mutable util::geo::Grid<size_t, util::geo::Point, float> _pgrid;
  mutable util::geo::Grid<size_t, util::geo::Line, float> _lgrid;
  mutable util::geo::FBox _bbox;

	mutable size_t _received;

  mutable std::string _dangling;
  mutable ParseState _state;

  mutable std::vector<std::vector<std::pair<std::string, std::string>>> _cols;
  mutable std::vector<size_t> _pcols;
  mutable std::vector<size_t> _lcols;
};
}  // namespace mapui

#endif  // MAPUI_SERVER_REQUESTOR_H_
