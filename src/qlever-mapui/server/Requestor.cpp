// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <curl/curl.h>
#include <cstring>
#include <iostream>
#include <sstream>
#include "qlever-mapui/server/Requestor.h"
#include "util/Misc.h"
#include "util/geo/Geo.h"
#include "util/geo/PolyLine.h"
#include "util/log/Log.h"

using mapui::Requestor;

// _____________________________________________________________________________
inline size_t writeCb(void* contents, size_t size, size_t nmemb, void* userp) {
  size_t realsize = size * nmemb;
  static_cast<Requestor*>(userp)->parse(static_cast<const char*>(contents),
                                        realsize);
  return realsize;
}

// _____________________________________________________________________________
void Requestor::parse(const char* c, size_t size) const {
  if (_state == DONE) return;

  const char* start = c;
  const char* tmp;
  while ((c - start) < size) {
    switch (_state) {
      case AW_RES:
        c = (const char*)(memchr(c, '"', (size - (c - start))));
        if (!c) return;

        if ((c - start) + 5 > size) {
          _state = AW_RES_DANG;
          _dangling = std::string((const char*)c, size - (c - start));
          return;
        }

        if (*(c + 1) == 'r' && *(c + 2) == 'e' && *(c + 3) == 's' &&
            *(c + 4) == '"' && *(c + 5) == ':') {
          _state = IN_RES_TENT;
          c += 6;
          continue;
        }

        c++;
        break;

      case IN_RES_TENT:
        c = (const char*)(memchr(c, '[', (size - (c - start))));
        if (!c) return;
        _state = IN_RES;
        c++;
        // fall through
      case IN_RES:
        if (*c == '[') {
          _cols.push_back({});
          _state = IN_RES_ROW;
          _received++;
          c++;
        } else if (*c == ']') {
          _state = DONE;
          return;
        } else {
          c++;
          continue;
        }
        // fall through
      case IN_RES_ROW:
        if (*c == '"') {
          _state = IN_RES_ROW_VALUE;
          _dangling = "";
          c++;
        } else if (*c == ']') {
          _state = IN_RES;
          c++;
          continue;
        } else {
          c++;
          continue;
        }
        // fall through
      case IN_RES_ROW_VALUE:
        if (*c == '"' && (_dangling.empty() || _dangling.back() != '\\')) {
          _state = IN_RES_ROW;

          auto p = _dangling.rfind("POINT(", 1);
          if (p != std::string::npos) {
            auto point = util::geo::latLngToWebMerc(util::geo::FPoint{
                util::atof(_dangling.c_str() + p + 6, 10),
                util::atof(
                    static_cast<const char*>(memchr(_dangling.c_str() + p + 6,
                                                    ' ', _dangling.size() - p) +
                                             1),
                    10)});

            if (point.getY() > std::numeric_limits<float>::max()) {
              continue;
            }

            if (point.getY() < std::numeric_limits<float>::lowest()) {
              continue;
            }

            if (point.getX() > std::numeric_limits<float>::max()) {
              continue;
            }

            if (point.getX() < std::numeric_limits<float>::lowest()) {
              continue;
            }

            _points.push_back(point);
            _pcols.push_back(_cols.size() - 1);

            _bbox = util::geo::extendBox(_points.back(), _bbox);
            // std::cout << util::geo::getWKT(_points.back()) << std::endl;
          } else if ((p = _dangling.rfind("LINESTRING(", 1)) !=
                     std::string::npos) {
            p += 11;
            util::geo::FLine line;
            while (true) {
              auto point = util::geo::latLngToWebMerc(util::geo::FPoint{
                  util::atof(_dangling.c_str() + p, 10),
                  util::atof(static_cast<const char*>(
                                 memchr(_dangling.c_str() + p, ' ',
                                        _dangling.size() - p) +
                                 1),
                             10)});

              if (point.getY() > std::numeric_limits<float>::max()) {
                continue;
              }

              if (point.getY() < std::numeric_limits<float>::lowest()) {
                continue;
              }

              if (point.getX() > std::numeric_limits<float>::max()) {
                continue;
              }

              if (point.getX() < std::numeric_limits<float>::lowest()) {
                continue;
              }

              line.push_back(point);

              // std::cout << util::geo::getWKT(_points.back()) << std::endl;
              _bbox = util::geo::extendBox(line.back(), _bbox);
              auto n = memchr(_dangling.c_str() + p, ',', _dangling.size() - p);
              if (!n) break;
              p = static_cast<const char*>(n) - _dangling.c_str() + 1;
            }
            _lines.push_back(line);
            _lcols.push_back(_cols.size() - 1);
          } else {
            _cols.back().push_back({"", _dangling});
          }

          _dangling = "";
          c++;
          continue;
        }
        if (*c == '"' && _dangling.back() == '\\')
          _dangling.back() = *c;
        else
          _dangling += *c;

        c++;

        break;

      default:
        break;
    }
  }
}

// _____________________________________________________________________________
void Requestor::request(const std::string& query) const {
  _m.lock();
  _state = AW_RES;
  _received = 0;
  CURLcode res;

  if (_curl) {
    auto qUrl = queryUrl(query);
    LOG(INFO) << "Query URL is " << qUrl;
    curl_easy_setopt(_curl, CURLOPT_URL, qUrl.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, writeCb);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, this);

    // accept any compression supported
    curl_easy_setopt(_curl, CURLOPT_ACCEPT_ENCODING, "");
    res = curl_easy_perform(_curl);
  }

  LOG(INFO) << "Done, received " << _received << " rows in total.";
  LOG(INFO) << "BBox: " << util::geo::getWKT(_bbox);
  LOG(INFO) << "Starting building grid...";

  _pgrid = util::geo::Grid<size_t, util::geo::Point, float>(50000, 50000, _bbox,
                                                            false);
  _lgrid = util::geo::Grid<size_t, util::geo::Line, float>(50000, 50000, _bbox,
                                                           false);

  LOG(INFO) << "...done init ...";

  size_t i = 0;
  for (const auto& p : _points) {
    _pgrid.add(p, i);
    i++;
  }

  i = 0;
  for (const auto& p : _lines) {
    _lgrid.add(p, i);
    i++;
  }

  LOG(INFO) << "...done";
  _m.unlock();
}

// _____________________________________________________________________________
std::string Requestor::queryUrl(std::string query) const {
  std::stringstream ss;

  if (util::toLower(query).find("limit") == std::string::npos) {
    query += " LIMIT 18446744073709551615";
  }

  auto esc = curl_easy_escape(_curl, query.c_str(), query.size());

  ss << _backendUrl << "/?send=18446744073709551615"
     << "&query=" << esc;

  return ss.str();
}

// _____________________________________________________________________________
const ResObj Requestor::getNearest(util::geo::FPoint rp, double rad) const {
  // points

  auto box = pad(getBoundingBox(rp), rad);

  std::set<size_t> ret;
  _pgrid.get(box, &ret);

  size_t nearest = 0;
  double dBest = std::numeric_limits<double>::max();

  for (const auto& i : ret) {
    auto p = _points[i];

    double d = util::geo::dist(p, rp);

    if (d < dBest) {
      nearest = i;
      dBest = d;
    }
  }

  // lines
  size_t nearestL = 0;
  double dBestL = std::numeric_limits<double>::max();

  std::set<size_t> retL;
  _lgrid.get(box, &retL);

  for (const auto& i : retL) {
    auto l = _lines[i];

    double d = util::geo::dist(l, rp);

    if (d < dBestL) {
      nearestL = i;
      dBestL = d;
    }
  }

  if (dBest < rad && dBest < dBestL) {
    return {true, _points[nearest], _cols[_pcols[nearest]]};
  }

  if (dBestL < rad && dBestL < dBest) {
    return {true, util::geo::PolyLine<float>(_lines[nearestL]).projectOn(rp).p,
            _cols[_lcols[nearestL]]};
  }

  return {false, {0, 0}, {}};
}
