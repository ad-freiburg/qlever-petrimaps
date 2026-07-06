// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <curl/curl.h>
#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>

#include "qlever-petrimaps/GeomCache.h"
#include "qlever-petrimaps/Misc.h"
#include "qlever-petrimaps/server/Requestor.h"
#include "util/Misc.h"
#include "util/geo/Geo.h"
#include "util/geo/PolyLine.h"
#include "util/log/Log.h"

using petrimaps::GeomCache;
using util::geo::DPoint;
using util::geo::FPoint;
using util::geo::latLngToWebMerc;
using util::geo::lineFromWKTProj;
using util::geo::multiLineFromWKTProj;
using util::geo::multiPointFromWKTProj;
using util::geo::multiPolygonFromWKTProj;
using util::geo::pointFromWKTProj;
using util::geo::polygonFromWKTProj;
using util::LogLevel::DEBUG;
using util::LogLevel::ERROR;
using util::LogLevel::INFO;
using util::LogLevel::WARN;

// _____________________________________________________________________________
const std::string &GeomCache::getFillQuery() const { return _config.fillQuery; }

// _____________________________________________________________________________
std::string GeomCache::getCountQuery() const {
  // Modify the query from `getFillQuery` to count the number of geometries.
  std::string query = getFillQuery();
  auto pos = query.find("SELECT");
  if (pos == std::string::npos) {
    LOG(ERROR) << "Could not find SELECT in query: " << query;
    return "SELECT ?count WHERE { VALUES ?count { 0 } }";
  }
  util::replaceAll(query, "INTERNAL SORT BY ?geometry", "");
  query.insert(pos, "SELECT (COUNT(?geometry) AS ?count) WHERE { ");
  query.append(" }");
  return query;
}

// _____________________________________________________________________________
void GeomCache::parse(const char *c, size_t size) {
  _loadStatusStage = _LoadStatusStages::Parse;

  _lastBytesReceived += size;

  const char *start = c;
  while (c < start + size) {
    if (_raw.size() < 1000) _raw.push_back(*c);
    switch (_state) {
      case IN_HEADER:
        if (*c == '\n') {
          _state = IN_ROW;
          c++;
          continue;
        } else {
          c++;
          continue;
        }
      case IN_ROW:
        if (*c == '\n') {
          // if the previous was not a multi geometry, and if the strings
          // match exactly, re-use the geometry
          if (_prev == _dangling && _lastQidToId.qid == 0) {
            IdMapping idm{0, _lastQidToId.id};
            _lastQidToId = idm;
            _qidToIdF.write(reinterpret_cast<const char *>(&idm),
                            sizeof(IdMapping));
            _qidToIdFSize++;
          } else {
            const char *s = 0;
            auto wktType = util::geo::getWKTType(_dangling.c_str(), &s);
            size_t i = 0;

            if (wktType == util::geo::WKTType::COLLECTION) {
              _curUniqueGeom++;
              const auto &coll =
                  util::geo::collectionFromWKTProj<double>(s, 0, &projD);

              for (const auto &g : coll) {
                if (g.getType() == 0) addMultiPoint({g.getPoint()}, &i);
                if (g.getType() == 1) addLineString(g.getLine(), &i);
                if (g.getType() == 2) addPolygon(g.getPolygon(), &i);
                if (g.getType() == 3) addMultiLineString(g.getMultiLine(), &i);
                if (g.getType() == 4) addMultiPolygon(g.getMultiPolygon(), &i);
                if (g.getType() == 6) addMultiPoint(g.getMultiPoint(), &i);
              }
            } else if (wktType == util::geo::WKTType::MULTIPOINT) {
              _curUniqueGeom++;
              const auto &mp = multiPointFromWKTProj<double>(s, 0, &projD);
              addMultiPoint(mp, &i);
            } else if (wktType == util::geo::WKTType::POINT) {
              _curUniqueGeom++;
              const auto &mp = multiPointFromWKTProj<double>(s, 0, &projD);
              addMultiPoint(mp, &i);
            } else if (wktType == util::geo::WKTType::MULTILINESTRING) {
              _curUniqueGeom++;
              const auto &ml = multiLineFromWKTProj<double>(s, 0, &projD);
              addMultiLineString(ml, &i);
            } else if (wktType == util::geo::WKTType::LINESTRING) {
              _curUniqueGeom++;
              const auto &l = lineFromWKTProj<double>(s, 0, &projD);
              addLineString(l, &i);
            } else if (wktType == util::geo::WKTType::MULTIPOLYGON) {
              _curUniqueGeom++;
              const auto &mp = multiPolygonFromWKTProj<double>(s, 0, &projD);
              addMultiPolygon(mp, &i);
            } else if (wktType == util::geo::WKTType::POLYGON) {
              _curUniqueGeom++;
              const auto &poly = polygonFromWKTProj<double>(s, 0, &projD);
              addPolygon(poly, &i);
            }

            // dummy element to keep sync
            if (i == 0) {
              IdMapping idm{0, std::numeric_limits<ID_TYPE>::max()};
              _lastQidToId = idm;
              _qidToIdF.write(reinterpret_cast<const char *>(&idm),
                              sizeof(IdMapping));
              _qidToIdFSize++;
            }
          }

          _curRow++;
          if (_curRow % 1000000 == 0) {
            LOG(INFO) << "[GEOMCACHE] "
                      << "@ " << _curRow << " (" << std::fixed
                      << std::setprecision(2) << getLoadStatusPercent() << "%, "
                      << _pointsFSize << " (multi-)points, " << _linesFSize
                      << " (open) polygons (with " << _linePointsFSize
                      << " points), " << _geometryDuplicates << " duplicates, "
                      << ((_lastBytesReceived / (1024.0 * 1024.0)) /
                          (TOOK(_lastReceivedTime) / 1000000000.0))
                      << " MB/s)";

            _lastReceivedTime = TIME();
            _lastBytesReceived = 0;
          }
          _prev = std::move(_dangling);
          _dangling.clear();
          _dangling.reserve(10000);
          c++;
        }
        {
          auto end =
              static_cast<const char *>(memchr(c, '\n', size - (c - start)));
          if (end) {
            _dangling.append(c, end - c);
            c = end;
          } else {
            _dangling.append(c, size - (c - start));
            c = start + size;
          }
        }
        break;
      default:
        break;
    }
  }
}

// _____________________________________________________________________________
double GeomCache::getLoadStatusPercent(bool total) {
  /*
  There are 2 loading stages: Parse, afterwards ParseIds.
  Because ParseIds is usually pretty short, we merge the progress of both stages
  to one total progress. Progress is calculated by _curRow / _totalSize, which
  are handled by each stage individually.
  */
  if (_totalSize == 0) {
    return 0.0;
  }

  if (!total) {
    double percent = _curRow / static_cast<double>(_totalSize) * 100.0;
    return std::min(100.0, percent);
  }

  double parsePercent = 95.0;
  double parseIdsPercent = 5.0;
  double totalPercent = 0.0;
  switch (_loadStatusStage) {
    case _LoadStatusStages::Parse:
      totalPercent = _curRow / static_cast<double>(_totalSize) * parsePercent;
      break;

    case _LoadStatusStages::ParseIds:
      totalPercent = parsePercent;
      totalPercent +=
          _curRow / static_cast<double>(_totalSize) * parseIdsPercent;
      break;

    case _LoadStatusStages::FromFile:
      totalPercent = _curRow / static_cast<double>(_totalSize) * 100.0;
      break;

    case _LoadStatusStages::Finished:
      totalPercent = 0;
      break;
  }

  return std::min(100.0, totalPercent);
}

// _____________________________________________________________________________
int GeomCache::getLoadStatusStage() { return _loadStatusStage; }

// _____________________________________________________________________________
size_t GeomCache::getTotalProgress() { return _totalSize; }

// _____________________________________________________________________________
size_t GeomCache::getCurrentProgress() { return _curRow; }

// _____________________________________________________________________________
void GeomCache::parseIds(const char *c, size_t size) {
  for (size_t i = 0; i < size; i++) {
    if (_raw.size() < 1000) _raw.push_back(c[i]);
    _curId.bytes[_curByte] = c[i];
    _curByte = (_curByte + 1) % 8;

    if (_curByte == 0) {
      _curRow++;

      if (_curRow % 1000000 == 0) {
        LOG(INFO) << "[GEOMCACHE] "
                  << "@ " << _curRow << " (" << std::fixed
                  << std::setprecision(2) << getLoadStatusPercent() << "%, "
                  << _pointsFSize << " (multi-)points, " << _linesFSize
                  << " (open) polygons (with " << _linePointsFSize
                  << " points), " << _geometryDuplicates << " duplicates)";
      }

      if (_curIdRow < _qidToId.size() && _qidToId[_curIdRow].qid == 0) {
        // if we have two consecutive and equivalent QLever ids, the geometry
        // was returned multiple times in the fill query. This can happen if the
        // same WKT string is used in multiple distinct objects, but then stored
        // in qlever using the same internal qlever ID. To avoid a false multi-
        // plication of results (all geoms of matching qlever ID are joined), we
        // set such repeated qlever IDs to an unused dummy value.
        if (_lastQid == _curId.val) {
          LOG(DEBUG) << "Found duplicate internal qlever ID " << _curId.val
                     << " for row " << _curRow
                     << ", ignoring this geometry duplicate!";
          _qidToId[_curIdRow].qid = -1;
          _geometryDuplicates++;
        } else {
          _qidToId[_curIdRow].qid = _curId.val;
        }
        _lastQid = _curId.val;
        if (_curId.val > _maxQid) _maxQid = _curId.val;
      } else {
        LOG(WARN) << "The results for the binary IDs are out of sync.";
      }

      // if a qlever entity contained multiple geometries (MULTILINESTRING,
      // MULTIPOLYGON, MULTIPOINT), they appear consecutively in
      // _qidToId; continuation geometries are marked by a
      // preliminary qlever ID of 1, while the first geometry always has a
      // preliminary id of 0
      while (_curIdRow + 1 < _qidToId.size() - 1 &&
             _qidToId[_curIdRow + 1].qid == 1) {
        _qidToId[++_curIdRow].qid = _curId.val;
      }

      _curIdRow++;
    }
  }
}

// _____________________________________________________________________________
void GeomCache::parseCount(const char *c, size_t size) {
  for (size_t i = 0; i < size; i++) {
    if (_raw.size() < 1000) _raw.push_back(c[i]);
    if (c[i] == '\n') _state = IN_ROW;
    if (_state == IN_ROW) _dangling += c[i];
  }
}

// _____________________________________________________________________________
size_t GeomCache::requestSize() {
  _state = IN_HEADER;
  _dangling.clear();
  _dangling.reserve(10000);
  _raw.clear();
  _raw.reserve(1000);

  const std::string &countQuery = getCountQuery();
  LOG(INFO) << "[GEOMCACHE] Count query to obtain the number of geometries:"
            << std::endl
            << countQuery;
  auto flds = queryFields(countQuery, 0, 1);

  try {
    performCurlRequest(
        _config.backend, flds, "text/tab-separated-values", "",
        [this](const char *c, size_t n) { parseCount(c, n); }, &_raw);
  } catch (const std::exception &e) {
    LOG(ERROR) << "[GEOMCACHE] Count query failed: " << e.what();
    return 0;
  }

  std::istringstream iss(_dangling);
  size_t ret;
  iss >> ret;
  return ret;
}

// _____________________________________________________________________________
void GeomCache::requestPart(size_t offset) {
  _state = IN_HEADER;
  _dangling.clear();
  _dangling.reserve(10000);
  _raw.clear();
  _raw.reserve(1000);
  _lastReceivedTime = TIME();
  _lastBytesReceived = 0;

  auto flds = queryFields(getFillQuery(), offset, 10000000);
  performCurlRequest(
      _config.backend, flds, "text/tab-separated-values", "",
      [this](const char *c, size_t n) { parse(c, n); }, &_raw);
}

// _____________________________________________________________________________
void GeomCache::request() {
  _totalSize = requestSize();
  _geometryDuplicates = 0;

  if (_totalSize == 0) {
    LOG(WARN)
        << "Could not determine number of rows, or number of rows was 0 (this "
           "may happen if your dataset only contains folded points)";
  }

  _state = IN_HEADER;
  _points.clear();
  _lines.clear();
  _linePoints.clear();
  _qidToId.clear();

  _lastQidToId = {-1, -1};

  _raw.clear();
  _raw.reserve(1000);

  char *pointsFName = strdup("pointsXXXXXX");
  int i = mkstemp(pointsFName);
  if (i == -1) throw std::runtime_error("Could not create temporary file");
  close(i);
  _pointsF.open(pointsFName, std::ios::out | std::ios::in | std::ios::binary);

  char *linePointsFName = strdup("linepointsXXXXXX");
  i = mkstemp(linePointsFName);
  if (i == -1) throw std::runtime_error("Could not create temporary file");
  close(i);
  _linePointsF.open(linePointsFName,
                    std::ios::out | std::ios::in | std::ios::binary);

  char *linesFName = strdup("linesXXXXXX");
  i = mkstemp(linesFName);
  if (i == -1) throw std::runtime_error("Could not create temporary file");
  close(i);
  _linesF.open(linesFName, std::ios::out | std::ios::in | std::ios::binary);

  char *qidToIdFName = strdup("qidtoidXXXXXX");
  i = mkstemp(qidToIdFName);
  if (i == -1) throw std::runtime_error("Could not create temporary file");
  close(i);
  _qidToIdF.open(qidToIdFName, std::ios::out | std::ios::in | std::ios::binary);

  // immediately unlink
  unlink(pointsFName);
  unlink(linePointsFName);
  unlink(linesFName);
  unlink(qidToIdFName);

  free(pointsFName);
  free(linePointsFName);
  free(linesFName);
  free(qidToIdFName);

  _pointsFSize = 0;
  _linePointsFSize = 0;
  _linesFSize = 0;
  _qidToIdFSize = 0;

  _curRow = 0;
  _curUniqueGeom = 0;

  size_t lastNum = -1;

  LOG(INFO) << "[GEOMCACHE] Total request size: " << _totalSize;
  LOG(INFO) << "[GEOMCACHE] Query is:\n" << getFillQuery();

  while (lastNum != 0) {
    size_t offset = _curRow;
    requestPart(offset);
    lastNum = _curRow - offset;
  }

  LOG(INFO) << "[GEOMCACHE] Received " << _curRow << " rows";

  if (_curRow != _totalSize) {
    LOG(WARN) << "Last received row was " << _curRow << ", but expected "
              << _totalSize << " rows (determined via count query)";
    LOG(WARN) << "Last answer from QLever began with " << _raw;
  }

  LOG(INFO) << "[GEOMCACHE] Building vectors...";

  checkMem(sizeof(util::geo::FPoint) * _pointsFSize, _maxMemory);
  _points.resize(_pointsFSize);
  _pointsF.seekg(0);
  _pointsF.read(reinterpret_cast<char *>(&_points[0]),
                sizeof(util::geo::FPoint) * _pointsFSize);
  _pointsF.close();

  checkMem(sizeof(util::geo::Point<int16_t>) * _linePointsFSize, _maxMemory);
  _linePoints.resize(_linePointsFSize);
  _linePointsF.seekg(0);
  _linePointsF.read(reinterpret_cast<char *>(&_linePoints[0]),
                    sizeof(util::geo::Point<int16_t>) * _linePointsFSize);
  _linePointsF.close();

  checkMem(sizeof(size_t) * _linesFSize, _maxMemory);
  _lines.resize(_linesFSize);
  _linesF.seekg(0);
  _linesF.read(reinterpret_cast<char *>(&_lines[0]),
               sizeof(size_t) * _linesFSize);
  _linesF.close();

  checkMem(sizeof(IdMapping) * _qidToIdFSize, _maxMemory);
  _qidToId.resize(_qidToIdFSize);
  _qidToIdF.seekg(0);
  _qidToIdF.read(reinterpret_cast<char *>(&_qidToId[0]),
                 sizeof(IdMapping) * _qidToIdFSize);
  _qidToIdF.close();

  LOG(INFO) << "[GEOMCACHE] Done";
  LOG(INFO) << "[GEOMCACHE] Received " << _curUniqueGeom << " unique geoms ("
            << _geometryDuplicates << " geometry duplicates transferred)";
  LOG(INFO) << "[GEOMCACHE] Received " << _points.size() << " points and "
            << _lines.size() << " lines";
}

// _____________________________________________________________________________
void GeomCache::requestIds() {
  _loadStatusStage = _LoadStatusStages::ParseIds;

  _curByte = 0;
  _curRow = 0;
  _curIdRow = 0;
  _curUniqueGeom = 0;
  _maxQid = 0;
  _lastQid = -1;

  LOG(INFO) << "[GEOMCACHE] Query is " << getFillQuery();

  size_t lastNum = -1;

  while (lastNum != 0) {
    size_t offset = _curRow;
    requestIdPart(offset);
    lastNum = _curRow - offset;
  }

  if (_curRow != _totalSize) {
    LOG(WARN) << "Last received row was " << _curRow << ", but expected "
              << _totalSize << " rows (determined via count query)";
    LOG(WARN) << "Last answer from QLever began with " << _raw;
  }

  LOG(INFO) << "[GEOMCACHE] Received " << _curRow << " rows";
  LOG(INFO) << "[GEOMCACHE] Done";

  // sorting by qlever id
  LOG(INFO) << "[GEOMCACHE] Sorting results by qlever ID...";
  std::stable_sort(_qidToId.begin(), _qidToId.end());
  LOG(INFO) << "[GEOMCACHE] ... done";
}

// _____________________________________________________________________________
void GeomCache::requestIdPart(size_t offset) {
  auto flds = queryFields(getFillQuery(), offset, 100000000);
  performCurlRequest(
      _config.backend, flds, "application/octet-stream", "",
      [this](const char *c, size_t n) { parseIds(c, n); }, &_raw);
}

// _____________________________________________________________________________
std::string GeomCache::queryFields(std::string query, size_t offset,
                                   size_t limit) const {
  std::stringstream ss;

  if (util::toLower(query).find("limit") == std::string::npos) {
    query += " LIMIT " + std::to_string(limit);
  }

  if (util::toLower(query).find("offset") == std::string::npos) {
    query += " OFFSET " + std::to_string(offset);
  }

  // TODO: dont spin up an entire CURL instance here, is this necessary?
  CURL *curl = curl_easy_init();
  auto esc = curl_easy_escape(curl, query.c_str(), query.size());

  ss << "send=" << std::to_string(MAXROWS) << "&query=" << esc;

  curl_free(esc);
  curl_easy_cleanup(curl);

  return ss.str();
}

// _____________________________________________________________________________
void GeomCache::addMultiPoint(const util::geo::MultiPoint<double> &mp,
                              size_t *i) {
  for (const auto &point : mp) {
    FPoint fpoint{point.getX(), point.getY()};
    _pointsF.write(reinterpret_cast<const char *>(&fpoint),
                   sizeof(util::geo::FPoint));
    _pointsFSize++;
    if (_pointsFSize >= I_OFFSET) {
      std::stringstream ss;
      ss << "Maximum number of points (" << I_OFFSET << ") exceeded.";
      throw std::runtime_error(ss.str());
    }
    IdMapping idm{*i == 0 ? 0 : 1, _pointsFSize - 1};
    _lastQidToId = idm;
    _qidToIdF.write(reinterpret_cast<const char *>(&idm), sizeof(IdMapping));
    _qidToIdFSize++;
    (*i)++;
  }
}

// _____________________________________________________________________________
void GeomCache::addMultiPolygon(const util::geo::MultiPolygon<double> &mp,
                                size_t *i) {
  for (const auto &poly : mp) {
    if (poly.getOuter().size() != 0) {
      _linesF.write(reinterpret_cast<const char *>(&_linePointsFSize),
                    sizeof(size_t));
      _linesFSize++;
      insertLine(poly.getOuter(), true);

      if (_linesFSize - 1 >= std::numeric_limits<ID_TYPE>::max() - I_OFFSET) {
        std::stringstream ss;
        ss << "Maximum number of non-point objects ("
           << std::numeric_limits<ID_TYPE>::max() - I_OFFSET << ") exceeded.";
        throw std::runtime_error(ss.str());
      }

      IdMapping idm{*i == 0 ? 0 : 1, I_OFFSET + _linesFSize - 1};
      _lastQidToId = idm;
      _qidToIdF.write(reinterpret_cast<const char *>(&idm), sizeof(IdMapping));
      _qidToIdFSize++;
      (*i)++;
    }

    for (const auto &line : poly.getInners()) {
      if (line.size() != 0) {
        _linesF.write(reinterpret_cast<const char *>(&_linePointsFSize),
                      sizeof(size_t));
        _linesFSize++;
        insertLine(line, true, true);

        if (_linesFSize - 1 >= std::numeric_limits<ID_TYPE>::max() - I_OFFSET) {
          std::stringstream ss;
          ss << "Maximum number of non-point objects ("
             << std::numeric_limits<ID_TYPE>::max() - I_OFFSET << ") exceeded.";
          throw std::runtime_error(ss.str());
        }

        IdMapping idm{*i == 0 ? 0 : 1, I_OFFSET + _linesFSize - 1};
        _lastQidToId = idm;
        _qidToIdF.write(reinterpret_cast<const char *>(&idm),
                        sizeof(IdMapping));
        _qidToIdFSize++;
        (*i)++;
      }
    }
  }
}

// _____________________________________________________________________________
void GeomCache::addLineString(const util::geo::Line<double> &line, size_t *i) {
  if (line.size() != 0) {
    _linesF.write(reinterpret_cast<const char *>(&_linePointsFSize),
                  sizeof(size_t));
    _linesFSize++;
    insertLine(line, false);

    if (_linesFSize - 1 >= std::numeric_limits<ID_TYPE>::max() - I_OFFSET) {
      std::stringstream ss;
      ss << "Maximum number of non-point objects ("
         << std::numeric_limits<ID_TYPE>::max() - I_OFFSET << ") exceeded.";
      throw std::runtime_error(ss.str());
    }

    IdMapping idm{*i == 0 ? 0 : 1, I_OFFSET + _linesFSize - 1};
    _lastQidToId = idm;
    _qidToIdF.write(reinterpret_cast<const char *>(&idm), sizeof(IdMapping));
    _qidToIdFSize++;
    (*i)++;
  }
}

// _____________________________________________________________________________
void GeomCache::addMultiLineString(const util::geo::MultiLine<double> &ml,
                                   size_t *i) {
  for (const auto &line : ml) {
    if (line.size() != 0) {
      _linesF.write(reinterpret_cast<const char *>(&_linePointsFSize),
                    sizeof(size_t));
      _linesFSize++;
      insertLine(line, false);

      if (_linesFSize - 1 >= std::numeric_limits<ID_TYPE>::max() - I_OFFSET) {
        std::stringstream ss;
        ss << "Maximum number of non-point objects ("
           << std::numeric_limits<ID_TYPE>::max() - I_OFFSET << ") exceeded.";
        throw std::runtime_error(ss.str());
      }

      IdMapping idm{*i == 0 ? 0 : 1, I_OFFSET + _linesFSize - 1};
      _lastQidToId = idm;
      _qidToIdF.write(reinterpret_cast<const char *>(&idm), sizeof(IdMapping));
      _qidToIdFSize++;
      (*i)++;
    }
  }
}

// _____________________________________________________________________________
void GeomCache::addPolygon(const util::geo::Polygon<double> &poly, size_t *i) {
  if (poly.getOuter().size() != 0) {
    _linesF.write(reinterpret_cast<const char *>(&_linePointsFSize),
                  sizeof(size_t));
    _linesFSize++;
    insertLine(poly.getOuter(), true);

    if (_linesFSize - 1 >= std::numeric_limits<ID_TYPE>::max() - I_OFFSET) {
      std::stringstream ss;
      ss << "Maximum number of non-point objects ("
         << std::numeric_limits<ID_TYPE>::max() - I_OFFSET << ") exceeded.";
      throw std::runtime_error(ss.str());
    }

    IdMapping idm{*i == 0 ? 0 : 1, I_OFFSET + _linesFSize - 1};
    _lastQidToId = idm;
    _qidToIdF.write(reinterpret_cast<const char *>(&idm), sizeof(IdMapping));
    _qidToIdFSize++;
    (*i)++;
  }

  for (const auto &inner : poly.getInners()) {
    if (inner.size() != 0) {
      _linesF.write(reinterpret_cast<const char *>(&_linePointsFSize),
                    sizeof(size_t));
      _linesFSize++;
      insertLine(inner, true, true);

      if (_linesFSize - 1 >= std::numeric_limits<ID_TYPE>::max() - I_OFFSET) {
        std::stringstream ss;
        ss << "Maximum number of non-point objects ("
           << std::numeric_limits<ID_TYPE>::max() - I_OFFSET << ") exceeded.";
        throw std::runtime_error(ss.str());
      }

      IdMapping idm{*i == 0 ? 0 : 1, I_OFFSET + _linesFSize - 1};
      _lastQidToId = idm;
      _qidToIdF.write(reinterpret_cast<const char *>(&idm), sizeof(IdMapping));
      _qidToIdFSize++;
      (*i)++;
    }
  }
}

// _____________________________________________________________________________
std::pair<std::vector<std::pair<ID_TYPE, ID_TYPE>>, size_t>
GeomCache::getRelObjects(const std::vector<IdMapping> &ids) const {
  // (geom id, result row)
  std::vector<std::pair<ID_TYPE, ID_TYPE>> ret;

  // in most cases, the return size will be exactly the size of the ids set
  ret.reserve(ids.size());

  // only counts multi-geometries once
  size_t numObjects = 0;

  size_t i = 0;
  size_t j = 0;

  while (i < ids.size() && j < _qidToId.size()) {
    if (ids[i].qid == _qidToId[j].qid) {
      size_t prefJ = j;

      while (j < _qidToId.size() && ids[i].qid == _qidToId[j].qid) {
        if (ret.size() == 0 || ret.back().second != ids[i].id) numObjects++;
        ret.push_back({_qidToId[j].id, ids[i].id});
        j++;
      }

      j = prefJ;
      i++;
    } else if (ids[i].qid < _qidToId[j].qid) {
      i++;
    } else {
      size_t gallop = 1;
      do {
        if (j + gallop >= _qidToId.size()) {
          j = std::lower_bound(_qidToId.begin() + j + gallop / 2,
                               _qidToId.end(), ids[i]) -
              _qidToId.begin();
          break;
        }

        if (_qidToId[j + gallop].qid >= ids[i].qid) {
          j = std::lower_bound(_qidToId.begin() + j + gallop / 2,
                               _qidToId.begin() + j + gallop, ids[i]) -
              _qidToId.begin();
          break;
        }

        gallop *= 2;

      } while (true);
    }
  }

  return {ret, numObjects};
}

// _____________________________________________________________________________
void GeomCache::insertLine(const util::geo::DLine &lR, bool isArea,
                           bool isInner) {
  // we also add the line's bounding box here to also
  // compress that
  const auto &bbox = util::geo::getBoundingBox(lR);

  // this is the THRESHOLD from Server.cpp
  auto l = lR;
  if (isArea)
    if (l.size()) l.push_back(l.front());
  l = util::geo::densify(l, 500);

  int16_t mainX = (bbox.getLowerLeft().getX() * 10.0) / M_COORD_GRANULARITY;
  int16_t mainY = (bbox.getLowerLeft().getY() * 10.0) / M_COORD_GRANULARITY;

  if (mainX != 0 || mainY != 0) {
    util::geo::Point<int16_t> p{mCoord(mainX), mCoord(mainY)};
    _linePointsF.write(reinterpret_cast<const char *>(&p),
                       sizeof(util::geo::Point<int16_t>));
    _linePointsFSize++;
  }

  // add bounding box lower left
  int16_t minorXLoc =
      (bbox.getLowerLeft().getX() * 10.0) - mainX * M_COORD_GRANULARITY;
  int16_t minorYLoc =
      (bbox.getLowerLeft().getY() * 10.0) - mainY * M_COORD_GRANULARITY;

  util::geo::Point<int16_t> p{minorXLoc, minorYLoc};
  _linePointsF.write(reinterpret_cast<const char *>(&p),
                     sizeof(util::geo::Point<int16_t>));
  _linePointsFSize++;

  // add bounding box upper left
  int16_t mainXLoc = (bbox.getUpperRight().getX() * 10.0) / M_COORD_GRANULARITY;
  int16_t mainYLoc = (bbox.getUpperRight().getY() * 10.0) / M_COORD_GRANULARITY;
  minorXLoc =
      (bbox.getUpperRight().getX() * 10.0) - mainXLoc * M_COORD_GRANULARITY;
  minorYLoc =
      (bbox.getUpperRight().getY() * 10.0) - mainYLoc * M_COORD_GRANULARITY;
  if (mainXLoc != mainX || mainYLoc != mainY) {
    mainX = mainXLoc;
    mainY = mainYLoc;

    util::geo::Point<int16_t> p{mCoord(mainX), mCoord(mainY)};
    _linePointsF.write(reinterpret_cast<const char *>(&p),
                       sizeof(util::geo::Point<int16_t>));
    _linePointsFSize++;
  }
  p = util::geo::Point<int16_t>{minorXLoc, minorYLoc};
  _linePointsF.write(reinterpret_cast<const char *>(&p),
                     sizeof(util::geo::Point<int16_t>));
  _linePointsFSize++;

  // add line points
  for (const auto &p : l) {
    mainXLoc = (p.getX() * 10.0) / M_COORD_GRANULARITY;
    mainYLoc = (p.getY() * 10.0) / M_COORD_GRANULARITY;

    if (mainXLoc != mainX || mainYLoc != mainY) {
      mainX = mainXLoc;
      mainY = mainYLoc;

      util::geo::Point<int16_t> p{mCoord(mainX), mCoord(mainY)};
      _linePointsF.write(reinterpret_cast<const char *>(&p),
                         sizeof(util::geo::Point<int16_t>));
      _linePointsFSize++;
    }

    int16_t minorXLoc = (p.getX() * 10.0) - mainXLoc * M_COORD_GRANULARITY;
    int16_t minorYLoc = (p.getY() * 10.0) - mainYLoc * M_COORD_GRANULARITY;

    util::geo::Point<int16_t> pp{minorXLoc, minorYLoc};
    _linePointsF.write(reinterpret_cast<const char *>(&pp),
                       sizeof(util::geo::Point<int16_t>));
    _linePointsFSize++;
  }

  // add closing point for area
  if (isArea && l.size()) {
    const auto &p = l.front();
    mainXLoc = (p.getX() * 10.0) / M_COORD_GRANULARITY;
    mainYLoc = (p.getY() * 10.0) / M_COORD_GRANULARITY;

    if (mainXLoc != mainX || mainYLoc != mainY) {
      mainX = mainXLoc;
      mainY = mainYLoc;

      util::geo::Point<int16_t> p{mCoord(mainX), mCoord(mainY)};
      _linePointsF.write(reinterpret_cast<const char *>(&p),
                         sizeof(util::geo::Point<int16_t>));
      _linePointsFSize++;
    }

    int16_t minorXLoc = (p.getX() * 10.0) - mainXLoc * M_COORD_GRANULARITY;
    int16_t minorYLoc = (p.getY() * 10.0) - mainYLoc * M_COORD_GRANULARITY;

    util::geo::Point<int16_t> pp{minorXLoc, minorYLoc};
    _linePointsF.write(reinterpret_cast<const char *>(&pp),
                       sizeof(util::geo::Point<int16_t>));
    _linePointsFSize++;
  }

  // if we have an area, we end in a major coord (which is not possible for
  // other types)
  if (isArea) {
    util::geo::Point<int16_t> p{mCoord(0), mCoord(0)};
    if (isInner) p = {mCoord(1), mCoord(1)};

    _linePointsF.write(reinterpret_cast<const char *>(&p),
                       sizeof(util::geo::Point<int16_t>));
    _linePointsFSize++;
  }
}

// _____________________________________________________________________________
util::geo::DBox GeomCache::getLineBBox(size_t lid) const {
  util::geo::DBox ret;
  size_t start = getLine(lid);

  bool s = false;

  double mainX = 0;
  double mainY = 0;
  for (size_t i = start; i < start + 4; i++) {
    // extract real geom
    const auto &cur = _linePoints[i];

    if (isMCoord(cur.getX())) {
      mainX = rmCoord(cur.getX());
      mainY = rmCoord(cur.getY());
      continue;
    }

    util::geo::DPoint curP((mainX * M_COORD_GRANULARITY + cur.getX()) / 10.0,
                           (mainY * M_COORD_GRANULARITY + cur.getY()) / 10.0);

    if (!s) {
      ret.setLowerLeft(curP);
      s = true;
    } else {
      ret.setUpperRight(curP);
      return ret;
    }
  }

  return ret;
}

// _____________________________________________________________________________
std::string GeomCache::indexHashFromDisk(const std::string &fname) {
  std::ifstream f(fname, std::ios::binary);
  char tmp[100];
  f.read(tmp, 100);
  tmp[99] = 0;

  return util::trim(tmp);
}

// _____________________________________________________________________________
std::string GeomCache::fillQueryFromDisk(const std::string &fname) {
  std::ifstream f(fname, std::ios::binary);
  size_t fillQuerySize;
  std::string fillQuery;

  // skip hash
  f.ignore(100);
  f.read(reinterpret_cast<char *>(&fillQuerySize), sizeof(size_t));
  if (!f) throw std::runtime_error("Corrupted cache file");
  fillQuery.resize(fillQuerySize);
  f.read(reinterpret_cast<char *>(&fillQuery[0]), fillQuerySize);
  if (!f) throw std::runtime_error("Corrupted cache file");

  return fillQuery;
}

// _____________________________________________________________________________
void GeomCache::fromDisk(const std::string &fname, size_t blockSize) {
  _loadStatusStage = _LoadStatusStages::FromFile;

  // a block size of 0 means "single blokc"
  if (blockSize == 0) blockSize = std::numeric_limits<size_t>::max();
  _points.clear();
  _linePoints.clear();
  _lines.clear();

  std::ifstream f(fname, std::ios::binary);

  // load hash
  char tmp[100];
  f.read(tmp, 100);
  tmp[99] = 0;
  _indexHash = util::trim(tmp);

  LOG(INFO) << " Disk cache (" << fname << ") hash is " << _indexHash;
  size_t fillQuerySize;
  std::string fillQuery;
  f.read(reinterpret_cast<char *>(&fillQuerySize), sizeof(size_t));
  if (!f) throw std::runtime_error("Corrupted cache file");
  fillQuery.resize(fillQuerySize);
  f.read(reinterpret_cast<char *>(&fillQuery[0]), fillQuerySize);
  if (!f) throw std::runtime_error("Corrupted cache file");

  LOG(INFO) << " Disk cache (" << fname << ") fill query is " << fillQuery;

  size_t numPoints;
  size_t numLinePoints;
  size_t numLines;
  size_t numQidToId;
  std::streampos posPoints;
  std::streampos posLinePoints;
  std::streampos posLines;
  std::streampos posQidToId;

  // get total num points
  // points
  f.read(reinterpret_cast<char *>(&numPoints), sizeof(size_t));

  checkMem(sizeof(util::geo::FPoint) * numPoints, _maxMemory);
  _points.resize(numPoints);
  posPoints = f.tellg();
  f.seekg(sizeof(util::geo::FPoint) * numPoints, f.cur);
  if (!f) throw std::runtime_error("Corrupted cache file");

  // linePoints
  f.read(reinterpret_cast<char *>(&numLinePoints), sizeof(size_t));
  if (!f) throw std::runtime_error("Corrupted cache file");
  checkMem(sizeof(util::geo::Point<int16_t>) * numLinePoints, _maxMemory);
  _linePoints.resize(numLinePoints);
  posLinePoints = f.tellg();
  f.seekg(sizeof(util::geo::Point<int16_t>) * numLinePoints, f.cur);
  if (!f) throw std::runtime_error("Corrupted cache file");

  // lines
  f.read(reinterpret_cast<char *>(&numLines), sizeof(size_t));
  if (!f) throw std::runtime_error("Corrupted cache file");
  checkMem(sizeof(size_t) * numLines, _maxMemory);
  _lines.resize(numLines);
  posLines = f.tellg();
  f.seekg(sizeof(size_t) * numLines, f.cur);
  if (!f) throw std::runtime_error("Corrupted cache file");

  // qidToId
  f.read(reinterpret_cast<char *>(&numQidToId), sizeof(size_t));
  if (!f) throw std::runtime_error("Corrupted cache file");
  checkMem(sizeof(IdMapping) * numQidToId, _maxMemory);
  _qidToId.resize(numQidToId);
  posQidToId = f.tellg();
  f.seekg(sizeof(IdMapping) * numQidToId, f.cur);
  if (!f) throw std::runtime_error("Corrupted cache file");

  _totalSize = numPoints + numLinePoints + numLines + numQidToId;
  _curRow = 0;

  // read data from files, directly into the vector, in blocks of blockSize
  auto readBlocks = [&](char *data, size_t num, size_t elemSize) {
    for (size_t i = 0; i < num; i += blockSize) {
      size_t n = std::min(blockSize, num - i);
      f.read(data + i * elemSize, elemSize * n);
      if (!f) throw std::runtime_error("Corrupted cache file");
      _curRow += n;
    }
  };

  // points
  f.seekg(posPoints);
  if (!f) throw std::runtime_error("Corrupted cache file");
  readBlocks(reinterpret_cast<char *>(_points.data()), numPoints,
             sizeof(util::geo::FPoint));

  // linePoints
  f.seekg(posLinePoints);
  if (!f) throw std::runtime_error("Corrupted cache file");
  readBlocks(reinterpret_cast<char *>(_linePoints.data()), numLinePoints,
             sizeof(util::geo::Point<int16_t>));

  // lines
  f.seekg(posLines);
  if (!f) throw std::runtime_error("Corrupted cache file");
  readBlocks(reinterpret_cast<char *>(_lines.data()), numLines, sizeof(size_t));

  // qidToId
  f.seekg(posQidToId);
  if (!f) throw std::runtime_error("Corrupted cache file");
  readBlocks(reinterpret_cast<char *>(_qidToId.data()), numQidToId,
             sizeof(IdMapping));

  f.close();
}

// _____________________________________________________________________________
void GeomCache::serializeToDisk(const std::string &fname) const {
  std::ofstream f;
  f.open(fname, std::ios::binary);

  std::string h = _indexHash;
  h.insert(h.end(), 99 - h.size(), ' ');

  // null byte is 100
  assert(h.size() == 99);
  f.write(h.c_str(), 100);

  // fill query
  size_t fillQuerySize = _config.fillQuery.size();
  f.write(reinterpret_cast<const char *>(&fillQuerySize), sizeof(size_t));
  f.write(_config.fillQuery.c_str(), _config.fillQuery.size());

  size_t num = _points.size();
  f.write(reinterpret_cast<const char *>(&num), sizeof(size_t));
  f.write(reinterpret_cast<const char *>(&_points[0]),
          sizeof(util::geo::FPoint) * num);

  num = _linePoints.size();
  f.write(reinterpret_cast<const char *>(&num), sizeof(size_t));
  f.write(reinterpret_cast<const char *>(&_linePoints[0]),
          sizeof(util::geo::Point<int16_t>) * num);

  num = _lines.size();
  f.write(reinterpret_cast<const char *>(&num), sizeof(size_t));
  f.write(reinterpret_cast<const char *>(&_lines[0]), sizeof(size_t) * num);

  num = _qidToId.size();
  f.write(reinterpret_cast<const char *>(&num), sizeof(size_t));
  f.write(reinterpret_cast<const char *>(&_qidToId[0]),
          sizeof(IdMapping) * num);

  f.close();
}

// _____________________________________________________________________________
void GeomCache::requestRasterMeta() {
  auto r = RequestReader(getConfig().backend, _maxMemory, 0, 0, 0);

  _rasterMeta = r.requestRasterMeta(getConfig().rasterMetaQuery, "");

  for (const auto &rm : _rasterMeta) {
    LOG(INFO) << "Configured raster " << rm.first << ": " << rm.second.first
              << "x" << rm.second.second;
  }
}

// _____________________________________________________________________________
std::string GeomCache::requestIndexHash() {
  auto r = RequestReader(getConfig().backend, _maxMemory, 0, 0, 0);

  return r.requestIndexHash(getConfig().getHash());
}

// _____________________________________________________________________________
std::pair<double, double> GeomCache::getRasterMeta(size_t did) const {
  auto i = _rasterMeta.find(did);
  if (i != _rasterMeta.end()) return i->second;

  LOG(WARN) << "[GEOMCACHE] Unknown raster dataset " << did;

  return {10, 10};
}

// _____________________________________________________________________________
std::string GeomCache::load(const std::string &cacheDir) {
  std::lock_guard<std::mutex> guard(_m);

  if (_ready) {
    auto indexHash = requestIndexHash();

    // if the hash size is 0, we could not obtain an index hash from
    // qlever. In this case, just assume they matched
    if (indexHash.size() == 0 || _indexHash == indexHash) return _indexHash;
    LOG(INFO) << "Loaded index hash (" << _indexHash
              << ") and remote index hash (" << indexHash << ") dont match.";
    _ready = false;
  }

  if (cacheDir.size()) {
    std::string backend = getConfig().backend;
    util::replaceAll(backend, "/", "#");
    std::string cacheFile = cacheDir + "/" + backend;

    // why is this called a second time here, reuse!
    auto indexHash = requestIndexHash();

    // if the hash size is 0, we could not obtain an index hash from
    // qlever. In this case, just assume they matched
    if (access(cacheFile.c_str(), F_OK) != -1 &&
        (indexHash.size() == 0 || indexHash == indexHashFromDisk(cacheFile)) &&
        _config.fillQuery == fillQueryFromDisk(cacheFile)) {
      LOG(INFO) << "Reading from cache file " << cacheFile << "...";
      fromDisk(cacheFile);
      LOG(INFO) << "done ...";
    } else {
      if (access(cacheDir.c_str(), W_OK) != 0) {
        std::stringstream ss;
        ss << "No write access to cache dir " << cacheDir;
        throw std::runtime_error(ss.str());
      }
      _indexHash = requestIndexHash();
      LOG(INFO) << "Index hash is '" << _indexHash << "'";
      request();
      requestIds();
      LOG(INFO) << "Serializing to cache file " << cacheFile << "...";
      serializeToDisk(cacheFile);
      LOG(INFO) << "done ...";
    }
  } else {
    _indexHash = requestIndexHash();
    LOG(INFO) << "Index hash is '" << _indexHash << "'";
    request();
    requestIds();
  }

  // read raster meta data
  if (getConfig().rasterMetaQuery.size()) {
    requestRasterMeta();
  }

  _ready = true;
  _loadStatusStage = Finished;

  return _indexHash;
}
