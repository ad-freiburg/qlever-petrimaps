// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_SPARQLCACHE_H_
#define PETRIMAPS_SPARQLCACHE_H_

#include "qlever-petrimaps/GeomCache.h"

namespace petrimaps {
class SPARQLCache : public GeomCache {
 public:
  SPARQLCache() {
    _backendUrl = "";
    _curl = 0;
  };
  explicit SPARQLCache(const std::string& backendUrl);

  SPARQLCache& operator=(SPARQLCache&& o) {
    _backendUrl = o._backendUrl;
    _curl = curl_easy_init();
    _lines = std::move(o._lines);
    _linePoints = std::move(o._linePoints);
    _points = std::move(o._points);
    _dangling = o._dangling;
    _state = o._state;
    return *this;
  };

  const util::geo::FPoint& getPoint(ID_TYPE id) const {
    return _points[id];
  }
  size_t getLine(ID_TYPE id) const {
    return _lines[id];
  }
  size_t getLineEnd(ID_TYPE id) const {
    return id + 1 < _lines.size() ? getLine(id + 1) : _linePoints.size();
  }

  std::string load(const std::string& cacheFile);
  void request();
  size_t requestSize();
  void requestPart(size_t offset);
  void requestIds();
  void parse(const char*, size_t size);
  void parseIds(const char*, size_t size);
  void parseCount(const char*, size_t size);

  std::pair<std::vector<std::pair<ID_TYPE, ID_TYPE>>, size_t> getRelObjects(
      const std::vector<IdMapping>& id) const;
  const std::string& getBackendURL() const { return _backendUrl; }

  void serializeToDisk(const std::string& fname) const;
  void fromDisk(const std::string& fname);

 private:
  std::string _backendUrl;
  uint8_t _curByte;
  ID _curId;
  QLEVER_ID_TYPE _maxQid;

  static size_t writeCb(void* contents, size_t size, size_t nmemb, void* userp);
  static size_t writeCbIds(void* contents, size_t size, size_t nmemb,
                           void* userp);
  static size_t writeCbCount(void* contents, size_t size, size_t nmemb,
                             void* userp);
  static size_t writeCbString(void* contents, size_t size, size_t nmemb,
                              void* userp);

  // Get right SPARQL query for given backend.
  const std::string& getQuery(const std::string& backendUrl) const;
  const std::string& getCountQuery(const std::string& backendUrl) const;

  std::string requestIndexHash();
  std::string queryUrl(std::string query, size_t offset, size_t limit) const;

  util::geo::DLine parseLineString(const std::string& a, size_t p) const;
  util::geo::FPoint parsePoint(const std::string& a, size_t p) const;

  void insertLine(const util::geo::DLine& l, bool isArea);
  std::string indexHashFromDisk(const std::string& fname);

  size_t _pointsFSize;
  size_t _linePointsFSize;
  size_t _linesFSize;
  size_t _qidToIdFSize;

  std::fstream _pointsF;
  std::fstream _linePointsF;
  std::fstream _linesF;
  std::fstream _qidToIdF;

  IdMapping _lastQidToId;

  std::vector<IdMapping> _qidToId;
  std::vector<util::geo::FPoint> _points;
  std::vector<size_t> _lines;

  std::string _dangling, _prev, _raw;
  ParseState _state;

  std::string _indexHash;

 protected:
  std::exception_ptr _exceptionPtr;
};
}  // namespace petrimaps

#endif  // PETRIMAPS_SPARQLCACHE_H_
