// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_GEOMCACHE_H_
#define PETRIMAPS_GEOMCACHE_H_

#include <curl/curl.h>

#include <atomic>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>

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

  bool ready() const {
    _m.lock();
    bool ready = _ready;
    _m.unlock();
    return ready;
  }

  std::string load(const std::string& cacheFile);

  void request();
  size_t requestSize();
  void requestPart(size_t offset);

  void requestIds();
  void requestIdPart(size_t offset);

  void parse(const char*, size_t size);
  void parseIds(const char*, size_t size);
  void parseCount(const char*, size_t size);

  std::pair<std::vector<std::pair<ID_TYPE, ID_TYPE>>, size_t> getRelObjects(
      const std::vector<IdMapping>& id) const;

  const std::string& getBackendURL() const { return _backendUrl; }

  const std::vector<util::geo::FPoint>& getPoints() const { return _points; }

  const std::vector<util::geo::Point<int16_t>>& getLinePoints() const {
    return _linePoints;
  }

  const std::vector<size_t>& getLines() const { return _lines; }

  util::geo::FBox getPointBBox(size_t id) const {
    return util::geo::getBoundingBox(_points[id]);
  }
  util::geo::DBox getLineBBox(size_t id) const;

  void serializeToDisk(const std::string& fname) const;

  void fromDisk(const std::string& fname);

  size_t getLine(ID_TYPE id) const { return _lines[id]; }

  size_t getLineEnd(ID_TYPE id) const {
    return id + 1 < _lines.size() ? _lines[id + 1] : _linePoints.size();
  }

  double getLoadStatusPercent(bool total);
  double getLoadStatusPercent() { return getLoadStatusPercent(false); };
  int getLoadStatusStage();
  size_t getTotalProgress();
  size_t getCurrentProgress();

 private:
  std::string _backendUrl;
  CURL* _curl;

  uint8_t _curByte;
  ID _curId;
  QLEVER_ID_TYPE _maxQid;
  size_t _totalSize = 0;
  std::atomic<size_t> _curRow;
  std::atomic<size_t> _curIdRow;
  size_t _curUniqueGeom;

  enum _LoadStatusStages { Parse = 1, ParseIds, FromFile, Finished };
  _LoadStatusStages _loadStatusStage = Parse;

  static size_t writeCb(void* contents, size_t size, size_t nmemb, void* userp);
  static size_t writeCbIds(void* contents, size_t size, size_t nmemb,
                           void* userp);
  static size_t writeCbCount(void* contents, size_t size, size_t nmemb,
                             void* userp);
  static size_t writeCbString(void* contents, size_t size, size_t nmemb,
                              void* userp);

  // Get the right SPARQL query for the given backend.
  const std::string& getQuery(const std::string& backendUrl) const;
  std::string getCountQuery(const std::string& backendUrl) const;

  std::string requestIndexHash();

  std::string queryUrl(std::string query, size_t offset, size_t limit) const;

  util::geo::FPoint createPoint(const std::string& a, size_t p) const;

  static bool pointValid(const util::geo::FPoint& p);
  static bool pointValid(const util::geo::DPoint& p);

  static util::geo::DLine createLineString(const std::string& a, size_t p);

	size_t parsePolygon(const std::string& str, size_t p, size_t end, size_t* i);

	size_t parseMultiPoint(const std::string &str, size_t p, size_t end, size_t* i);
	size_t parseMultiLineString(const std::string &str, size_t p, size_t end, size_t* i);
  size_t parseMultiPolygon(const std::string &str, size_t p, size_t end, size_t* i);

  void insertLine(const util::geo::DLine& l, bool isArea);

	static std::vector<size_t> getGeomStarts(const std::string &str, size_t a);

  std::string indexHashFromDisk(const std::string& fname);

  std::vector<util::geo::FPoint> _points;
  std::vector<util::geo::Point<int16_t>> _linePoints;
  std::vector<size_t> _lines;

  size_t _pointsFSize;
  size_t _linePointsFSize;
  size_t _linesFSize;
  size_t _qidToIdFSize;

  size_t _lastBytesReceived;
  std::chrono::time_point<std::chrono::high_resolution_clock> _lastReceivedTime;

  std::fstream _pointsF;
  std::fstream _linePointsF;
  std::fstream _linesF;
  std::fstream _qidToIdF;

  size_t _geometryDuplicates = 0;

  size_t _lastQid = -1;

  IdMapping _lastQidToId;

  std::vector<IdMapping> _qidToId;

  std::string _dangling, _prev, _raw;
  ParseState _state;

  std::exception_ptr _exceptionPtr;

  mutable std::mutex _m;
  bool _ready = false;

  std::string _indexHash;
};
}  // namespace petrimaps

#endif  // PETRIMAPS_GEOMCACHE_H_
