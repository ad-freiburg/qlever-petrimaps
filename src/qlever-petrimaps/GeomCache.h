// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_GEOMCACHE_H_
#define PETRIMAPS_GEOMCACHE_H_

#include <curl/curl.h>

#include <atomic>
#include <chrono>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "qlever-petrimaps/Misc.h"
#include "util/BlockReader.h"
#include "util/geo/Geo.h"
#include "util/log/Log.h"

namespace petrimaps {

const static std::string RASTER_META_QUERY_DEFAULT =
    "PREFIX wp: <https://wald.cs.uni-freiburg.de/ontology#>"
    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    "SELECT ?s ?w ?h WHERE {"
    "  ?s wp:fieldHeight ?h ."
    "  ?s wp:fieldWidth ?w ."
    "}";

const static std::string FILL_QUERY_DEFAULT =
    "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "
    "SELECT ?geometry WHERE {"
    " ?subject geo:asWKT ?geometry "
    " FILTER (!ql:isGeoPoint(?geometry)) "
    "} INTERNAL SORT BY ?geometry";

const static std::string FILL_QUERY_WDTP625 =
    "PREFIX wdt: <http://www.wikidata.org/prop/direct/> "
    "SELECT ?geometry WHERE {"
    " ?subject wdt:P625 ?geometry"
    " FILTER (!ql:isGeoPoint(?geometry)) "
    "} INTERNAL SORT BY ?geometry";

const static std::string FILL_QUERY_WDTP625_SERVICE =
    "PREFIX wdt: <http://www.wikidata.org/prop/direct/> "
    "SELECT ?geometry WHERE {"
    " SERVICE <https://qlever.cs.uni-freiburg.de/api/wikidata> {"
    "  SELECT ?geometry WHERE {"
    "   ?subject wdt:P625 ?geometry"
    "   FILTER (!ql:isGeoPoint(?geometry)) "
    " } INTERNAL SORT BY ?geometry"
    " }"
    "}";

inline std::string getFillQuery(const std::string& backend) {
  auto backendName = util::split(backend, '/').back();
  if (backendName.rfind("wikidata", 0) == 0) return FILL_QUERY_WDTP625;
  if (backendName.rfind("gbif", 0) == 0) return FILL_QUERY_WDTP625;
  if (backendName.rfind("dblp-plus", 0) == 0) return FILL_QUERY_WDTP625;
  if (backendName.rfind("dblp", 0) == 0) return FILL_QUERY_WDTP625_SERVICE;
  return FILL_QUERY_DEFAULT;
}

struct GeomCacheConfig {
  std::string backend;
  std::string fillQuery;
  std::string rasterMetaQuery = RASTER_META_QUERY_DEFAULT;

  std::string getHash() const {
    std::hash<std::string> hashF;
    return std::to_string(hashF(backend + fillQuery + rasterMetaQuery));
  }

  std::string toJSON() const {
    std::stringstream ss;
    ss << "{\"backend\": \"" << util::jsonStringEscape(backend) << "\"";
    ss << ",\"fillQuery\": \"" << util::jsonStringEscape(fillQuery) << "\"";
    ss << "}";
    return ss.str();
  }
};

inline bool operator==(const GeomCacheConfig& l, const GeomCacheConfig& r) {
  return l.backend == r.backend && l.fillQuery == r.fillQuery;
}

inline bool operator!=(const GeomCacheConfig& l, const GeomCacheConfig& r) {
  return !(l == r);
}

typedef std::vector<util::geo::Point<int16_t>,
                    util::no_init_allocator<util::geo::Point<int16_t>>>
    LinePoints;

class GeomCache {
 public:
  GeomCache() : _config(), _curRow(0), _maxMemory(-1) {}
  explicit GeomCache(const GeomCacheConfig& config, size_t maxMemory)
      : _config(config), _curRow(0), _maxMemory(maxMemory) {}

  ~GeomCache() {
    closeQidToIdOut();
    closeLinePoints();
  }

  // owns file descriptors, must not be copied
  GeomCache(const GeomCache&) = delete;
  GeomCache& operator=(const GeomCache&) = delete;

  GeomCache& operator=(GeomCache&& o) {
    _config = o._config;
    _lineBoxes = std::move(o._lineBoxes);
    _lineFlags = std::move(o._lineFlags);
    closeLinePoints();
    _linePointsFd = o._linePointsFd;
    _linePointsOff = o._linePointsOff;
    _numLinePoints = o._numLinePoints;
    _linesOff = o._linesOff;
    _numLines = o._numLines;
    o._linePointsFd = -1;
    _points = std::move(o._points);
    _qidToId = std::move(o._qidToId);
    _dangling = o._dangling;
    _state = o._state;
    return *this;
  };

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

  std::pair<std::vector<std::pair<GID_TYPE, ROW_TYPE>>, size_t> getRelObjects(
      const std::vector<IdMapping>& id) const;

  const GeomCacheConfig& getConfig() const { return _config; }

  // The datatype value this backend uses for points and how it encodes their
  // coordinates, see `RequestReader::requestGeoPointFormat`.
  GeoPointFormat getGeoPointFormat() const { return _geoPointFormat; }

  const std::vector<util::geo::FPoint,
                    util::no_init_allocator<util::geo::FPoint>>&
  getPoints() const {
    return _points;
  }

  // read lineId points from disk into a buffer
  void readLinePoints(LINEID_TYPE lineId, LinePoints* buf) const;

  util::geo::FBox getPointBBox(size_t id) const {
    return util::geo::getBoundingBox(_points[id]);
  }
  util::geo::DBox getLineBBox(LINEID_TYPE lineId) const {
    const auto& b = _lineBoxes[lineId];
    return util::geo::DBox(
        util::geo::DPoint(b.getLowerLeft().getX() / 10.0,
                          b.getLowerLeft().getY() / 10.0),
        util::geo::DPoint(b.getUpperRight().getX() / 10.0,
                          b.getUpperRight().getY() / 10.0));
  }

  bool isArea(LINEID_TYPE lineId) const {
    return _lineFlags[lineId] & LINE_IS_AREA;
  }

  bool isInnerArea(LINEID_TYPE lineId) const {
    return _lineFlags[lineId] & LINE_IS_INNER_AREA;
  }

  struct DiskOffsets {
    size_t linePoints;
    size_t lines;
    size_t qidToId;
  };

  DiskOffsets serializeToDisk(const std::string& fname) const;

  void fromDisk(const std::string& fname, size_t blockSize = 1024 * 1024);

  bool setConfig(const GeomCacheConfig& cfg) {
    if (_config.fillQuery != cfg.fillQuery) {
      _config = cfg;
      return true;
    }
    return false;
  }

  static std::string indexHashFromDisk(const std::string& fname);
  static std::string fillQueryFromDisk(const std::string& fname);

  std::pair<double, double> getRasterMeta(size_t did) const;

  double getLoadStatusPercent(bool total);
  double getLoadStatusPercent() { return getLoadStatusPercent(false); };
  int getLoadStatusStage();
  size_t getTotalProgress();
  size_t getCurrentProgress();

 private:
  GeomCacheConfig _config;

  uint8_t _curByte;
  ID _curId;
  size_t _totalSize = 0;
  std::atomic<size_t> _curRow;
  size_t _curUniqueGeom;
  size_t _maxMemory;

  enum _LoadStatusStages { Parse = 1, ParseIds, FromFile, Finished };
  _LoadStatusStages _loadStatusStage = Parse;

  // Get the right SPARQL query for the given backend.
  const std::string& getFillQuery() const;
  std::string getCountQuery() const;

  std::string requestIndexHash();

  GeoPointFormat requestGeoPointFormat();

  void requestRasterMeta();

  std::string queryFields(std::string query, size_t offset, size_t limit) const;

  void addPolygon(const util::geo::Polygon<double>& p, size_t* i);
  void addMultiPoint(const util::geo::MultiPoint<double>& mp, size_t* i);
  void addMultiLineString(const util::geo::MultiLine<double>& ml, size_t* i);
  void addLineString(const util::geo::Line<double>& l, size_t* i);
  void addMultiPolygon(const util::geo::MultiPolygon<double>& mp, size_t* i);

  void insertLine(const util::geo::DLine& l, bool isArea, bool isInner = false);

  static util::geo::DPoint projD(const util::geo::DPoint& p,
                                 util::geo::CRSType sourceCRS) {
    return util::geo::projectToWebMerc<double>(p, sourceCRS);
  }

  std::vector<util::geo::FPoint, util::no_init_allocator<util::geo::FPoint>>
      _points;

  static const uint8_t LINE_IS_AREA = 1;
  static const uint8_t LINE_IS_INNER_AREA = 2;
  std::vector<util::geo::I32Box, util::no_init_allocator<util::geo::I32Box>>
      _lineBoxes;
  std::vector<uint8_t, util::no_init_allocator<uint8_t>> _lineFlags;

  int _linePointsFd = -1;
  size_t _linePointsOff = 0;
  size_t _numLinePoints = 0;
  size_t _linesOff = 0;
  size_t _numLines = 0;

  int _linePointsTmpFd = -1;
  int _linesTmpFd = -1;

  void openLinePoints(const std::string& fname, size_t linePointsOff,
                      size_t numLinePoints, size_t linesOff, size_t numLines);
  void closeLinePoints();
  void readLineStarts(size_t first, size_t num, size_t* out) const;

  static const size_t LINE_POINTS_WINDOW = 2 * 1024 * 1024;
  void readLinePoints(size_t start, size_t num,
                      util::geo::Point<int16_t>* out) const;
  void buildLineMetas();

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

  std::map<size_t, std::pair<double, double>> _rasterMeta;

  size_t _geometryDuplicates = 0;

  size_t _lastQid = -1;

  IdMapping _lastQidToId;

  IdMapping _nextPlaceholder;
  bool _hasNextPlaceholder = false;
  size_t _placeholdersRead = 0;

  int _qidToIdOutFd = -1;
  size_t _qidToIdOutSize = 0;
  std::vector<IdMapping> _qidToIdOutBuf;
  QLEVER_ID_TYPE _qidToIdOutLastQid = 0;
  bool _qidToIdOutSorted = true;

  static const size_t QID_TO_ID_OUT_BUF = 64 * 1024;
  static const size_t QID_TO_ID_SORT_MEM = 512 * 1024 * 1024;

  static int cmpQidToId(const void* a, const void* b);

  void readPlaceholder();
  void writeQidToId(const IdMapping& idm);
  void flushQidToIdOut();
  void closeQidToIdOut();

  util::BlockReader<IdMapping> _qidToId;

  std::string _dangling, _prev, _raw;
  ParseState _state;

  mutable std::mutex _m;
  bool _ready = false;

  std::string _indexHash;

  GeoPointFormat _geoPointFormat;
};
}  // namespace petrimaps

#endif  // PETRIMAPS_GEOMCACHE_H_
