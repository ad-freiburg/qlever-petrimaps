// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>
//          Bohyun Kim <bk233@email.uni-freiburg.de>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdint.h>
#include <sys/socket.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <exception>
#include <fstream>
#include <iomanip>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "qlever-petrimaps/Misc.h"
#include "util/String.h"
#include "util/geo/Geo.h"
#include "util/log/Log.h"
#include "util/xml/XmlWriter.h"

using petrimaps::RequestReader;
using util::LogLevel::ERROR;
using util::LogLevel::INFO;
using util::LogLevel::WARN;

// Probe coordinate used to obtain the point encoding from the backend,
// deliberately not round number and far from the equator and prime meridian, so
// that the two encodings yield results that are nowhere near each other.
#define PROBE_LON 7.835
#define PROBE_LAT 47.999

#define STR_(x) #x
#define STR(x) STR_(x)

// change on each index-breaking change to the code base
const static std::string INDEX_HASH_PREFIX = "_6_";

// max quantized point coordinate (`GeoPoint::maxCoordinateEncoded` in QLever)
const static uint64_t MAX_QUANTIZED_COORD = (uint64_t(1) << 30) - 1;

// _____________________________________________________________________________
static uint64_t everySecondBit(uint64_t bits) {
  // Returns number constructed from every second bit of the input
  bits &= 0x5555555555555555ull;
  bits = (bits | (bits >> 1)) & 0x3333333333333333ull;
  bits = (bits | (bits >> 2)) & 0x0F0F0F0F0F0F0F0Full;
  bits = (bits | (bits >> 4)) & 0x00FF00FF00FF00FFull;
  bits = (bits | (bits >> 8)) & 0x0000FFFF0000FFFFull;
  bits = (bits | (bits >> 16)) & 0x00000000FFFFFFFFull;
  return bits & MAX_QUANTIZED_COORD;
}

// _____________________________________________________________________________
void petrimaps::performCurlRequest(
    const std::string& url, const std::string& postFields,
    const std::string& acceptHeader, const std::string& xRealIP,
    const std::function<void(const char*, size_t)>& parse,
    const std::string* raw) {
  CURL* curl = curl_easy_init();

  if (!curl) {
    throw std::runtime_error("Failed to perform curl request.");
  }

  char errbuf[CURL_ERROR_SIZE];
  errbuf[0] = 0;

  // separate parsing thread to be able to continue receiving while parsing
  CurlParseThread parseThread(parse);

  petrimapsCurlSetup(curl);
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

  // if we have POST fields, add them
  if (postFields.size()) {
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postFields.c_str());
  }

  size_t (*cb)(void* contents, size_t size, size_t nmemb, void* userp) =
      [](void* contents, size_t size, size_t nmemb, void* userp) -> size_t {
    size_t realsize = size * nmemb;
    auto* t = static_cast<CurlParseThread*>(userp);
    if (!t->push(static_cast<const char*>(contents), realsize)) {
      return CURLE_WRITE_ERROR;
    }
    return realsize;
  };

  // any newly read block will be given to the parse() method of the handed cb
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cb);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &parseThread);
  curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);

  struct curl_slist* headers = 0;
  if (acceptHeader.size()) {
    headers = curl_slist_append(headers, ("Accept: " + acceptHeader).c_str());
  }
  if (xRealIP.size()) {
    headers = curl_slist_append(headers, ("X-Real-IP: " + xRealIP).c_str());
  }

  if (headers) curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode res = curl_easy_perform(curl);

  parseThread.finalize();

  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);

  if (headers) curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  // rethrow any exception encountered while parsing
  if (parseThread.exception()) std::rethrow_exception(parseThread.exception());

  if (res != CURLE_OK) {
    std::stringstream ss;
    ss << "QLever backend request failed: ";
    if (strlen(errbuf) > 0) {
      LOG(ERROR) << "[CURL] " << errbuf;
      ss << errbuf;
    } else {
      LOG(ERROR) << "[CURL] " << curl_easy_strerror(res);
      ss << curl_easy_strerror(res);
    }
    throw std::runtime_error(ss.str());
  }

  if (httpCode != 200) {
    std::stringstream ss;
    ss << "QLever backend returned status code " << httpCode;
    if (raw) ss << "\n" << *raw;
    throw std::runtime_error(ss.str());
  }
}

// _____________________________________________________________________________
std::vector<std::string> RequestReader::requestColumns(
    const std::string& query) {
  std::string resString;

  try {
    resString =
        httpRequest(_backendUrl, queryFields(query) + "&action=tsv_export");
  } catch (const std::runtime_error& e) {
    std::stringstream ss;
    ss << "[REQUESTREADER] " << e.what();
    throw std::runtime_error(ss.str());
  }

  return util::split(util::trim(resString), '\t');
}

// _____________________________________________________________________________
void RequestReader::requestIds(const std::string& query,
                               const std::string& remoteAddr) {
  _raw.clear();
  _raw.reserve(10000);

  performCurlRequest(
      _backendUrl, queryFields(query), "application/octet-stream", remoteAddr,
      [this](const char* c, size_t n) { parseIds(c, n); }, &_raw);
}

// _____________________________________________________________________________
std::map<size_t, std::pair<double, double>> RequestReader::requestRasterMeta(
    const std::string& query, const std::string& remoteAddr) {
  _curRasterFieldDimensions = {};

  _raw.clear();
  _raw.reserve(10000);

  performCurlRequest(
      _backendUrl, queryFields(query), "application/octet-stream", remoteAddr,
      [this](const char* c, size_t n) { parseRasterMeta(c, n); }, &_raw);

  return _curRasterFieldDimensions;
}

// _____________________________________________________________________________
void RequestReader::requestRows(const std::string& query,
                                const std::string& remoteAddr) {
  return requestRows(
      query, [this](const char* c, size_t n) { parse(c, n); }, remoteAddr);
}

// _____________________________________________________________________________
void RequestReader::requestRows(
    const std::string& query,
    const std::function<void(const char*, size_t)>& parse,
    const std::string& remoteAddr) {
  _raw.clear();
  _raw.reserve(10000);

  performCurlRequest(_backendUrl, queryFields(query),
                     "text/tab-separated-values", remoteAddr, parse, &_raw);
}

// _____________________________________________________________________________
std::string RequestReader::queryFields(const std::string& query) const {
  // TODO: dont spin up an entire CURL instance here, is this necessary?
  CURL* curl = curl_easy_init();
  auto escStr = curl_easy_escape(curl, query.c_str(), query.size());
  std::string esc = escStr;
  curl_free(escStr);
  curl_easy_cleanup(curl);

  return "send=18446744073709551615&query=" + esc;
}

// _____________________________________________________________________________
size_t petrimaps::writeStringCb(void* contents, size_t size, size_t nmemb,
                                void* userp) {
  ((std::string*)userp)->append((char*)contents, size * nmemb);
  return size * nmemb;
}

// _____________________________________________________________________________
void RequestReader::parseRasterMeta(const char* c, size_t size) {
  for (size_t i = 0; i < size; i++) {
    if (_raw.size() < 10000) _raw.push_back(c[i]);
    _curId.bytes[_curByte] = c[i];
    _curByte = (_curByte + 1) % 8;

    _curIdCol = _curIdCol % 3;

    if (_curByte == 0) {
      uint8_t type = idDatatype(_curId.val);

      if (_curIdCol == 0) {
        // raster dataset it
        _curDatasetId = _curId.val;
      } else if (_curIdCol == 1) {
        // field width
        if (type == 3) {
          // 3 = double in qlever
          uint64_t rawBits = (_curId.val << 4);
          std::memcpy(&_curFieldWidth, &rawBits, sizeof(_curFieldWidth));
        } else if (type == 2) {
          // 2 = int in qlever
          uint64_t rawBits = (_curId.val << 4) >> 4;
          int64_t val = 0;
          std::memcpy(&val, &rawBits, sizeof(val));
          _curFieldWidth = val;
        } else {
          // default value if unparsable
          _curFieldWidth = 1;
        }
      } else if (_curIdCol == 2) {
        // field height
        if (type == 3) {
          // 3 = double in qlever
          uint64_t rawBits = (_curId.val << 4);
          std::memcpy(&_curFieldHeight, &rawBits, sizeof(_curFieldHeight));
        } else if (type == 2) {
          // 2 = int in qlever
          uint64_t rawBits = (_curId.val << 4) >> 4;
          int64_t val = 0;
          std::memcpy(&val, &rawBits, sizeof(val));
          _curFieldHeight = val;
        }

        _curRasterFieldDimensions[_curDatasetId] = {_curFieldWidth,
                                                    _curFieldHeight};
      }
      _curIdCol += 1;
    }
  }
}

// _____________________________________________________________________________
void RequestReader::parseIds(const char* c, size_t size) {
  // TODO: just a rough approximation
  checkMem(size, _maxMemory);

  for (size_t i = 0; i < size; i++) {
    if (_raw.size() < 10000) _raw.push_back(c[i]);
    _curId.bytes[_curByte] = c[i];
    _curByte = (_curByte + 1) % 8;

    _curIdCol = _curIdCol % (_geomFields + _valFields + _rasterMetaFields);

    if (_curByte == 0) {
      if (_curIdCol < _geomFields) {
        // geometry ID
        _ids[_curIdCol].push_back({QLEVER_ID_TYPE{_curId.val}, ID_TYPE{_ids[_curIdCol].size()}});
      } else if (_curIdCol < _valFields + _geomFields) {
        // value

        uint8_t type = idDatatype(_curId.val);
        if (type == 3) {
          // 3 = double in qlever
          uint64_t rawBits = (_curId.val << 4);
          double val = 0;
          std::memcpy(&val, &rawBits, sizeof(val));
          _vals[_curIdCol - _geomFields].push_back(val);
        } else if (type == 2) {
          // 2 = int in qlever
          uint64_t rawBits = (_curId.val << 4) >> 4;
          int64_t val = 0;
          std::memcpy(&val, &rawBits, sizeof(val));
          _vals[_curIdCol - _geomFields].push_back(val);
        } else {
          _vals[_curIdCol - _geomFields].push_back(0);
        }
      } else {
        _rasterMetas[_curIdCol - _geomFields - _valFields].push_back(
            _curId.val);
      }
      _curIdCol += 1;
    }
  }
}

// _____________________________________________________________________________
void RequestReader::parse(const char* c, size_t size) {
  checkMem(size, _maxMemory);

  const char* start = c;
  while (c < start + size) {
    if (_raw.size() < 10000) _raw.push_back(*c);
    switch (_state) {
      case IN_HEADER:
        if (*c == '\t' || *c == '\n') {
          _colNames.push_back(_dangling);
          _dangling.clear();
        }

        if (*c == '\n') {
          _curRow++;
          _state = IN_ROW;
          c++;
          continue;
        } else {
          if (*c != '\t') _dangling += *c;
          c++;
          continue;
        }
      case IN_ROW:
        if (*c == '\t' || *c == '\n') {
          _curCols.push_back({_colNames[_curCol], _dangling});

          if (*c == '\n') {
            _curRow++;
            _rows.push_back(_curCols);
            _curCols = {};
            _curCol = 0;
          } else {
            _curCol++;
          }
          _dangling = "";
          c++;
          continue;
        }

        _dangling += *c;
        c++;

        break;
    }
  }
}

namespace petrimaps {
namespace {

std::string getRequiredCell(
    const std::vector<std::pair<std::string, std::string>>& row,
    const std::string& wantedColumn) {
  for (const auto& cell : row) {
    if (normalizeSparqlResultColumn(cell.first) == wantedColumn) {
      return cell.second;
    }
  }

  throw std::runtime_error("Missing required column: " + wantedColumn);
}

std::string getRequiredCell(const std::vector<std::string>& columns,
                            const std::vector<std::string>& row,
                            const std::string& wantedColumn) {
  for (size_t i = 0; i < columns.size() && i < row.size(); i++) {
    if (normalizeSparqlResultColumn(columns[i]) == wantedColumn) {
      return row[i];
    }
  }

  throw std::runtime_error("Missing required column: " + wantedColumn);
}

std::string doubleToXmlAttr(double value) {
  std::stringstream out;
  out << std::setprecision(15) << value;
  return out.str();
}

std::string intToXmlAttr(int64_t value) {
  return std::to_string(value);
}

using GeneratedNodeIds =
    std::map<std::pair<std::string, std::string>, int64_t>;

int64_t addGeneratedNode(
    OsmPrimitiveStore* store, int64_t* nextNodeId,
    GeneratedNodeIds* generatedNodeIdsByCoordinate,
    const util::geo::DPoint& point) {
  const auto coordinateKey =
      std::make_pair(doubleToXmlAttr(point.getX()),
                     doubleToXmlAttr(point.getY()));

  const auto existing = generatedNodeIdsByCoordinate->find(coordinateKey);
  if (existing != generatedNodeIdsByCoordinate->end()) {
    return existing->second;
  }

  const int64_t id = (*nextNodeId)--;
  generatedNodeIdsByCoordinate->emplace(coordinateKey, id);
  store->insertNode({id, point.getX(), point.getY(), {}});
  return id;
}

int64_t addWayFromLine(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    GeneratedNodeIds* generatedNodeIdsByCoordinate,
    const util::geo::DLine& line,
    const std::unordered_map<std::string, std::string>& tags) {
  OsmWay way;
  way.id = (*nextWayId)--;
  way.tags = tags;

  for (const auto& point : line) {
    way.nodeRefs.push_back(addGeneratedNode(
        store, nextNodeId, generatedNodeIdsByCoordinate, point));
  }

  store->insertWay(way);
  return way.id;
}

int64_t addWayFromRing(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    GeneratedNodeIds* generatedNodeIdsByCoordinate,
    const util::geo::DLine& ring,
    const std::unordered_map<std::string, std::string>& tags) {
  OsmWay way;
  way.id = (*nextWayId)--;
  way.tags = tags;

  const bool inputIsClosed =
      ring.size() > 1 && ring.front() == ring.back();
  auto end = ring.end();
  if (inputIsClosed) {
    --end;
  }

  for (auto it = ring.begin(); it != end; ++it) {
    way.nodeRefs.push_back(addGeneratedNode(
        store, nextNodeId, generatedNodeIdsByCoordinate, *it));
  }

  if (!way.nodeRefs.empty()) {
    way.nodeRefs.push_back(way.nodeRefs.front());
  }

  store->insertWay(way);
  return way.id;
}

void addPolygonMembersToRelation(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    GeneratedNodeIds* generatedNodeIdsByCoordinate,
    const util::geo::DPolygon& polygon, OsmRelation* relation) {
  const std::unordered_map<std::string, std::string> noTags;

  const auto outerWayId = addWayFromRing(
      store, nextNodeId, nextWayId, generatedNodeIdsByCoordinate,
      polygon.getOuter(), noTags);
  relation->members.push_back({"way", outerWayId, "outer"});

  for (const auto& inner : polygon.getInners()) {
    const auto innerWayId =
        addWayFromRing(store, nextNodeId, nextWayId,
                       generatedNodeIdsByCoordinate, inner, noTags);
    relation->members.push_back({"way", innerWayId, "inner"});
  }
}

void setRelationTags(
    OsmRelation* relation,
    const std::unordered_map<std::string, std::string>& tags,
    const std::string& relationType, const std::string& osmId) {
  relation->tags = tags;

  const auto typeIt = relation->tags.find("type");
  if (typeIt != relation->tags.end() &&
      typeIt->second != relationType) {
    throw std::runtime_error(
        "Cannot replace non-" + relationType +
        " type tag for osm_id " + osmId);
  }

  relation->tags["type"] = relationType;
}

OsmRelationMember addWayFromPolygon(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    GeneratedNodeIds* generatedNodeIdsByCoordinate,
    int64_t* nextRelationId, const util::geo::DPolygon& polygon,
    const std::unordered_map<std::string, std::string>& tags,
    const std::string& osmId) {
  const auto& outer = polygon.getOuter();
  const auto& inners = polygon.getInners();

  if (inners.empty()) {
    const auto wayId = addWayFromRing(
        store, nextNodeId, nextWayId, generatedNodeIdsByCoordinate, outer,
        tags);
    return {"way", wayId, ""};
  }

  OsmRelation relation;
  relation.id = (*nextRelationId)--;

  addPolygonMembersToRelation(
      store, nextNodeId, nextWayId, generatedNodeIdsByCoordinate,
      polygon, &relation);
  setRelationTags(&relation, tags, "multipolygon", osmId);

  store->insertRelation(relation);
  return {"relation", relation.id, ""};
}

OsmRelationMember addWayFromMultiPolygon(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    GeneratedNodeIds* generatedNodeIdsByCoordinate,
    int64_t* nextRelationId,
    const util::geo::DMultiPolygon& multiPolygon,
    const std::unordered_map<std::string, std::string>& tags,
    const std::string& osmId) {
  OsmRelation relation;
  relation.id = (*nextRelationId)--;

  for (const auto& polygon : multiPolygon) {
    addPolygonMembersToRelation(store, nextNodeId, nextWayId,
                                generatedNodeIdsByCoordinate, polygon,
                                &relation);
  }

  setRelationTags(&relation, tags, "multipolygon", osmId);
  store->insertRelation(relation);
  return {"relation", relation.id, ""};
}

OsmRelationMember addNodesFromMultiPoint(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextRelationId,
    GeneratedNodeIds* generatedNodeIdsByCoordinate,
    const util::geo::DMultiPoint& multiPoint,
    const std::unordered_map<std::string, std::string>& tags,
    const std::string& osmId) {
  OsmRelation relation;
  relation.id = (*nextRelationId)--;

  for (const auto& point : multiPoint) {
    const auto nodeId = addGeneratedNode(
        store, nextNodeId, generatedNodeIdsByCoordinate, point);
    relation.members.push_back({"node", nodeId, ""});
  }

  setRelationTags(&relation, tags, "multipoint", osmId);
  store->insertRelation(relation);
  return {"relation", relation.id, ""};
}

bool isEscapedLiteralQuote(const std::string& value, size_t quotePosition) {
  size_t precedingBackslashes = 0;

  while (quotePosition > precedingBackslashes &&
         value[quotePosition - precedingBackslashes - 1] == '\\') {
    ++precedingBackslashes;
  }

  return precedingBackslashes % 2 == 1;
}

OsmRelationMember addWaysFromMultiLine(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    int64_t* nextRelationId, GeneratedNodeIds* generatedNodeIdsByCoordinate,
    const util::geo::DMultiLine& multiLine,
    const std::unordered_map<std::string, std::string>& tags,
    const std::string& osmId) {
  OsmRelation relation;
  relation.id = (*nextRelationId)--;

  const std::unordered_map<std::string, std::string> noTags;

  for (const auto& line : multiLine) {
    const auto wayId = addWayFromLine(
        store, nextNodeId, nextWayId, generatedNodeIdsByCoordinate, line,
        noTags);
    relation.members.push_back({"way", wayId, ""});
  }

  setRelationTags(&relation, tags, "multilinestring", osmId);
  store->insertRelation(relation);
  return {"relation", relation.id, ""};
}

OsmRelationMember addCollectionMember(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    int64_t* nextRelationId, GeneratedNodeIds* generatedNodeIdsByCoordinate,
    const util::geo::AnyGeometry<double>& geometry);

OsmRelationMember addGeometryCollection(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    int64_t* nextRelationId, GeneratedNodeIds* generatedNodeIdsByCoordinate,
    const util::geo::DCollection& collection,
    const std::unordered_map<std::string, std::string>& tags,
    const std::string& osmId) {
  OsmRelation relation;
  relation.id = (*nextRelationId)--;

  for (const auto& geometry : collection) {
    relation.members.push_back(addCollectionMember(
        store, nextNodeId, nextWayId, nextRelationId,
        generatedNodeIdsByCoordinate, geometry));
  }

  setRelationTags(&relation, tags, "geometrycollection", osmId);
  store->insertRelation(relation);

  return {"relation", relation.id, ""};
}

OsmRelationMember addCollectionMember(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    int64_t* nextRelationId, GeneratedNodeIds* generatedNodeIdsByCoordinate,
    const util::geo::AnyGeometry<double>& geometry) {
  const std::unordered_map<std::string, std::string> noTags;

  switch (geometry.getType()) {
    case 0: {
      const auto nodeId = addGeneratedNode(
          store, nextNodeId, generatedNodeIdsByCoordinate,
          geometry.getPoint());
      return {"node", nodeId, ""};
    }

    case 1: {
      const auto wayId = addWayFromLine(
          store, nextNodeId, nextWayId, generatedNodeIdsByCoordinate,
          geometry.getLine(), noTags);
      return {"way", wayId, ""};
    }

    case 2:
      return addWayFromPolygon(
          store, nextNodeId, nextWayId, generatedNodeIdsByCoordinate,
          nextRelationId,
          geometry.getPolygon(), noTags, "");

    case 3:
      return addWaysFromMultiLine(
          store, nextNodeId, nextWayId, nextRelationId,
          generatedNodeIdsByCoordinate,
          geometry.getMultiLine(), noTags,"");

    case 4:
      return addWayFromMultiPolygon(
          store, nextNodeId, nextWayId, generatedNodeIdsByCoordinate,
          nextRelationId,
          geometry.getMultiPolygon(), noTags, "");

    case 5:
      return addGeometryCollection(
          store, nextNodeId, nextWayId, nextRelationId,
          generatedNodeIdsByCoordinate,
          geometry.getCollection(), noTags, "");

    case 6:
      return addNodesFromMultiPoint(
          store, nextNodeId, nextRelationId, generatedNodeIdsByCoordinate,
          geometry.getMultiPoint(), noTags, "");

    default:
      throw std::runtime_error(
          "Unsupported geometry inside GEOMETRYCOLLECTION");
  }
}

std::string unescapeSparqlLiteralLexicalForm(const std::string& lexicalForm) {
  std::string result;
  result.reserve(lexicalForm.size());

  for (size_t i = 0; i < lexicalForm.size(); ++i) {
    if (lexicalForm[i] != '\\' || i + 1 == lexicalForm.size()) {
      result += lexicalForm[i];
      continue;
    }

    const auto escapedCharacter = lexicalForm[++i];
    switch (escapedCharacter) {
      case '"':
        result += '"';
        break;
      case '\\':
        result += '\\';
        break;
      case 'n':
        result += '\n';
        break;
      case 'r':
        result += '\r';
        break;
      case 't':
        result += '\t';
        break;
      default:
        result += '\\';
        result += escapedCharacter;
        break;
      }
  }

  return result;
}
}  // namespace

bool OsmPrimitiveStore::insertNode(const OsmNode& node) {
  const auto existing = std::find_if(
      nodes.begin(), nodes.end(),
      [&](const OsmNode& candidate) { return candidate.id == node.id;});

  if (existing == nodes.end()) {
    nodes.push_back(node);
    return true;
  }

  if (existing->lon == node.lon && existing->lat == node.lat &&
    existing->tags == node.tags) {
  return false;
  }

  throw std::runtime_error("Conflicting node definition for OSM ID: " +
                            std::to_string(node.id));
}

bool OsmPrimitiveStore::insertWay(const OsmWay& way) {
  const auto existing = std::find_if(
      ways.begin(), ways.end(),
      [&](const OsmWay& candidate) { return candidate.id == way.id;});

  if (existing == ways.end()) {
    ways.push_back(way);
    return true;
  }

  if (existing->nodeRefs == way.nodeRefs &&
    existing->tags == way.tags) {
    return false;
  }

  throw std::runtime_error("Conflicting way definition for OSM ID: " +
                            std::to_string(way.id));
}

bool OsmPrimitiveStore::insertRelation(const OsmRelation& relation) {
  const auto existing = std::find_if(
      relations.begin(), relations.end(),
      [&](const OsmRelation& candidate) {
        return candidate.id == relation.id;
      });

  if (existing == relations.end()) {
    relations.push_back(relation);
    return true;
  }

  const bool sameMembers =
      existing->members.size() == relation.members.size() &&
      std::equal(existing->members.begin(), existing->members.end(),
                 relation.members.begin(),
                 [](const OsmRelationMember& left,
                    const OsmRelationMember& right) {
                   return left.type == right.type &&
                          left.ref == right.ref &&
                          left.role == right.role;
                 });

  if (sameMembers && existing->tags == relation.tags) {
    return false;
  }

  throw std::runtime_error("Conflicting relation definition for OSM ID: " +
                           std::to_string(relation.id));
}

OsmUnresolvedReferences findUnresolvedOsmReferences(
    const OsmPrimitiveStore& store) {
  std::set<int64_t> presentNodeIds;
  std::set<int64_t> presentWayIds;
  std::set<int64_t> presentRelationIds;

  for (const auto& node : store.nodes) {
    presentNodeIds.insert(node.id);
  }

  for (const auto& way : store.ways) {
    presentWayIds.insert(way.id);
  }

  for (const auto& relation : store.relations) {
    presentRelationIds.insert(relation.id);
  }

  OsmUnresolvedReferences result;

  // Existing ways may reference nodes that have not been fetched yet.
  for (const auto& way : store.ways) {
    for (const auto nodeRef : way.nodeRefs) {
      if (presentNodeIds.count(nodeRef) == 0) {
        result.nodeIds.insert(nodeRef);
      }
    }
  }

  // Existing relations may reference any OSM primitive type.
  for (const auto& relation : store.relations) {
    for (const auto& member : relation.members) {
      if (member.type == "node") {
        if (presentNodeIds.count(member.ref) == 0) {
          result.nodeIds.insert(member.ref);
        }
        continue;
      }

      if (member.type == "way") {
        if (presentWayIds.count(member.ref) == 0) {
          result.wayIds.insert(member.ref);
        }
        continue;
      }

      if (member.type == "relation") {
        if (presentRelationIds.count(member.ref) == 0) {
          result.relationIds.insert(member.ref);
        }
        continue;
      }

      throw std::runtime_error(
          "Unsupported relation member type in primitive store: " +
          member.type);
    }
  }

  return result;
}

namespace {

template <typename Callback>
void forEachOsmIdBatch(
    const std::set<int64_t>& ids,
    size_t maxIdsPerRequest,
    Callback callback) {
  if (maxIdsPerRequest == 0) {
    throw std::invalid_argument(
        "maxIdsPerRequest must be greater than zero.");
  }

  auto iterator = ids.begin();

  while (iterator != ids.end()) {
    std::set<int64_t> batch;

    for (size_t count = 0;
         iterator != ids.end() && count < maxIdsPerRequest;
         ++iterator, ++count) {
      batch.insert(*iterator);
    }

    callback(batch);
  }
}

struct OsmSourceVocabulary {
  std::vector<std::string> nodeIriBases;
  std::string wayIriBase;
  std::string relationIriBase;
};

const OsmSourceVocabulary& osmSourceVocabulary(
    petrimaps::OsmSourceDataset source) {
  static const OsmSourceVocabulary osm = {
      {"http://www.openstreetmap.org/node/",
       "https://www.openstreetmap.org/node/"},
      "https://www.openstreetmap.org/way/",
      "https://www.openstreetmap.org/relation/"};
  static const OsmSourceVocabulary ohm = {
      {"http://www.openhistoricalmap.org/node/",
       "https://www.openhistoricalmap.org/node/"},
      "https://www.openhistoricalmap.org/way/",
      "https://www.openhistoricalmap.org/relation/"};

  return source == petrimaps::OsmSourceDataset::Ohm ? ohm : osm;
}

}  // namespace

// _____________________________________________________________________________
std::string buildQleverWayBatchQuery(
    const std::set<int64_t>& wayIds, OsmSourceDataset source) {
  if (wayIds.empty()) {
    throw std::invalid_argument(
        "Cannot build a QLever way query for an empty ID set.");
  }

  std::ostringstream query;
  const auto& vocabulary = osmSourceVocabulary(source);

  query << "PREFIX osmway: "
        << "<https://www.openstreetmap.org/way/>\n";
  query << "PREFIX osmkey: "
        << "<https://www.openstreetmap.org/wiki/Key:>\n\n";
  query << "PREFIX ql: "
        << "<http://qlever.cs.uni-freiburg.de/builtin-functions/>\n\n";

  query << "SELECT ?way_id ?record_kind ?tag_key ?tag_value "
        << "?node_ref ?node_sequence WHERE {\n";

  for (const auto wayId : wayIds) {
    if (wayId <= 0) {
      throw std::invalid_argument(
          "QLever source way IDs must be positive: " +
          std::to_string(wayId));
    }
  }

  const auto appendWayValues = [&] {
    query << "    VALUES (?way ?way_id) {\n";
    for (const auto wayId : wayIds) {
      query << "      (<" << vocabulary.wayIriBase << wayId << "> "
            << wayId << ")\n";
    }
    query << "    }\n";
  };

  query << "  {\n";
  appendWayValues();
  query << "    ?way osmway:member ?member .\n";
  query << "    ?member osmway:member_id ?node_ref ;\n";
  query << "            osmway:member_pos ?node_sequence .\n";
  query << "    BIND(\"node\" AS ?record_kind)\n";
  query << "  }\n";
  query << "  UNION\n";
  query << "  {\n";
  appendWayValues();
  query << "    ?way ?tag_key ?tag_value .\n";
  query << "    FILTER(ql:prefix-match(STR(?tag_key),\n";
  query << "      \"https://www.openstreetmap.org/wiki/Key:\"))\n";
  query << "    BIND(\"tag\" AS ?record_kind)\n";
  query << "  }\n";
  query << "}\n";
  query << "ORDER BY ?way_id\n";

  return query.str();
}

void fetchQleverWaysIntoStore(
    const std::string& backendUrl,
    const std::set<int64_t>& wayIds,
    OsmPrimitiveStore* store,
    const std::string& remoteAddr,
    size_t maxIdsPerRequest,
    OsmSourceDataset source) {
  if (store == nullptr) {
    throw std::invalid_argument(
        "fetchQleverWaysIntoStore requires an OsmPrimitiveStore.");
  }

  // A resolver may reach a fixed point with no missing ways. That is not an
  // error and must not create an invalid empty VALUES query.
  if (wayIds.empty()) {
    return;
  }

  LOG(INFO) << "[OSM EXPORT] Fetching "
          << wayIds.size()
          << " way definitions";

  size_t batchNumber = 0;

  forEachOsmIdBatch(
      wayIds, maxIdsPerRequest,
      [&](const std::set<int64_t>& batchIds) {
        ++batchNumber;

        LOG(INFO) << "[OSM EXPORT] Fetching way batch "
                  << batchNumber
                  << " with "
                  << batchIds.size()
                  << " definitions";

        OsmWayTsvCollector collector(store);
        RequestReader reader(backendUrl, 0, 0, 0, 0);

        reader.requestRows(
            buildQleverWayBatchQuery(batchIds, source),
            [&collector](const char* data, size_t size) {
              collector.parse(data, size);
            },
            remoteAddr);

        collector.finish();
    });

  LOG(INFO) << "[OSM EXPORT] Finished fetching way definitions";
}

std::string buildQleverNodeBatchQuery(
    const std::set<int64_t>& nodeIds, OsmSourceDataset source) {
  if (nodeIds.empty()) {
    throw std::invalid_argument(
        "Cannot build a QLever node query for an empty ID set.");
  }

  std::ostringstream query;
  const auto& vocabulary = osmSourceVocabulary(source);

  query << "PREFIX osmkey: "
        << "<https://www.openstreetmap.org/wiki/Key:>\n";
  query << "PREFIX ql: "
        << "<http://qlever.cs.uni-freiburg.de/builtin-functions/>\n\n";
  query << "PREFIX geo: "
        << "<http://www.opengis.net/ont/geosparql#>\n\n";

  query << "SELECT ?node_id ?record_kind ?tag_key ?tag_value ?wkt "
        << "WHERE {\n";

  for (const auto nodeId : nodeIds) {
    if (nodeId <= 0) {
      throw std::invalid_argument(
          "QLever source node IDs must be positive: " +
          std::to_string(nodeId));
    }
  }

  const auto appendNodeValues = [&](const std::string& nodeIriBase) {
    query << "    VALUES (?node ?node_id) {\n";
    for (const auto nodeId : nodeIds) {
      query << "      (<" << nodeIriBase << nodeId << "> "
            << nodeId << ")\n";
    }
    query << "    }\n";
  };

  bool needsUnion = false;

  for (const auto& nodeIriBase : vocabulary.nodeIriBases) {
    if (needsUnion) {
      query << "  UNION\n";
    }

    query << "  {\n";
    appendNodeValues(nodeIriBase);
    query << "    ?node geo:hasGeometry / geo:asWKT ?wkt .\n";
    query << "    BIND(\"coordinate\" AS ?record_kind)\n";
    query << "  }\n";
    needsUnion = true;
  }

  for (const auto& nodeIriBase : vocabulary.nodeIriBases) {
    if (needsUnion) {
      query << "  UNION\n";
    }

    query << "  {\n";
    appendNodeValues(nodeIriBase);
    query << "    ?node ?tag_key ?tag_value .\n";
    query << "    FILTER(ql:prefix-match(STR(?tag_key),\n";
    query << "      \"https://www.openstreetmap.org/wiki/Key:\"))\n";
    query << "    BIND(\"tag\" AS ?record_kind)\n";
    query << "  }\n";
    needsUnion = true;
  }

  query << "}\n";
  query << "ORDER BY ?node_id\n";

  return query.str();
}

void fetchQleverNodesIntoStore(
    const std::string& backendUrl,
    const std::set<int64_t>& nodeIds,
    OsmPrimitiveStore* store,
    const std::string& remoteAddr,
    size_t maxIdsPerRequest,
    OsmSourceDataset source) {
  if (store == nullptr) {
    throw std::invalid_argument(
        "fetchQleverNodesIntoStore requires an OsmPrimitiveStore.");
  }

  // No unresolved nodes is a normal resolver fixed point.
  if (nodeIds.empty()) {
    return;
  }

  LOG(INFO) << "[OSM EXPORT] Fetching "
          << nodeIds.size()
          << " node definitions";

  size_t batchNumber = 0;

  forEachOsmIdBatch(
      nodeIds, maxIdsPerRequest,
      [&](const std::set<int64_t>& batchIds) {
        ++batchNumber;

        LOG(INFO) << "[OSM EXPORT] Fetching node batch "
                  << batchNumber
                  << " with "
                  << batchIds.size()
                  << " definitions";

        OsmNodeTsvCollector collector(store);
        RequestReader reader(backendUrl, 0, 0, 0, 0);

        reader.requestRows(
            buildQleverNodeBatchQuery(batchIds, source),
            [&collector](const char* data, size_t size) {
              collector.parse(data, size);
            },
            remoteAddr);

        collector.finish();
      });


  LOG(INFO) << "[OSM EXPORT] Finished fetching node definitions";
}

std::string buildQleverRelationBatchQuery(
    const std::set<int64_t>& relationIds, OsmSourceDataset source) {
  if (relationIds.empty()) {
    throw std::invalid_argument(
        "Cannot build a QLever relation query for an empty ID set.");
  }

  std::ostringstream query;
  const auto& vocabulary = osmSourceVocabulary(source);

  query << "PREFIX osm: "
        << "<https://www.openstreetmap.org/>\n";
  query << "PREFIX osmrel: "
        << "<https://www.openstreetmap.org/relation/>\n";
  query << "PREFIX osmkey: "
        << "<https://www.openstreetmap.org/wiki/Key:>\n";
  query << "PREFIX ql: "
        << "<http://qlever.cs.uni-freiburg.de/builtin-functions/>\n\n";
  query << "PREFIX rdf: "
        << "<http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\n";

  query << "SELECT ?relation_id ?record_kind ?tag_key ?tag_value "
        << "?member_type ?member_ref ?member_role ?member_sequence "
        << "WHERE {\n";

  for (const auto relationId : relationIds) {
    if (relationId <= 0) {
      throw std::invalid_argument(
          "QLever source relation IDs must be positive: " +
          std::to_string(relationId));
    }
  }

  const auto appendRelationValues = [&] {
    query << "    VALUES (?relation ?relation_id) {\n";
    for (const auto relationId : relationIds) {
      query << "      (<" << vocabulary.relationIriBase << relationId
            << "> " << relationId << ")\n";
    }
    query << "    }\n";
  };

  query << "  {\n";
  appendRelationValues();
  query << "    ?relation rdf:type osm:relation .\n";
  query << "    ?relation osmrel:member ?member .\n";
  query << "    ?member osmrel:member_id ?member_ref ;\n";
  query << "            osmrel:member_pos ?member_sequence .\n";
  query << "    OPTIONAL { ?member osmrel:member_role ?member_role . }\n";
  query << "    ?member_ref rdf:type ?member_type .\n";
  query << "    BIND(\"member\" AS ?record_kind)\n";
  query << "  }\n";
  query << "  UNION\n";
  query << "  {\n";
  appendRelationValues();
  query << "    ?relation rdf:type osm:relation .\n";
  query << "    ?relation ?tag_key ?tag_value .\n";
  query << "    FILTER(ql:prefix-match(STR(?tag_key),\n";
  query << "      \"https://www.openstreetmap.org/wiki/Key:\"))\n";
  query << "    BIND(\"tag\" AS ?record_kind)\n";
  query << "  }\n";
  query << "}\n";
  query << "ORDER BY ?relation_id\n";

  return query.str();
}

void fetchQleverRelationsIntoStore(
    const std::string& backendUrl,
    const std::set<int64_t>& relationIds,
    OsmPrimitiveStore* store,
    const std::string& remoteAddr,
    size_t maxIdsPerRequest,
    OsmSourceDataset source) {
  if (store == nullptr) {
    throw std::invalid_argument(
        "fetchQleverRelationsIntoStore requires an OsmPrimitiveStore.");
  }

  // Normal completion of a resolver iteration: no nested relations remain.
  if (relationIds.empty()) {
    return;
  }

  LOG(INFO) << "[OSM EXPORT] Fetching "
          << relationIds.size()
          << " relation definitions";

  size_t batchNumber = 0;

  forEachOsmIdBatch(
      relationIds, maxIdsPerRequest,
      [&](const std::set<int64_t>& batchIds) {
        ++batchNumber;

        LOG(INFO) << "[OSM EXPORT] Fetching relation batch "
                  << batchNumber
                  << " with "
                  << batchIds.size()
                  << " definitions";

        OsmRelationTsvCollector collector(store);
        RequestReader reader(backendUrl, 0, 0, 0, 0);

        reader.requestRows(
            buildQleverRelationBatchQuery(batchIds, source),
            [&collector](const char* data, size_t size) {
              collector.parse(data, size);
            },
            remoteAddr);

        collector.finish();
      });

  LOG(INFO) << "[OSM EXPORT] Finished fetching relation definitions";
}

void resolveQleverOsmReferencesIntoStore(
    const std::string& backendUrl,
    OsmPrimitiveStore* store,
    const std::string& remoteAddr,
    const OsmQleverResolutionLimits& limits,
    OsmSourceDataset source) {
  if (store == nullptr) {
    throw std::invalid_argument(
        "resolveQleverOsmReferencesIntoStore requires an "
        "OsmPrimitiveStore.");
  }

  std::map<int64_t, size_t> relationDepths;

  // Relations already present when resolution begins are traversal roots.
  for (const auto& relation : store->relations) {
    relationDepths.emplace(relation.id, 0);
  }

  const auto discoverRelationDepths =
      [&relationDepths](const OsmPrimitiveStore& currentStore) {
        for (const auto& relation : currentStore.relations) {
          const auto parentIt = relationDepths.find(relation.id);

          if (parentIt == relationDepths.end() ||
              parentIt->second == std::numeric_limits<size_t>::max()) {
            continue;
          }

          const auto childDepth = parentIt->second + 1;

          for (const auto& member : relation.members) {
            if (member.type != "relation") {
              continue;
            }

            const auto childIt = relationDepths.find(member.ref);

            if (childIt == relationDepths.end() ||
                childDepth < childIt->second) {
              relationDepths[member.ref] = childDepth;
            }
          }
        }
  };

  const auto objectCount = [](const OsmPrimitiveStore& currentStore) {
    return currentStore.nodes.size() +
           currentStore.ways.size() +
           currentStore.relations.size();
  };

  const auto describeIds = [](const std::set<int64_t>& ids) {
    std::ostringstream out;
    out << ids.size();

    if (ids.empty()) {
      return out.str();
    }

    out << " [";

    size_t shown = 0;
    for (const auto id : ids) {
      if (shown > 0) {
        out << ", ";
      }

      out << id;
      ++shown;

      if (shown == 10) {
        if (ids.size() > shown) {
          out << ", ...";
        }
        break;
      }
    }

    out << "]";
    return out.str();
  };

  const auto describeUnresolved =
      [&describeIds](const OsmUnresolvedReferences& unresolved) {
        return "nodes=" + describeIds(unresolved.nodeIds) +
               ", ways=" + describeIds(unresolved.wayIds) +
               ", relations=" + describeIds(unresolved.relationIds);
      };

  const auto ensureCapacityFor = [&](
      size_t additionalObjects,
      const char* kind,
      const OsmUnresolvedReferences& unresolved) {
    const auto currentObjects = objectCount(*store);

    if (currentObjects > limits.maxObjects ||
        additionalObjects > limits.maxObjects - currentObjects) {
      throw std::runtime_error(
        "QLever reference resolution would exceed maxObjects=" +
        std::to_string(limits.maxObjects) +
        "; collected objects=" + std::to_string(currentObjects) +
        "; next " + kind + " batch contains " +
        std::to_string(additionalObjects) +
        "; remaining references: " +
        describeUnresolved(unresolved));
    }
  };

  size_t completedRounds = 0;

  while (true) {
    discoverRelationDepths(*store);

    const auto unresolvedBefore =
        findUnresolvedOsmReferences(*store);

    LOG(INFO) << "[OSM EXPORT] Resolution frontier: "
          << unresolvedBefore.relationIds.size() << " relations, "
          << unresolvedBefore.wayIds.size() << " ways, "
          << unresolvedBefore.nodeIds.size() << " nodes";

    if (unresolvedBefore.empty()) {
      return;
    }

    for (const auto relationId : unresolvedBefore.relationIds) {
      const auto depthIt = relationDepths.find(relationId);

      if (depthIt == relationDepths.end()) {
        throw std::runtime_error(
            "Missing relation depth for unresolved relation ID: " +
            std::to_string(relationId));
      }

      if (depthIt->second > limits.maxRelationDepth) {
        throw std::runtime_error(
            "QLever reference resolution exceeded maxRelationDepth=" +
            std::to_string(limits.maxRelationDepth) +
            "; relation ID " + std::to_string(relationId) +
            " requires depth " + std::to_string(depthIt->second));
      }
    }

    if (completedRounds == limits.maxRounds) {
      throw std::runtime_error(
          "QLever reference resolution reached maxRounds=" +
          std::to_string(limits.maxRounds) +
          "; remaining references: " +
          describeUnresolved(unresolvedBefore));
    }

    const auto objectCountBefore = objectCount(*store);

    if (objectCountBefore >= limits.maxObjects) {
      throw std::runtime_error(
          "QLever reference resolution reached maxObjects=" +
          std::to_string(limits.maxObjects) +
          "; collected objects=" +
          std::to_string(objectCountBefore) +
          "; remaining references: " +
          describeUnresolved(unresolvedBefore));
    }

    ++completedRounds;

    ensureCapacityFor(
        unresolvedBefore.relationIds.size(),
        "relation",
        unresolvedBefore);

    fetchQleverRelationsIntoStore(
        backendUrl,
        unresolvedBefore.relationIds,
        store,
        remoteAddr,
        limits.maxIdsPerRequest,
        source);

    const auto unresolvedAfterRelations =
        findUnresolvedOsmReferences(*store);

    ensureCapacityFor(
        unresolvedAfterRelations.wayIds.size(),
        "way",
        unresolvedAfterRelations);

    fetchQleverWaysIntoStore(
        backendUrl,
        unresolvedAfterRelations.wayIds,
        store,
        remoteAddr,
        limits.maxIdsPerRequest,
        source);

    const auto unresolvedAfterWays =
        findUnresolvedOsmReferences(*store);

    ensureCapacityFor(
        unresolvedAfterWays.nodeIds.size(),
        "node",
        unresolvedAfterWays);

    fetchQleverNodesIntoStore(
        backendUrl,
        unresolvedAfterWays.nodeIds,
        store,
        remoteAddr,
        limits.maxIdsPerRequest,
        source);

    const auto unresolvedAfterRound =
        findUnresolvedOsmReferences(*store);

    const auto objectCountAfter = objectCount(*store);

    if (objectCountAfter > limits.maxObjects) {
      throw std::runtime_error(
          "QLever reference resolution exceeded maxObjects=" +
          std::to_string(limits.maxObjects) +
          "; collected objects=" +
          std::to_string(objectCountAfter) +
          "; remaining references: " +
          describeUnresolved(unresolvedAfterRound));
    }

    if (unresolvedAfterRound.empty()) {
      return;
    }

    if (objectCountAfter == objectCountBefore) {
      throw std::runtime_error(
          "QLever reference resolution made no progress after round " +
          std::to_string(completedRounds) +
          "; remaining references: " +
          describeUnresolved(unresolvedAfterRound));
    }
  }
}

// _____________________________________________________________________________
std::string normalizeSparqlResultColumn(std::string column) {
  if (!column.empty() && (column[0] == '?' || column[0] == '$')) {
    column.erase(0, 1);
  }

  return column;
}

// _____________________________________________________________________________
std::string normalizeSparqlLiteralValue(std::string value) {
  if (value.empty() || value.front() != '"') {
    return value;
  }

  for (size_t i = 1; i < value.size(); ++i) {
    if (value[i] != '"' || isEscapedLiteralQuote(value, i)) {
      continue;
    }

    const auto suffix = value.substr(i + 1);
    const bool hasValidSuffix =
        suffix.empty() || suffix.front() == '@' || suffix.rfind("^^", 0) == 0;

    if (!hasValidSuffix) {
      return value;
    }

    return unescapeSparqlLiteralLexicalForm(value.substr(1, i - 1));
  }

  return value;
}

std::string normalizeSparqlIri(std::string value) {
  if (value.size() >= 2 && value.front() == '<' && value.back() == '>') {
    return value.substr(1, value.size() - 2);
  }

  return value;
}

bool hasForbiddenOsmTagCharacter(const std::string& value) {
  for (const unsigned char character : value) {
    if (character <= 0x1F || character == 0x7F) {
      return true;
    }
  }

  return false;
}

void validateOsmTags(
    const std::unordered_map<std::string, std::string>& tags,
    const std::string& osmId) {
  for (const auto& tag : tags) {
    const auto& key = tag.first;
    const auto& value = tag.second;
    if (key.empty()) {
      throw std::runtime_error(
          "OSM export contains an empty tag key for osm_id: " + osmId);
    }

    if (hasForbiddenOsmTagCharacter(key)) {
      throw std::runtime_error(
          "OSM export contains a forbidden control character in tag key "
          "for osm_id: " + osmId);
    }

    if (hasForbiddenOsmTagCharacter(value)) {
      throw std::runtime_error(
          "OSM export contains a forbidden control character in tag value "
          "for osm_id: " + osmId + ", key: " + key);
    }
  }
}

std::map<std::string, std::string> sortedTags(
    const std::unordered_map<std::string, std::string>& tags) {
  return {tags.begin(), tags.end()};
}

std::string joinOsmTagValues(const std::set<std::string>& values) {
  std::ostringstream out;
  bool isFirst = true;

  for (const auto& value : values) {
    if (!isFirst) {
      out << ';';
    }
    out << value;
    isFirst = false;
  }

  return out.str();
}

// _____________________________________________________________________________
std::string normalizeOsmTagKey(std::string key) {
  if (key.size() >= 2 && key.front() == '<' && key.back() == '>') {
    key = key.substr(1, key.size() - 2);
  }

  const std::string osmKeyPrefix = "osmkey:";
  if (key.rfind(osmKeyPrefix, 0) == 0) {
    return key.substr(osmKeyPrefix.size());
  }

  const std::string osmKeyHttpUri = "http://www.openstreetmap.org/wiki/Key:";
  if (key.rfind(osmKeyHttpUri, 0) == 0) {
    return key.substr(osmKeyHttpUri.size());
  }

  const std::string osmKeyHttpsUri = "https://www.openstreetmap.org/wiki/Key:";
  if (key.rfind(osmKeyHttpsUri, 0) == 0) {
    return key.substr(osmKeyHttpsUri.size());
  }

  return key;
}
// _____________________________________________________________________________
std::string inferOsmObjectType(const std::string& id) {
  const auto value = normalizeSparqlIri(id);

  const auto hasSuffix =
      [](const std::string& text, const std::string& suffix) {
        return text.size() >= suffix.size() &&
               text.compare(text.size() - suffix.size(),
                            suffix.size(), suffix) == 0;
      };

  if (value == "node" ||
      value == "osm:node" ||
      value.rfind("osmnode:", 0) == 0 ||
      value.find("/node/") != std::string::npos ||
      hasSuffix(value, "/node")) {
    return "node";
  }

  if (value == "way" ||
      value == "osm:way" ||
      value.rfind("osmway:", 0) == 0 ||
      value.find("/way/") != std::string::npos ||
      hasSuffix(value, "/way")) {
    return "way";
  }

  if (value == "relation" ||
      value == "osm:relation" ||
      value.rfind("osmrel:", 0) == 0 ||
      value.rfind("osmrelation:", 0) == 0 ||
      value.find("/relation/") != std::string::npos ||
      hasSuffix(value, "/relation")) {
    return "relation";
  }

  return "unknown";
}
// _____________________________________________________________________________
std::vector<OsmObject> osmObjectsFromTsvRows(
    const std::vector<std::vector<std::pair<std::string, std::string>>>& rows) {
  std::vector<OsmObject> objects;
  OsmObject current;
  bool hasCurrent = false;

  for (const auto& row : rows) {
    const auto osmId = getRequiredCell(row, "osm_id");
    const auto tagKey = normalizeOsmTagKey(getRequiredCell(row, "a"));
    const auto tagValue =
        normalizeSparqlLiteralValue(getRequiredCell(row, "b"));
    const auto wkt = getRequiredCell(row, "hasgeometry");

    if (!hasCurrent || current.id != osmId) {
      if (hasCurrent) {
        objects.push_back(current);
      }

      current = {};
      current.id = osmId;
      current.type = inferOsmObjectType(osmId);
      current.wkt = wkt;
      hasCurrent = true;
    } else if (current.wkt != wkt) {
      throw std::runtime_error("Conflicting WKT for osm_id: " + osmId);
    }

    auto it = current.tags.find(tagKey);
    if (it != current.tags.end() && it->second != tagValue) {
      throw std::runtime_error("Conflicting tag value for osm_id: " + osmId +
                               ", key: " + tagKey);
    }

    current.tags[tagKey] = tagValue;
  }

  if (hasCurrent) {
    objects.push_back(current);
  }
  return objects;
}

TsvStreamReader::TsvStreamReader(HeaderCallback onHeader, RowCallback onRow)
    : _onHeader(std::move(onHeader)), _onRow(std::move(onRow)) {}

void TsvStreamReader::parse(const char* data, size_t size) {
  const char* start = data;

  while (data < start + size) {
    switch (_state) {
      case IN_HEADER:
        if (*data == '\t' || *data == '\n') {
          finishCell();
        }

        if (*data == '\n') {
          finishRow();
          _state = IN_ROW;
          ++data;
          continue;
        }

        if (*data != '\t') {
          _dangling += *data;
        }

        ++data;
        continue;

      case IN_ROW:
        if (*data == '\t' || *data == '\n') {
          finishCell();

          if (*data == '\n') {
            finishRow();
          }

          ++data;
          continue;
        }

        _dangling += *data;
        ++data;
        break;
    }
  }
}

void TsvStreamReader::finish() {
  if (!_dangling.empty()) {
    finishCell();
  }

  if (_state == IN_HEADER) {
    if (_header.empty()) {
      throw std::runtime_error(
          "QLever returned an empty response; expected a TSV header.");
    }

    finishRow();
    _state = IN_ROW;
  }

  if (!_currentRow.empty()) {
    finishRow();
  }
}

void TsvStreamReader::finishCell() {
  if (_state == IN_HEADER) {
    _header.push_back(_dangling);
  } else {
    _currentRow.push_back(_dangling);
  }

  _dangling.clear();
}

void TsvStreamReader::finishRow() {
  if (_state == IN_HEADER) {
    if (_headerFinished) {
      throw std::runtime_error("TSV header was completed more than once.");
    }

    _onHeader(_header);
    _headerFinished = true;
    return;
  }

  if (_currentRow.empty()) {
    return;
  }

  _onRow(_currentRow);
  _currentRow.clear();
}

int64_t parseOsmId(std::string value,
                       const std::string& column) {
  value = normalizeSparqlIri(std::move(value));

  const std::vector<std::string> osmIriPrefixes = {
      "http://www.openstreetmap.org/node/",
      "https://www.openstreetmap.org/node/",
      "http://www.openstreetmap.org/way/",
      "https://www.openstreetmap.org/way/",
      "http://www.openstreetmap.org/relation/",
      "https://www.openstreetmap.org/relation/",
      "http://www.openhistoricalmap.org/node/",
      "https://www.openhistoricalmap.org/node/",
      "http://www.openhistoricalmap.org/way/",
      "https://www.openhistoricalmap.org/way/",
      "http://www.openhistoricalmap.org/relation/",
      "https://www.openhistoricalmap.org/relation/"};

  for (const auto& prefix : osmIriPrefixes) {
    if (value.rfind(prefix, 0) == 0) {
      value = value.substr(prefix.size());
      break;
    }
  }

  try {
    size_t parsedLength = 0;
    const auto result = std::stoll(value, &parsedLength);

    if (parsedLength == value.size()) {
      return result;
    }
  } catch (const std::exception&) {
  }

  throw std::runtime_error(
      "Invalid OSM ID in TSV column " + column + ": " + value);
}

uint64_t parseMemberSequence(const std::string& value) {
  if (value.empty() || value.front() == '-') {
    throw std::runtime_error(
        "Invalid member_sequence in relation TSV: " + value);
  }

  try {
    size_t parsedLength = 0;
    const auto result = std::stoull(value, &parsedLength);

    if (parsedLength == value.size()) {
      return result;
    }
  } catch (const std::exception&) {
  }

  throw std::runtime_error(
      "Invalid member_sequence in relation TSV: " + value);
}

double parseCoordinate(const std::string& value,
                       const std::string& column) {
  try {
    size_t parsedLength = 0;
    const auto result = std::stod(value, &parsedLength);

    if (parsedLength == value.size() && std::isfinite(result)) {
      return result;
    }
  } catch (const std::exception&) {
  }

  throw std::runtime_error(
        "Invalid coordinate in node TSV column " + column + ": " + value);
}

std::pair<double, double> parseQleverPointWkt(
    std::string value) {
  value = normalizeSparqlLiteralValue(std::move(value));

  const std::string prefix = "POINT(";

  if (value.rfind(prefix, 0) != 0 ||
      value.size() <= prefix.size() ||
      value.back() != ')') {
    throw std::runtime_error(
        "Invalid Qlever node WKT: " + value);
  }

  std::istringstream coordinates(
      value.substr(prefix.size(), value.size() - prefix.size() - 1));

  double lon = 0.0;
  double lat = 0.0;
  std::string trailing;

  if (!(coordinates >> lon >> lat) ||
      (coordinates >> trailing) ||
      !std::isfinite(lon)) {
    throw std::runtime_error(
        "Invalid Qlever node WKT: " + value);
  }

  return {lon, lat};
}

OsmWayResultReader::OsmWayResultReader(WayCallback cb)
    : _cb(std::move(cb)),
      _tsvReader(
          [this](const std::vector<std::string>& header) {
            _colNames = header;
            validateHeader();
          },
          [this](const std::vector<std::string>& row) {
            mergeRowIntoCurrentWay(row);
          }) {}

void OsmWayResultReader::parse(const char* data, size_t size) {
  _tsvReader.parse(data, size);
}

void OsmWayResultReader::finish() {
  _tsvReader.finish();
  emitCurrentWay();
}

void OsmWayResultReader::validateHeader() const {
  for (const auto& required :
       {"way_id", "record_kind", "tag_key", "tag_value",
        "node_ref", "node_sequence"}) {
    bool found = false;

    for (const auto& column : _colNames) {
      if (normalizeSparqlResultColumn(column) == required) {
        found = true;
        break;
      }
    }

    if (!found) {
      throw std::runtime_error(
          "Invalid way TSV header: missing required column " +
          std::string(required));
    }
  }
}

void OsmWayResultReader::startWay(int64_t wayId) {
  if (_hasCurrent) {
    emitCurrentWay();
  }

  if (_hasFinishedWay && wayId <= _lastFinishedId) {
    throw std::runtime_error(
        "Way TSV rows are not strictly ordered by way_id.");
  }

  _current = {};
  _current.id = wayId;
  _currentTagValues.clear();
  _currentNodeRefs.clear();
  _hasCurrent = true;
}

void OsmWayResultReader::mergeRowIntoCurrentWay(
    const std::vector<std::string>& row) {
  const auto wayId =
      parseOsmId(getRequiredCell(_colNames, row, "way_id"), "way_id");

  if (!_hasCurrent || _current.id != wayId) {
    startWay(wayId);
  }

  const auto recordKind = normalizeSparqlLiteralValue(
      getRequiredCell(_colNames, row, "record_kind"));

  if (recordKind == "tag") {
    const auto key = normalizeOsmTagKey(
        getRequiredCell(_colNames, row, "tag_key"));
    const auto value = normalizeSparqlLiteralValue(
        getRequiredCell(_colNames, row, "tag_value"));

    if (key.empty()) {
      throw std::runtime_error("Way tag row has an empty tag_key.");
    }

    _currentTagValues[key].insert(value);
    return;
  }

  if (recordKind == "node") {
    const auto nodeRef =
        parseOsmId(getRequiredCell(_colNames, row, "node_ref"),
                   "node_ref");
    const auto sequence = parseMemberSequence(
        getRequiredCell(_colNames, row, "node_sequence"));

    _currentNodeRefs.push_back({sequence, nodeRef});
    return;
  }

  throw std::runtime_error(
      "Unsupported way TSV record_kind: " + recordKind);
}

void OsmWayResultReader::emitCurrentWay() {
  if (!_hasCurrent) {
    return;
  }

  std::sort(
      _currentNodeRefs.begin(), _currentNodeRefs.end(),
      [](const std::pair<uint64_t, int64_t>& left,
         const std::pair<uint64_t, int64_t>& right) {
        return left.first < right.first;
      });

  _current.nodeRefs.clear();

  uint64_t previousSequence = 0;
  bool hasPreviousSequence = false;

  for (const auto& entry : _currentNodeRefs) {
    if (hasPreviousSequence && entry.first == previousSequence) {
      throw std::runtime_error(
          "Duplicate node_sequence for way ID: " +
          std::to_string(_current.id));
    }

    // Do not deduplicate node refs: repeated nodes are valid OSM way data.
    _current.nodeRefs.push_back(entry.second);
    previousSequence = entry.first;
    hasPreviousSequence = true;
  }

  for (const auto& entry : _currentTagValues) {
  _current.tags.emplace(entry.first, joinOsmTagValues(entry.second));
  }

  _cb(_current);
  _lastFinishedId = _current.id;
  _hasFinishedWay = true;
  _current = {};
  _currentNodeRefs.clear();
  _hasCurrent = false;
}

OsmWayTsvCollector::OsmWayTsvCollector(OsmPrimitiveStore* store)
    : _store(store),
      _reader([this](const OsmWay& way) {
        _store->insertWay(way);
      }) {
  if (_store == nullptr) {
    throw std::runtime_error(
        "OsmWayTsvCollector requires an OsmPrimitiveStore");
  }
}

void OsmWayTsvCollector::parse(const char* data, size_t size) {
  if (_finished) {
    throw std::runtime_error(
        "Cannot parse into a finished way TSV collector");
  }

  _reader.parse(data, size);
}

void OsmWayTsvCollector::finish() {
  if (_finished) {
    throw std::runtime_error(
        "Way TSV collector was already finished");
  }

  _reader.finish();
  _finished = true;
}

OsmNodeResultReader::OsmNodeResultReader(NodeCallback cb)
    : _cb(std::move(cb)),
      _tsvReader(
          [this](const std::vector<std::string>& header) {
            _colNames = header;
            validateHeader();
          },
          [this](const std::vector<std::string>& row) {
            mergeRowIntoCurrentNode(row);
          }) {}

void OsmNodeResultReader::parse(const char* data, size_t size) {
  _tsvReader.parse(data, size);
}

void OsmNodeResultReader::finish() {
  _tsvReader.finish();
  emitCurrentNode();
}

void OsmNodeResultReader::validateHeader() {

  bool hasNodeId = false;
  bool hasRecordKind = false;
  bool hasTagKey = false;
  bool hasTagValue = false;
  bool hasLon = false;
  bool hasLat = false;
  bool hasWkt = false;

  for (const auto& column : _colNames) {
    const auto name = normalizeSparqlResultColumn(column);

    hasNodeId = hasNodeId || name == "node_id";
    hasRecordKind = hasRecordKind || name == "record_kind";
    hasTagKey = hasTagKey || name == "tag_key";
    hasTagValue = hasTagValue || name == "tag_value";
    hasLon = hasLon || name == "lon";
    hasLat = hasLat || name == "lat";
    hasWkt = hasWkt || name == "wkt";
  }

  if (!hasNodeId || !hasRecordKind || !hasTagKey || !hasTagValue) {
    throw std::runtime_error(
        "Invalid node TSV header: missing node ID, record kind, "
        "or tag columns.");
  }

  if (hasWkt && (hasLon || hasLat)) {
    throw std::runtime_error(
        "Invalid node TSV header: wkt cannot be combined with lon or lat.");
  }

  if (!hasWkt && !(hasLon && hasLat)) {
    throw std::runtime_error(
        "Invalid node TSV header: expected either wkt or lon and lat.");
  }

  _usesWktCoordinates = hasWkt;
}

void OsmNodeResultReader::startNode(int64_t nodeId) {
  if (_hasCurrent) {
    emitCurrentNode();
  }

  if (_hasFinishedNode && nodeId <= _lastFinishedId) {
    throw std::runtime_error(
        "Node TSV rows are not strictly ordered by node_id.");
  }

  _current = {};
  _current.id = nodeId;
  _currentTagValues.clear();
  _hasCurrent = true;
  _hasCoordinates = false;
}

void OsmNodeResultReader::mergeRowIntoCurrentNode(
    const std::vector<std::string>& row) {
  const auto nodeId =
      parseOsmId(getRequiredCell(_colNames, row, "node_id"), "node_id");

  if (!_hasCurrent || _current.id != nodeId) {
    startNode(nodeId);
  }

  const auto recordKind = normalizeSparqlLiteralValue(
      getRequiredCell(_colNames, row, "record_kind"));

  if (recordKind == "coordinate") {
    double lon = 0.0;
    double lat = 0.0;

    if (_usesWktCoordinates) {
      const auto point = parseQleverPointWkt(
          getRequiredCell(_colNames, row, "wkt"));
      lon = point.first;
      lat = point.second;
    } else {
      lon = parseCoordinate(
          getRequiredCell(_colNames, row, "lon"), "lon");
      lat = parseCoordinate(
          getRequiredCell(_colNames, row, "lat"), "lat");
    }

    if (_hasCoordinates &&
        (_current.lon != lon || _current.lat != lat)) {
      throw std::runtime_error(
          "Conflicting coordinates for node ID: " +
          std::to_string(nodeId));
    }

    _current.lon = lon;
    _current.lat = lat;
    _hasCoordinates = true;
    return;
  }

  if (recordKind == "tag") {
    const auto key = normalizeOsmTagKey(
        getRequiredCell(_colNames, row, "tag_key"));
    const auto value = normalizeSparqlLiteralValue(
        getRequiredCell(_colNames, row, "tag_value"));

    if (key.empty()) {
      throw std::runtime_error("Node tag row has an empty tag_key.");
    }

    _currentTagValues[key].insert(value);
    return;
  }

  throw std::runtime_error(
      "Unsupported node TSV record_kind: " + recordKind);
}

void OsmNodeResultReader::emitCurrentNode() {
  if (!_hasCurrent) {
    return;
  }

  if (!_hasCoordinates) {
    throw std::runtime_error(
        "Node TSV has no coordinate row for node ID: " +
        std::to_string(_current.id));
  }

  for (const auto& entry : _currentTagValues) {
  _current.tags.emplace(entry.first, joinOsmTagValues(entry.second));
  }

  _cb(_current);
  _lastFinishedId = _current.id;
  _hasFinishedNode = true;
  _current = {};
  _hasCurrent = false;
  _hasCoordinates = false;
}

OsmNodeTsvCollector::OsmNodeTsvCollector(OsmPrimitiveStore* store)
    : _store(store),
      _reader([this](const OsmNode& node) {
        _store->insertNode(node);
      }) {
  if (_store == nullptr) {
    throw std::runtime_error(
        "OsmNodeTsvCollector requires an OsmPrimitiveStore");
  }
}

void OsmNodeTsvCollector::parse(const char* data, size_t size) {
  if (_finished) {
    throw std::runtime_error(
        "Cannot parse into a finished node TSV collector");
  }

  _reader.parse(data, size);
}

void OsmNodeTsvCollector::finish() {
  if (_finished) {
    throw std::runtime_error(
        "Node TSV collector was already finished");
  }

  _reader.finish();
  _finished = true;
}

OsmRelationResultReader::OsmRelationResultReader(RelationCallback cb)
    : _cb(std::move(cb)),
      _tsvReader(
          [this](const std::vector<std::string>& header) {
            _colNames = header;
            validateHeader();
          },
          [this](const std::vector<std::string>& row) {
            mergeRowIntoCurrentRelation(row);
          }) {}

void OsmRelationResultReader::parse(const char* data, size_t size) {
  _tsvReader.parse(data, size);
}

void OsmRelationResultReader::finish() {
  _tsvReader.finish();
  emitCurrentRelation();
}

void OsmRelationResultReader::validateHeader() const {
  for (const auto& required :
        {"relation_id", "record_kind", "tag_key", "tag_value",
         "member_type", "member_ref", "member_role", "member_sequence"}) {
    bool found = false;

    for (const auto& column : _colNames) {
      if (normalizeSparqlResultColumn(column) == required) {
        found = true;
        break;
      }
    }

    if (!found) {
      throw std::runtime_error(
          "Invalid relation TSV header: missing required column " +
          std::string(required));
    }
  }
}

void OsmRelationResultReader::startRelation(int64_t relationId) {
  if (_hasCurrent) {
    emitCurrentRelation();
  }

  if (_hasFinishedRelation && relationId <= _lastFinishedId) {
    throw std::runtime_error(
        "Relation TSV rows are not strictly ordered by relation_id.");
  }

  _current = {};
  _current.id = relationId;
  _currentTagValues.clear();
  _currentMembers.clear();
  _hasCurrent = true;
}

void OsmRelationResultReader::mergeRowIntoCurrentRelation(
    const std::vector<std::string>& row) {
  const auto relationId =
      parseOsmId(getRequiredCell(_colNames, row, "relation_id"),
                      "relation_id");
  if (!_hasCurrent || _current.id != relationId) {
    startRelation(relationId);
  }

  const auto recordKind = normalizeSparqlLiteralValue(
      getRequiredCell(_colNames, row, "record_kind"));

  if (recordKind == "tag") {
    const auto key = normalizeOsmTagKey(
        getRequiredCell(_colNames, row, "tag_key"));
    const auto value = normalizeSparqlLiteralValue(
        getRequiredCell(_colNames, row, "tag_value"));

    if (key.empty()) {
      throw std::runtime_error(
          "Relation tag row has an empty tag_key.");
    }

    _currentTagValues[key].insert(value);
    return;
  }

  if (recordKind == "member") {
    const auto rawType =
        getRequiredCell(_colNames, row, "member_type");

    const auto type = inferOsmObjectType(rawType);

    if (type == "unknown") {
      throw std::runtime_error(
          "Unsupported relation member type: " + rawType);
    }

    const auto ref = parseOsmId(
        getRequiredCell(_colNames, row, "member_ref"),
        "member_ref");
    const auto role = normalizeSparqlLiteralValue(
        getRequiredCell(_colNames, row, "member_role"));
    const auto sequence = parseMemberSequence(
        getRequiredCell(_colNames, row, "member_sequence"));

    _currentMembers.push_back({sequence, {type, ref, role}});
    return;
  }

  throw std::runtime_error(
        "Unsupported relation TSV record_kind: " + recordKind);
}

void OsmRelationResultReader::emitCurrentRelation() {
  if (!_hasCurrent) {
    return;
  }

  std::sort(
      _currentMembers.begin(), _currentMembers.end(),
      [](const std::pair<uint64_t, OsmRelationMember>& left,
         const std::pair<uint64_t, OsmRelationMember>& right) {
        return left.first < right.first;
      });

  _current.members.clear();

  uint64_t previousSequence = 0;
  bool hasPreviousSequence = false;

  for (const auto& entry : _currentMembers) {
    if (hasPreviousSequence && entry.first == previousSequence) {
      throw std::runtime_error(
          "Duplicate member_sequence for relation ID: " +
          std::to_string(_current.id));
    }

    _current.members.push_back(entry.second);
    previousSequence = entry.first;
    hasPreviousSequence = true;
  }

  for (const auto& entry : _currentTagValues) {
  _current.tags.emplace(entry.first, joinOsmTagValues(entry.second));
  }

  _cb(_current);
  _lastFinishedId = _current.id;
  _hasFinishedRelation = true;
  _current = {};
  _currentMembers.clear();
  _hasCurrent = false;
}

OsmRelationTsvCollector::OsmRelationTsvCollector(
    OsmPrimitiveStore* store)
    : _store(store),
      _reader([this](const OsmRelation& relation) {
         _store->insertRelation(relation);
      }) {
  if (_store == nullptr) {
    throw std::runtime_error(
        "OsmRelationTsvCollector requires an OsmPrimitiveStore");
  }
}

void OsmRelationTsvCollector::parse(const char* data, size_t size) {
  if (_finished) {
    throw std::runtime_error(
        "Cannot parse into a finished relation TSV collector");
  }

  _reader.parse(data, size);
}

void OsmRelationTsvCollector::finish() {
  if (_finished) {
    throw std::runtime_error(
        "Relation TSV collector was already finished");
  }

  _reader.finish();
  _finished = true;
}

OsmResultReader::OsmResultReader(ObjectCallback cb)
    : _cb(std::move(cb)),
      _tsvReader(
          [this](const std::vector<std::string>& header) {
            _colNames = header;
            validateHeader();
          },
          [this](const std::vector<std::string>& row) {
            mergeRowIntoCurrentObject(row);
          }) {}

void OsmResultReader::parse(const char* data, size_t size) {
  _tsvReader.parse(data, size);
}

void OsmResultReader::finish() {
  _tsvReader.finish();

  if (_hasCurrent) {
    _cb(_current);
    _hasCurrent = false;
  }
}

void OsmResultReader::validateHeader() const {
  for (const auto& required :
       {"osm_id", "a", "b", "hasgeometry"}) {
    bool found = false;

    for (const auto& column : _colNames) {
      if (normalizeSparqlResultColumn(column) == required) {
        found = true;
        break;
      }
    }

    if (!found) {
      throw std::runtime_error(
        "Invalid OSM export TSV header: missing required column " +
        std::string(required));
    }
  }
}

void OsmResultReader::startObject(const std::string& osmId,
                                  const std::string& wkt) {
  if (_hasCurrent) {
    _cb(_current);
    _lastFinishedId = _current.id;
  }

  if (!_lastFinishedId.empty() && osmId <= _lastFinishedId) {
    throw std::runtime_error(
        "OSM export rows are not strictly ordered by osm_id."
        "Add ORDER BY ?osm_id to the export query.");
  }

  _current = {};
  _current.id = osmId;
  _current.type = inferOsmObjectType(osmId);
  _current.wkt = wkt;
  _hasCurrent = true;
}

void OsmResultReader::mergeRowIntoCurrentObject(
    const std::vector<std::string>& row) {
  const auto osmId = getRequiredCell(_colNames, row, "osm_id");
  const auto tagKey =
      normalizeOsmTagKey(getRequiredCell(_colNames, row, "a"));
  const auto tagValue =
      normalizeSparqlLiteralValue(getRequiredCell(_colNames, row, "b"));
  const auto wkt = getRequiredCell(_colNames, row, "hasgeometry");

  if (!_hasCurrent || _current.id != osmId) {
    startObject(osmId, wkt);
  } else if (_current.wkt != wkt) {
    throw std::runtime_error("Conflicting WKT for osm_id: " + osmId);
  }

  auto it = _current.tags.find(tagKey);
  if (it != _current.tags.end() && it->second != tagValue) {
    throw std::runtime_error("Conflicting tag value for osm_id: " + osmId +
                             ", key: " + tagKey);
  }

  _current.tags[tagKey] = tagValue;
}

void OsmPrimitiveBuilder::append(const OsmObject& object,
                                 OsmPrimitiveStore* store) {
  auto tags = object.tags;
  tags["qlever:source_uri"] = normalizeSparqlIri(object.id);
  tags["qlever:source_type"] = object.type;

  validateOsmTags(tags, object.id);

  const char* wktStart = nullptr;
  const auto crsType = util::geo::getCRSType(object.wkt.c_str(), &wktStart);
  const auto wktType = util::geo::getWKTType(wktStart, &wktStart);

  if (crsType == util::geo::CRSType::UNSUPPORTED) {
    throw std::runtime_error("Unsupported CRS for osm_id: " + object.id);
  }

  if (wktType == util::geo::WKTType::POINT) {
    const auto point = util::geo::pointFromWKT<double>(wktStart, nullptr);
    store->insertNode(
        {_nextNodeId--, point.getX(), point.getY(), tags});
  } else if (wktType == util::geo::WKTType::MULTIPOINT) {
    const auto multiPoint =
        util::geo::multiPointFromWKT<double>(wktStart, nullptr);
    addNodesFromMultiPoint(store, &_nextNodeId, &_nextRelationId,
                           &_generatedNodeIdsByCoordinate, multiPoint, tags,
                           object.id);
  } else if (wktType == util::geo::WKTType::LINESTRING) {
    const auto line = util::geo::lineFromWKT<double>(wktStart, nullptr);
    addWayFromLine(store, &_nextNodeId, &_nextWayId,
                   &_generatedNodeIdsByCoordinate, line, tags);
  } else if (wktType == util::geo::WKTType::MULTILINESTRING) {
    const auto multiLine =
        util::geo::multiLineFromWKT<double>(wktStart, nullptr);
    addWaysFromMultiLine(store, &_nextNodeId, &_nextWayId,
                         &_nextRelationId, &_generatedNodeIdsByCoordinate,
                         multiLine, tags, object.id);
  } else if (wktType == util::geo::WKTType::POLYGON) {
    const auto polygon =
        util::geo::polygonFromWKT<double>(wktStart, nullptr);
    addWayFromPolygon(store, &_nextNodeId, &_nextWayId,
                      &_generatedNodeIdsByCoordinate, &_nextRelationId,
                      polygon, tags, object.id);
  } else if (wktType == util::geo::WKTType::MULTIPOLYGON) {
    const auto multiPolygon =
        util::geo::multiPolygonFromWKT<double>(wktStart, nullptr);
    addWayFromMultiPolygon(
        store, &_nextNodeId, &_nextWayId, &_generatedNodeIdsByCoordinate,
        &_nextRelationId, multiPolygon, tags, object.id);
  } else if (wktType == util::geo::WKTType::COLLECTION) {
    const auto collection =
        util::geo::collectionFromWKT<double>(wktStart, nullptr);
    addGeometryCollection(
        store, &_nextNodeId, &_nextWayId, &_nextRelationId,
        &_generatedNodeIdsByCoordinate, collection, tags, object.id);
  } else {
    throw std::runtime_error("Unsupported WKT type for osm_id: " + object.id);
  }
}

// _____________________________________________________________________________
OsmPrimitiveStore osmPrimitivesFromOsmObjects(
    const std::vector<OsmObject>& objects) {
  OsmPrimitiveStore store;
  OsmPrimitiveBuilder builder;

  for (const auto& object : objects) {
    builder.append(object, &store);
  }

  return store;
}

OsmXmlStreamWriter::OsmXmlStreamWriter(std::ostream& out)
    : _out(out), 
    _xml(new util::xml::XmlWriter(&out, true, 2)) {
  _out << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

  _xml->openTag("osm", {
      {"version", "0.6"},
      {"generator", "qlever-petrimaps"},
      {"upload", "never"},
  });
}

OsmXmlStreamWriter::~OsmXmlStreamWriter() = default;

void OsmXmlStreamWriter::writeNode(const OsmNode& node) {
  _xml->openTag("node", {
      {"id", intToXmlAttr(node.id)},
      {"lat", doubleToXmlAttr(node.lat)},
      {"lon", doubleToXmlAttr(node.lon)},
  });

  for (const auto& tag : sortedTags(node.tags)) {
    _xml->openTag("tag", {{"k", tag.first}, {"v", tag.second}});
    _xml->closeTag();
  }

  _xml->closeTag();
}

void OsmXmlStreamWriter::writeWay(const OsmWay& way) {
  _xml->openTag("way", {{"id", intToXmlAttr(way.id)}});

  for (const auto& nodeRef : way.nodeRefs) {
    _xml->openTag("nd", {{"ref", intToXmlAttr(nodeRef)}});
    _xml->closeTag();
  }

  for (const auto& tag : sortedTags(way.tags)) {
    _xml->openTag("tag", {{"k", tag.first}, {"v", tag.second}});
    _xml->closeTag();
  }

  _xml->closeTag();
}

void OsmXmlStreamWriter::writeRelation(const OsmRelation& relation) {
  _xml->openTag("relation", {{"id", intToXmlAttr(relation.id)}});

  for (const auto& member : relation.members) {
    _xml->openTag("member",
                  {{"type", member.type},
                   {"ref", intToXmlAttr(member.ref)},
                   {"role", member.role}});
    _xml->closeTag();
  }

  for (const auto& tag : sortedTags(relation.tags)) {
    _xml->openTag("tag", {{"k", tag.first}, {"v", tag.second}});
    _xml->closeTag();
  }

  _xml->closeTag();
}

void OsmXmlStreamWriter::write(const OsmPrimitiveStore& store) {
  if (_finished) {
    throw std::runtime_error("Cannot write to a finished OSM XML document");
  }

  for (const auto& node : store.nodes) {
    writeNode(node);
  }

  for (const auto& way : store.ways) {
    writeWay(way);
  }

  for (const auto& relation : store.relations) {
    writeRelation(relation);
  }
}

void OsmXmlStreamWriter::finish() {
  if (_finished) {
    throw std::runtime_error("OSM XML document was already finished");
  }

  _xml->closeTag();
  _out << "\n";

  if (!_out) {
    throw std::runtime_error("Failed to write OSM XML output");
  }

  _finished = true;
}

// _____________________________________________________________________________
void exportQleverTsvToOsmXml(const std::string& backendUrl,
                             const std::string& query,
                             std::ostream& out,
                             const std::string& remoteAddr) {
  OsmTsvToXmlExporter exporter(out);

  RequestReader reader(backendUrl, 0, 0, 0, 0);

  LOG(INFO) << "[OSM EXPORT] Fetching root relation IDs";

  reader.requestRows(
      query,
      [&exporter](const char* data, size_t size) {
        exporter.parse(data, size);
      },
      remoteAddr);

  LOG(INFO) << "[OSM EXPORT] Finished fetching root relation IDs";

  exporter.finish();
}

std::set<int64_t> readRootRelationIdsFromQleverQuery(
    const std::string& backendUrl,
    const std::string& query,
    const std::string& remoteAddr) {
  std::set<int64_t> relationIds;
  std::vector<std::string> columns;
  size_t relationIdColumn = 0;
  bool hasHeader = false;

  TsvStreamReader reader(
      [&columns, &relationIdColumn, &hasHeader](
          const std::vector<std::string>& header) {
        columns = header;

        for (size_t i = 0; i < columns.size(); ++i) {
          if (normalizeSparqlResultColumn(columns[i]) == "relation_id") {
            relationIdColumn = i;
            hasHeader = true;
            return;
          }
        }

        throw std::runtime_error(
            "Relation-graph query result has no relation_id column.");
      },
      [&relationIds, &relationIdColumn, &hasHeader](
          const std::vector<std::string>& row) {
        if (!hasHeader || relationIdColumn >= row.size()) {
          throw std::runtime_error(
              "Relation-graph TSV row has no relation_id value.");
        }

        const auto relationId =
            parseOsmId(row[relationIdColumn], "relation_id");

        if (relationId <= 0) {
          throw std::runtime_error(
              "Relation-graph query returned a non-positive relation ID.");
        }

        relationIds.insert(relationId);
      });

  RequestReader requestReader(backendUrl, 0, 0, 0, 0);

  LOG(INFO) << "[OSM EXPORT] Fetching root relation IDs";

  requestReader.requestRows(
      query,
      [&reader](const char* data, size_t size) {
        reader.parse(data, size);
      },
      remoteAddr);

  reader.finish();

  LOG(INFO) << "[OSM EXPORT] Found "
          << relationIds.size()
          << " root relation IDs";


  if (relationIds.empty()) {
    throw std::runtime_error(
        "Relation-graph query returned no relation IDs.");
  }

  return relationIds;
}

// _____________________________________________________________________________
void exportQleverTsvToOsmXmlFile(const std::string& backendUrl,
                                 const std::string& query,
                                 const std::string& fileName,
                                 const std::string& remoteAddr) {
  std::ofstream out(fileName);

  if (!out) {
    throw std::runtime_error("Could not open OSM XML output file: " + fileName);
  }

  exportQleverTsvToOsmXml(backendUrl, query, out, remoteAddr);

  if (!out) {
    throw std::runtime_error("Failed to write OSM XML output file: " +
                             fileName);
  }
}

void exportQleverRelationTsvToOsmXml(
    const std::string& backendUrl, const std::string& query,
    std::ostream& out, const std::string& remoteAddr) {
  OsmPrimitiveStore store;
  OsmRelationTsvCollector collector(&store);

  RequestReader reader(backendUrl, 0, 0, 0, 0);
  reader.requestRows(
      query,
      [&collector](const char* data, size_t size) {
        collector.parse(data, size);
      },
      remoteAddr);

  collector.finish();
  writeOsmXml(store, out);
}

void exportQleverRelationGraphToOsmXml(
    const std::string& backendUrl,
    const std::set<int64_t>& rootRelationIds,
    std::ostream& out,
    const std::string& remoteAddr,
    const OsmQleverResolutionLimits& limits,
    OsmSourceDataset source) {
  if (rootRelationIds.empty()) {
    throw std::invalid_argument(
        "Relation-graph export requires at least one root relatoin ID.");
  }

  OsmPrimitiveStore store;

  fetchQleverRelationsIntoStore(
      backendUrl,
      rootRelationIds,
      &store,
      remoteAddr,
      limits.maxIdsPerRequest,
      source);

  for (const auto rootId : rootRelationIds) {
    const auto found = std::find_if(
        store.relations.begin(), store.relations.end(),
        [rootId](const OsmRelation& relation) {
          return relation.id == rootId;
        });

    if (found == store.relations.end()) {
      throw std::runtime_error(
          "Qlever did not return requested root relation ID: " +
          std::to_string(rootId));
    }
  }

  resolveQleverOsmReferencesIntoStore(
      backendUrl, &store, remoteAddr, limits, source);

  writeOsmXml(store, out);
}

void exportQleverRelationTsvToOsmXmlFile(
    const std::string& backendUrl, const std::string& query,
    const std::string& fileName, const std::string& remoteAddr) {
  std::ofstream out(fileName);

  if (!out) {
    throw std::runtime_error(
        "Could not open relation OSM export file: " + fileName);
  }

  exportQleverRelationTsvToOsmXml(backendUrl, query, out, remoteAddr);

  if (!out) {
    throw std::runtime_error(
        "Failed to write relation OSM export file: " + fileName);
  }
}

void exportQleverRelationGraphToOsmXmlFile(
    const std::string& backendUrl,
    const std::set<int64_t>& rootRelationIds,
    const std::string& fileName,
    const std::string& remoteAddr,
    const OsmQleverResolutionLimits& limits,
    OsmSourceDataset source) {
  std::ofstream out(fileName);

  if (!out) {
    throw std::runtime_error(
        "Could not open relation-graph OSM export file: " + fileName);
  }

  exportQleverRelationGraphToOsmXml(
      backendUrl, rootRelationIds, out, remoteAddr, limits, source);

  if (!out) {
    throw std::runtime_error(
        "Failed to write relation-graph OSM export file: " + fileName);
  }
}

// _____________________________________________________________________________
OsmTsvToXmlExporter::OsmTsvToXmlExporter(std::ostream& out)
    : _xmlWriter(out),
      _reader([this](const OsmObject& object) {
        OsmPrimitiveStore store;
        _primitiveBuilder.append(object, &store);
        _xmlWriter.write(store);
      }) {}

void OsmTsvToXmlExporter::parse(const char* data, size_t size) {
  if (_finished) {
    throw std::runtime_error("Cannot parse into a finished OSM XML export");
  }

  _reader.parse(data, size);
}

void OsmTsvToXmlExporter::finish() {
  if (_finished) {
    throw std::runtime_error("OSM XML export was already finished");
  }

  _reader.finish();
  _xmlWriter.finish();
  _finished = true;
}
// _____________________________________________________________________________
void writeOsmXml(const OsmPrimitiveStore& store, std::ostream& out) {
  OsmXmlStreamWriter writer(out);
  writer.write(store);
  writer.finish();
}

void writeOsmXmlFile(const OsmPrimitiveStore& store,
                     const std::string& fileName) {
  std::ofstream out(fileName);

  if (!out) {
    throw std::runtime_error("Could not open OSM XML output file: " + fileName);
  }

  writeOsmXml(store, out);

  if (!out) {
    throw std::runtime_error("Failed to write OSM XML output file: " + fileName);
  }
}
}  // namespace petrimaps

// _____________________________________________________________________________
std::string petrimaps::normalizeURL(const std::string& inURL) {
  CURLU* url = curl_url();
  if (!url) {
    std::stringstream ss;
    ss << "Could not normalize URL " << inURL;
    throw std::runtime_error(ss.str());
  }

  CURLUcode ret =
      curl_url_set(url, CURLUPART_URL, inURL.c_str(), CURLU_NON_SUPPORT_SCHEME);
  if (ret != CURLUE_OK) {
    curl_url_cleanup(url);
    std::stringstream ss;
    ss << "Could not normalize URL " << inURL;
    throw std::runtime_error(ss.str());
  }

  char* out = nullptr;
  ret = curl_url_get(url, CURLUPART_URL, &out, 0);
  if (ret != CURLUE_OK) {
    curl_url_cleanup(url);
    std::stringstream ss;
    ss << "Could not normalize URL " << inURL;
    throw std::runtime_error(ss.str());
  }

  std::string res(out);
  curl_free(out);
  curl_url_cleanup(url);

  // drop trailing /
  if (res.size() && res.back() == '/') res.pop_back();

  return res;
}

// _____________________________________________________________________________
std::string petrimaps::canonizeURL(const std::string& inURL,
                                   const std::string& remoteAddr) {
  CURL* curl = curl_easy_init();
  if (!curl) {
    std::stringstream ss;
    ss << "Could not canonize URL " << inURL;
    throw std::runtime_error(ss.str());
  }

  curl_easy_setopt(curl, CURLOPT_URL, inURL.c_str());
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 10L);
  curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);

  struct curl_slist* headers = 0;
  if (remoteAddr.size()) {
    headers = curl_slist_append(headers, ("X-Real-IP: " + remoteAddr).c_str());
  }

  if (headers) curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode res = curl_easy_perform(curl);
  if (res != CURLE_OK) {
    if (headers) curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    std::stringstream ss;
    ss << "Could not canonize URL " << inURL;
    ss << "\n";
    ss << curl_easy_strerror(res);
    throw std::runtime_error(ss.str());
  }

  char* effective = nullptr;
  curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &effective);

  if (!effective) {
    if (headers) curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    std::stringstream ss;
    ss << "Could not canonize URL " << inURL;
    throw std::runtime_error(ss.str());
  }

  std::string ret(effective);

  if (headers) curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  return normalizeURL(ret);
}

// _____________________________________________________________________________
std::string petrimaps::remoteAddress(int sock, const HeaderParams& headers) {
  auto it = headers.find("X-Real-IP");
  if (it != headers.end()) return it->second;
  struct sockaddr_storage addr;
  socklen_t len = sizeof(addr);
  if (getpeername(sock, reinterpret_cast<struct sockaddr*>(&addr), &len) != 0) {
    return "";
  }

  char buf[INET6_ADDRSTRLEN] = {0};

  if (addr.ss_family == AF_INET) {
    auto* s = reinterpret_cast<struct sockaddr_in*>(&addr);
    inet_ntop(AF_INET, &s->sin_addr, buf, sizeof(buf));
  } else if (addr.ss_family == AF_INET6) {
    auto* s = reinterpret_cast<struct sockaddr_in6*>(&addr);
    inet_ntop(AF_INET6, &s->sin6_addr, buf, sizeof(buf));

    // unwrap IPv4-mapped IPv6 addresses
    std::string ip(buf);
    if (ip.rfind("::ffff:", 0) == 0 && ip.find('.') != std::string::npos)
      return ip.substr(7);

    return ip;
  }

  return buf;
}

// _____________________________________________________________________________
std::string RequestReader::requestIndexHash(const std::string& configHash) {
  std::string response;
  std::string url = _backendUrl + "/?cmd=get-index-id";

  try {
    performCurlRequest(
        url, "", "", "",
        [&response](const char* c, size_t n) { response.append(c, n); },
        nullptr);
  } catch (const std::exception& e) {
    LOG(WARN) << "[GEOMCACHE] Could not obtain index hash: " << e.what();
    return "";
  }

  return INDEX_HASH_PREFIX + "|" + configHash + "|" + response;
}

// _____________________________________________________________________________
petrimaps::GeoPointFormat RequestReader::requestGeoPointFormat() {
  // Probe query to determine the point format from a known coordinate.
  const static std::string query =
      "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "
      "SELECT ?point WHERE { BIND(\"POINT(" STR(PROBE_LON) " " STR(
          PROBE_LAT) ")\"^^geo:wktLiteral AS ?point) }";

  std::string response;

  try {
    performCurlRequest(
        _backendUrl, queryFields(query), "application/octet-stream", "",
        [&response](const char* c, size_t n) { response.append(c, n); },
        nullptr);
  } catch (const std::exception& e) {
    LOG(WARN) << "[GEOMCACHE] Could not obtain the format of a point: "
              << e.what();
    return {};
  }

  // The answer is a single ID in the byte order it was stored in
  if (response.size() != sizeof(ID)) {
    LOG(WARN)
        << "[GEOMCACHE] Unexpected answer of size " << response.size()
        << " when asking for the format of a point, expected single ID of size "
        << sizeof(ID);
    return {};
  }

  ID id;
  std::memcpy(id.bytes, response.data(), sizeof(id.bytes));

  GeoPointFormat format;
  format.datatype = idDatatype(id.val);

  uint64_t valueBits = id.val & ((uint64_t(1) << 60) - 1);
  for (auto encoding :
       {GeoPointEncoding::ZOrder, GeoPointEncoding::LatitudeAndLongitude}) {
    auto point = decodeGeoPoint(valueBits, encoding);
    if (fabs(point.getX() - PROBE_LON) < 0.001 &&
        fabs(point.getY() - PROBE_LAT) < 0.001) {
      format.encoding = encoding;
      return format;
    }
  }

  LOG(WARN) << "[GEOMCACHE] Could determine point encoding, assuming lat/lon";
  return format;
}

// _____________________________________________________________________________
util::geo::FPoint petrimaps::decodeGeoPoint(uint64_t valueBits,
                                            GeoPointEncoding encoding) {
  uint64_t lat, lon;
  if (encoding == GeoPointEncoding::ZOrder) {
    lat = everySecondBit(valueBits >> 1);
    lon = everySecondBit(valueBits);
  } else {
    lat = (valueBits >> 30) & MAX_QUANTIZED_COORD;
    lon = valueBits & MAX_QUANTIZED_COORD;
  }
  return {
      static_cast<float>(
          (static_cast<double>(lon) / MAX_QUANTIZED_COORD) * 2 * 180 - 180),
      static_cast<float>(
          (static_cast<double>(lat) / MAX_QUANTIZED_COORD) * 2 * 90 - 90)};
}
