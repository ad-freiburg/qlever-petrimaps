// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>
//          Bohyun Kim <bk233@email.uni-freiburg.de>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdint.h>
#include <sys/socket.h>

#include <cmath>
#include <cstring>
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

  // this is a context that holds to things: a std::function for parsing, and
  // an exception_ptr for storing any exception encountered during parsing (for
  // later rethrow)
  struct CallbackContext {
    const std::function<void(const char*, size_t)>& parse;
    std::exception_ptr exception;
  } cbContext{parse, nullptr};

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
    auto* c = static_cast<CallbackContext*>(userp);
    try {
      c->parse(static_cast<const char*>(contents), realsize);
    } catch (...) {
      // store exception, then return with an error (aborts curl request)
      c->exception = std::current_exception();
      return CURLE_WRITE_ERROR;
    }
    return realsize;
  };

  // any newly read block will be given to the parse() method of the handed cb
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cb);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &cbContext);
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

  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);

  if (headers) curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  if (httpCode != 200) {
    std::stringstream ss;
    ss << "QLever backend returned status code " << httpCode;
    if (raw) ss << "\n" << *raw;
    throw std::runtime_error(ss.str());
  }

  // rethrow any exception encountered during write callback
  if (cbContext.exception) std::rethrow_exception(cbContext.exception);

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
        _ids[_curIdCol].push_back({_curId.val, _ids[_curIdCol].size()});
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
          curCols.push_back({_colNames[_curCol], _dangling});

          if (*c == '\n') {
            _curRow++;
            rows.push_back(curCols);
            curCols = {};
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

int64_t addGeneratedNode(OsmPrimitiveStore* store, int64_t* nextNodeId,
                         const util::geo::DPoint& point) {
  const int64_t id = (*nextNodeId)--;
  store->nodes.push_back({id, point.getX(), point.getY(), {}});
  return id;
}

int64_t addWayFromLine(
  OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
  const util::geo::DLine& line,
  const std::unordered_map<std::string, std::string>& tags) {
  OsmWay way;
  way.id = (*nextWayId)--;
  way.tags = tags;

  for (const auto& point : line) {
    way.nodeRefs.push_back(
      addGeneratedNode(store, nextNodeId, point));
  }

  store->ways.push_back(way);
  return way.id;
}

int64_t addWayFromRing(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
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
    way.nodeRefs.push_back(addGeneratedNode(store, nextNodeId, *it));
  }

  if (!way.nodeRefs.empty()) {
    way.nodeRefs.push_back(way.nodeRefs.front());
  }

  store->ways.push_back(way);
  return way.id;
}

void addPolygonMembersToRelation(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    const util::geo::DPolygon& polygon, OsmRelation* relation) {
  const std::unordered_map<std::string, std::string> noTags;

  const auto outerWayId = addWayFromRing(
      store, nextNodeId, nextWayId, polygon.getOuter(), noTags);
  relation->members.push_back({"way", outerWayId, "outer"});

  for (const auto& inner : polygon.getInners()) {
    const auto innerWayId =
        addWayFromRing(store, nextNodeId, nextWayId, inner, noTags);
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
    int64_t* nextRelationId, const util::geo::DPolygon& polygon,
    const std::unordered_map<std::string, std::string>& tags,
    const std::string& osmId) {
  const auto& outer = polygon.getOuter();
  const auto& inners = polygon.getInners();

  if (inners.empty()) {
    const auto wayId=
        addWayFromRing(store, nextNodeId, nextWayId, outer, tags);
    return {"way", wayId, ""};
  }

  OsmRelation relation;
  relation.id = (*nextRelationId)--;

  addPolygonMembersToRelation(
      store, nextNodeId, nextWayId, polygon, &relation);
  setRelationTags(&relation, tags, "multipolygon", osmId);

  store->relations.push_back(relation);
  return {"relation", relation.id, ""};
}

OsmRelationMember addWayFromMultiPolygon(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    int64_t* nextRelationId,
    const util::geo::DMultiPolygon& multiPolygon,
    const std::unordered_map<std::string, std::string>& tags,
    const std::string& osmId) {
  OsmRelation relation;
  relation.id = (*nextRelationId)--;

  for (const auto& polygon : multiPolygon) {
    addPolygonMembersToRelation(
        store, nextNodeId, nextWayId, polygon, &relation);
  }

  setRelationTags(&relation, tags, "multipolygon", osmId);
  store->relations.push_back(relation);
  return {"relation", relation.id, ""};
}

OsmRelationMember addNodesFromMultiPoint(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextRelationId,
    const util::geo::DMultiPoint& multiPoint,
    const std::unordered_map<std::string, std::string>& tags,
    const std::string& osmId) {
  OsmRelation relation;
  relation.id = (*nextRelationId)--;

  for (const auto& point : multiPoint) {
    const auto nodeId = addGeneratedNode(store, nextNodeId, point);
    relation.members.push_back({"node", nodeId, ""});
  }

  setRelationTags(&relation, tags, "multipoint", osmId);
  store->relations.push_back(relation);
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
    int64_t* nextRelationId, const util::geo::DMultiLine& multiLine,
    const std::unordered_map<std::string, std::string>& tags,
    const std::string& osmId) {
  OsmRelation relation;
  relation.id = (*nextRelationId)--;

  const std::unordered_map<std::string, std::string> noTags;

  for (const auto& line : multiLine) {
    const auto wayId =
        addWayFromLine(store, nextNodeId, nextWayId, line, noTags);
    relation.members.push_back({"way", wayId, ""});
  }

  setRelationTags(&relation, tags, "multilinestring", osmId);
  store->relations.push_back(relation);
  return {"relation", relation.id, ""};
}

OsmRelationMember addCollectionMember(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    int64_t* nextRelationId,
    const util::geo::AnyGeometry<double>& geometry);

OsmRelationMember addGeometryCollection(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    int64_t* nextRelationId, const util::geo::DCollection& collection,
    const std::unordered_map<std::string, std::string>& tags,
    const std::string& osmId) {
  OsmRelation relation;
  relation.id = (*nextRelationId)--;

  for (const auto& geometry : collection) {
    relation.members.push_back(addCollectionMember(
        store, nextNodeId, nextWayId, nextRelationId, geometry));
  }

  setRelationTags(&relation, tags, "geometrycollection", osmId);
  store->relations.push_back(relation);

  return {"relation", relation.id, ""};
}

OsmRelationMember addCollectionMember(
    OsmPrimitiveStore* store, int64_t* nextNodeId, int64_t* nextWayId,
    int64_t* nextRelationId,
    const util::geo::AnyGeometry<double>& geometry) {
  const std::unordered_map<std::string, std::string> noTags;

  switch (geometry.getType()) {
    case 0: {
      const auto nodeId =
          addGeneratedNode(store, nextNodeId, geometry.getPoint());
      return {"node", nodeId, ""};
    }

    case 1: {
      const auto wayId = addWayFromLine(
          store, nextNodeId, nextWayId, geometry.getLine(), noTags);
      return {"way", wayId, ""};
    }

    case 2:
      return addWayFromPolygon(
          store, nextNodeId, nextWayId, nextRelationId,
          geometry.getPolygon(), noTags, "");

    case 3:
      return addWaysFromMultiLine(
          store, nextNodeId, nextWayId, nextRelationId,
          geometry.getMultiLine(), noTags,"");

    case 4:
      return addWayFromMultiPolygon(
          store, nextNodeId, nextWayId, nextRelationId,
          geometry.getMultiPolygon(), noTags, "");

    case 5:
      return addGeometryCollection(
          store, nextNodeId, nextWayId, nextRelationId,
          geometry.getCollection(), noTags, "");

    case 6:
      return addNodesFromMultiPoint(
          store, nextNodeId, nextRelationId,
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
  if (id.rfind("osmnode:", 0) == 0 ||
      id.find("/node/") != std::string::npos) {
    return "node";
  }
  if (id.rfind("osmway:", 0) == 0 ||
      id.find("/way/") != std::string::npos) {
    return "way";
  }

  if (id.rfind("osmrel:", 0) == 0 ||
      id.rfind("osmrelation:", 0) == 0 ||
      id.find("/relation/") != std::string::npos) {
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

OsmResultReader::OsmResultReader(ObjectCallback cb) : _cb(cb) {}

void OsmResultReader::parse(const char* data, size_t size) {
  const char* start = data;
  while (data < start + size) {
    switch (_state) {
      case IN_HEADER:
        if (*data == '\t' || *data == '\n') {
          finishCell();
        }

        if (*data == '\n') {
          _state = IN_ROW;
          data++;
          continue;
        }

        if (*data != '\t') _dangling += *data;
        data++;
        continue;

      case IN_ROW:
        if (*data == '\t' || *data == '\n') {
          finishCell();

          if (*data == '\n') {
            finishRow();
          }

          data++;
          continue;
        }

        _dangling += *data;
        data++;
        break;
    }
  }
}

void OsmResultReader::finish() {
  if (!_dangling.empty()) {
    finishCell();
  }

  if (!_curRow.empty()) {
    finishRow();
  }

  if (_hasCurrent) {
    _cb(_current);
    _hasCurrent = false;
  }
}

void OsmResultReader::finishCell() {
  if (_state == IN_HEADER) {
    _colNames.push_back(_dangling);
  } else {
    _curRow.push_back(_dangling);
  }

  _dangling.clear();
}

void OsmResultReader::finishRow() {
  if (_curRow.empty()) return;

  mergeRowIntoCurrentObject();
  _curRow.clear();
  _curCol = 0;
}

void OsmResultReader::startObject(const std::string& osmId,
                                  const std::string& wkt) {
  if (_hasCurrent) {
    _cb(_current);
  }

  _current = {};
  _current.id = osmId;
  _current.type = inferOsmObjectType(osmId);
  _current.wkt = wkt;
  _hasCurrent = true;
}

void OsmResultReader::mergeRowIntoCurrentObject() {
  const auto osmId = getRequiredCell(_colNames, _curRow, "osm_id");
  const auto tagKey =
      normalizeOsmTagKey(getRequiredCell(_colNames, _curRow, "a"));
  const auto tagValue =
      normalizeSparqlLiteralValue(getRequiredCell(_colNames, _curRow, "b"));
  const auto wkt = getRequiredCell(_colNames, _curRow, "hasgeometry");

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
  const char* wktStart = nullptr;
  const auto crsType = util::geo::getCRSType(object.wkt.c_str(), &wktStart);
  const auto wktType = util::geo::getWKTType(wktStart, &wktStart);

  if (crsType == util::geo::CRSType::UNSUPPORTED) {
    throw std::runtime_error("Unsupported CRS for osm_id: " + object.id);
  }

  if (wktType == util::geo::WKTType::POINT) {
    const auto point = util::geo::pointFromWKT<double>(wktStart, nullptr);
    store->nodes.push_back(
        {_nextNodeId--, point.getX(), point.getY(), object.tags});
  } else if (wktType == util::geo::WKTType::MULTIPOINT) {
    const auto multiPoint =
        util::geo::multiPointFromWKT<double>(wktStart, nullptr);
    addNodesFromMultiPoint(
        store, &_nextNodeId, &_nextRelationId, multiPoint, object.tags,
        object.id);
  } else if (wktType == util::geo::WKTType::LINESTRING) {
    const auto line = util::geo::lineFromWKT<double>(wktStart, nullptr);
    addWayFromLine(store, &_nextNodeId, &_nextWayId, line, object.tags);
  } else if (wktType == util::geo::WKTType::MULTILINESTRING) {
    const auto multiLine =
        util::geo::multiLineFromWKT<double>(wktStart, nullptr);
    addWaysFromMultiLine(
        store, &_nextNodeId, &_nextWayId, &_nextRelationId, multiLine,
        object.tags, object.id);
  } else if (wktType == util::geo::WKTType::POLYGON) {
    const auto polygon =
        util::geo::polygonFromWKT<double>(wktStart, nullptr);
    addWayFromPolygon(store, &_nextNodeId, &_nextWayId, &_nextRelationId,
                      polygon, object.tags, object.id);
  } else if (wktType == util::geo::WKTType::MULTIPOLYGON) {
    const auto multiPolygon =
        util::geo::multiPolygonFromWKT<double>(wktStart, nullptr);
    addWayFromMultiPolygon(
        store, &_nextNodeId, &_nextWayId, &_nextRelationId, multiPolygon,
        object.tags, object.id);
  } else if (wktType == util::geo::WKTType::COLLECTION) {
    const auto collection =
        util::geo::collectionFromWKT<double>(wktStart, nullptr);
    addGeometryCollection(
        store, &_nextNodeId, &_nextWayId, &_nextRelationId, collection,
        object.tags, object.id);
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
  });
}

OsmXmlStreamWriter::~OsmXmlStreamWriter() = default;

void OsmXmlStreamWriter::writeNode(const OsmNode& node) {
  _xml->openTag("node", {
      {"id", intToXmlAttr(node.id)},
      {"lat", doubleToXmlAttr(node.lat)},
      {"lon", doubleToXmlAttr(node.lon)},
  });

  for (const auto& tag : node.tags) {
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

  for (const auto& tag : way.tags) {
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

  for (const auto& tag : relation.tags) {
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
  reader.requestRows(
      query,
      [&exporter](const char* data, size_t size) {
        exporter.parse(data, size);
      },
      remoteAddr);

  exporter.finish();
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
  return {(static_cast<double>(lon) / MAX_QUANTIZED_COORD) * 2 * 180 - 180,
          (static_cast<double>(lat) / MAX_QUANTIZED_COORD) * 2 * 90 - 90};
}
