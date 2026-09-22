// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdint.h>
#include <sys/socket.h>

#include <cmath>
#include <cstring>
#include <exception>
#include <string>
#include <vector>

#include "qlever-petrimaps/Misc.h"
#include "util/String.h"
#include "util/log/Log.h"

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
