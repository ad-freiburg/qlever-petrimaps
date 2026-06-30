// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <curl/curl.h>
#include <stdint.h>

#include <exception>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "util/Misc.h"
#include "util/log/Log.h"

#ifndef PETRIMAPS_MISC_H_
#define PETRIMAPS_MISC_H_

#define ID_TYPE uint32_t
#define QLEVER_ID_TYPE size_t

// half of the ID space for points, half for the rest
const static ID_TYPE I_OFFSET = 2147483648;
const static size_t MAXROWS = 18446744073709551615u;

// major coordinates will fit into 2^15, as coordinates go from
// -200375083.427892 to +200375083.427892
const static int16_t M_COORD_GRANULARITY = 12230;
const static int16_t M_COORD_OFFSET = 16384;

const static std::string CURL_USER_AGENT = "petrimaps";

namespace petrimaps {

enum ParseState { IN_HEADER, IN_ROW };

struct IdMapping {
  QLEVER_ID_TYPE qid;
  ID_TYPE id;
};

union ID {
  uint64_t val;
  uint8_t bytes[8];
};

inline bool operator<(const IdMapping& lh, const IdMapping& rh) {
  if (lh.qid < rh.qid) return true;
  return false;
}

inline int16_t mCoord(int16_t c) {
  if (c < 0) return c - M_COORD_OFFSET;
  return c + M_COORD_OFFSET;
}

inline int16_t rmCoord(int16_t c) {
  if (c < -M_COORD_OFFSET) return c + M_COORD_OFFSET;
  return c - M_COORD_OFFSET;
}

inline int16_t isMCoord(int16_t c) {
  return c < -M_COORD_OFFSET || c >= M_COORD_OFFSET;
}

std::string normalizeURL(const std::string& inURL);
std::string canonizeURL(const std::string& inURL,
                        const std::string& remoteAddr);
std::string remoteAddress(int sock);

void performCurlRequest(const std::string& url, const std::string& postFields,
                        const std::string& acceptHeader,
                        const std::string& xRealIP,
                        const std::function<void(const char*, size_t)>& parse,
                        const std::string* raw);

class OutOfMemoryError : public std::exception {
 public:
  explicit OutOfMemoryError(double want, size_t have, size_t max) {
    std::stringstream ss;
    ss << "Out of memory, ";
    ss << "want: " << want << " bytes, already used: " << have << " of " << max
       << " bytes";

    _msg = ss.str();
  }

  const char* what() const noexcept { return _msg.c_str(); }

 private:
  std::string _msg;
};

inline void checkMem(double want, double max) {
  double currentSize = util::getCurrentRSS();

  if (currentSize + want > max) {
    throw OutOfMemoryError(want, currentSize, max);
  }
}

inline void petrimapsCurlSetup(CURL* curl) {
  curl_easy_reset(curl);
  curl_easy_setopt(curl, CURLOPT_USERAGENT, CURL_USER_AGENT.c_str());
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, false);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, false);
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, 0);
  curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, "");
}

size_t writeStringCb(void* contents, size_t size, size_t nmemb, void* userp);

inline std::string httpRequest(const std::string& url,
                               const std::string& postFields = "",
                               const std::string& xRealIP = "") {
  CURL* curl = curl_easy_init();
  CURLcode res;
  char errbuf[CURL_ERROR_SIZE];

  std::string resString;

  petrimapsCurlSetup(curl);
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  if (postFields.size()) {
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postFields.c_str());
  }
  struct curl_slist* headers = 0;
  if (xRealIP.size()) {
    LOG(util::INFO) << "[SERVER] Remote address is " << xRealIP;
    headers = curl_slist_append(headers,
                                ("X-Real-IP: " + xRealIP).c_str());
  }
  if (headers) curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeStringCb);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resString);
  curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
  res = curl_easy_perform(curl);

  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);

  if (httpCode != 200) {
    std::stringstream ss;
    ss << "Remote server returned status code " << httpCode;
    ss << "\n";
    ss << resString;

    if (headers) curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    throw std::runtime_error(ss.str());
  }

  if (res != CURLE_OK) {
    std::stringstream ss;
    ss << "HTTP request failed: ";
    size_t len = strlen(errbuf);
    if (len > 0) {
      ss << errbuf;
    } else {
      ss << curl_easy_strerror(res);
    }

    if (headers) curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    throw std::runtime_error(ss.str());
  }

  if (headers) curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  return resString;
}

struct RequestReader {
  explicit RequestReader(const std::string& backendUrl, size_t maxMemory,
                         size_t geomFields, size_t valFields,
                         size_t rasterMetaFields)
      : _backendUrl(backendUrl),
        _maxMemory(maxMemory),
        _geomFields(geomFields),
        _valFields(valFields),
        _rasterMetaFields(rasterMetaFields) {
    _ids.resize(geomFields);
    _vals.resize(valFields);
    _rasterMetas.resize(rasterMetaFields);
  }

  std::vector<std::string> requestColumns(const std::string& query);
  void requestIds(const std::string& qurl, const std::string& remoteAddr);
  std::map<size_t, std::pair<double, double>> requestRasterMeta(
      const std::string& query, const std::string& remoteAddr);
  std::string requestIndexHash(const std::string& configHash);
  void requestRows(const std::string& qurl, const std::string& remoteAddr);
  void requestRows(const std::string& query,
                   const std::function<void(const char*, size_t)>& parse,
                   const std::string& remoteAddr);
  void parse(const char*, size_t size);
  void parseIds(const char*, size_t size);
  void parseRasterMeta(const char*, size_t size);

  std::string queryFields(const std::string& query) const;

  std::string _backendUrl;

  std::vector<std::string> _colNames;
  size_t _curCol = 0;
  size_t _curRow = 0;

  std::string _dangling, _raw, _curVal;
  size_t _curDatasetId = 0;
  double _curFieldWidth = 0;
  double _curFieldHeight = 0;
  std::map<size_t, std::pair<double, double>> _curRasterFieldDimensions;

  ParseState _state = IN_HEADER;

  std::vector<std::vector<std::pair<std::string, std::string>>> rows;
  std::vector<std::pair<std::string, std::string>> curCols;

  uint8_t _curByte = 0;
  size_t _curIdCol = 0;
  ID _curId;
  size_t _received = 0;
  std::vector<std::vector<IdMapping>> _ids;
  std::vector<std::vector<double>> _vals;
  std::vector<std::vector<size_t>> _rasterMetas;
  size_t _maxMemory;

  size_t _geomFields;
  size_t _valFields;
  size_t _rasterMetaFields;
};

}  // namespace petrimaps

#endif  // PETRIMAPS_MISC_H_
