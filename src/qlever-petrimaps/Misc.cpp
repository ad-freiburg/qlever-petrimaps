// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <stdint.h>

#include <cstring>
#include <string>
#include <vector>

#include "qlever-petrimaps/Misc.h"
#include "util/String.h"
#include "util/log/Log.h"

using petrimaps::RequestReader;
using util::LogLevel::ERROR;
using util::LogLevel::INFO;
using util::LogLevel::WARN;

// _____________________________________________________________________________
std::vector<std::string> RequestReader::requestColumns(
    const std::string& query) {

  std::string resString;

  try {
    auto url = queryUrl(query) + "&action=tsv_export";
    resString = httpRequest(url);
  } catch (const std::runtime_error& e) {
    std::stringstream ss;
    ss << "[REQUESTREADER] " << e.what();
    throw std::runtime_error(ss.str());
  }

  return util::split(util::trim(resString), '\t');
}

// _____________________________________________________________________________
void RequestReader::requestIds(const std::string& query) {
  CURLcode res;
  char errbuf[CURL_ERROR_SIZE];

  _raw.clear();
  _raw.reserve(10000);

  if (_curl) {
    auto url = queryUrl(query);
    petrimapsCurlSetup(_curl);
    curl_easy_setopt(_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, RequestReader::writeCbIds);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, this);

    // set headers
    struct curl_slist* headers = 0;
    headers = curl_slist_append(headers, "Accept: application/octet-stream");
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(_curl, CURLOPT_ERRORBUFFER, errbuf);
    res = curl_easy_perform(_curl);

    long httpCode = 0;
    curl_easy_getinfo(_curl, CURLINFO_RESPONSE_CODE, &httpCode);

    curl_slist_free_all(headers);

    if (httpCode != 200) {
      std::stringstream ss;
      ss << "QLever backend returned status code " << httpCode;
      ss << "\n";
      ss << _raw;
      throw std::runtime_error(ss.str());
    }

    if (exceptionPtr) std::rethrow_exception(exceptionPtr);

  } else {
    LOG(ERROR) << "[REQUESTREADER] Failed to perform curl request.";
    return;
  }

  if (res != CURLE_OK) {
    std::stringstream ss;
    ss << "QLever backend request failed: ";
    size_t len = strlen(errbuf);
    if (len > 0) {
      LOG(ERROR) << "[REQUESTREADER] " << errbuf;
      ss << errbuf;
    } else {
      LOG(ERROR) << "[REQUESTREADER] " << curl_easy_strerror(res);
      ss << curl_easy_strerror(res);
    }

    throw std::runtime_error(ss.str());
  }
}

// _____________________________________________________________________________
void RequestReader::requestRows(const std::string& query) {
  return requestRows(query, RequestReader::writeCb, this);
}

// _____________________________________________________________________________
void RequestReader::requestRows(const std::string& query,
                                size_t (*writeCb)(void*, size_t, size_t, void*),
                                void* ptr) {
  CURLcode res;
  char errbuf[CURL_ERROR_SIZE];

  _raw.clear();
  _raw.reserve(10000);

  if (_curl) {
    auto url = queryUrl(query);
    petrimapsCurlSetup(_curl);
    curl_easy_setopt(_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, writeCb);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, ptr);

    // set headers
    struct curl_slist* headers = 0;
    headers = curl_slist_append(headers, "Accept: text/tab-separated-values");
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(_curl, CURLOPT_ERRORBUFFER, errbuf);
    res = curl_easy_perform(_curl);

    long httpCode = 0;
    curl_easy_getinfo(_curl, CURLINFO_RESPONSE_CODE, &httpCode);

    curl_slist_free_all(headers);

    if (httpCode != 200) {
      std::stringstream ss;
      ss << "QLever backend returned status code " << httpCode;
      ss << "\n";
      ss << _raw;
      throw std::runtime_error(ss.str());
    }

    if (exceptionPtr) std::rethrow_exception(exceptionPtr);
  } else {
    LOG(ERROR) << "[REQUESTREADER] Failed to perform curl request.";
    return;
  }

  if (res != CURLE_OK) {
    std::stringstream ss;
    ss << "QLever backend request failed: ";
    size_t len = strlen(errbuf);
    if (len > 0) {
      LOG(ERROR) << "[REQUESTREADER] " << errbuf;
      ss << errbuf;
    } else {
      LOG(ERROR) << "[REQUESTREADER] " << curl_easy_strerror(res);
      ss << curl_easy_strerror(res);
    }

    throw std::runtime_error(ss.str());
  }
}

// _____________________________________________________________________________
std::string RequestReader::queryUrl(const std::string& query) const {
  auto escStr = curl_easy_escape(_curl, query.c_str(), query.size());
  std::string esc = escStr;
  curl_free(escStr);

  return _backendUrl + "/?send=18446744073709551615" + "&query=" + esc;
}

// _____________________________________________________________________________
size_t petrimaps::writeStringCb(void* contents, size_t size, size_t nmemb,
                                    void* userp) {
  ((std::string*)userp)->append((char*)contents, size * nmemb);
  return size * nmemb;
}

// _____________________________________________________________________________
size_t RequestReader::writeCb(void* contents, size_t size, size_t nmemb,
                              void* userp) {
  size_t realsize = size * nmemb;
  try {
    static_cast<RequestReader*>(userp)->parse(
        static_cast<const char*>(contents), realsize);
  } catch (...) {
    static_cast<RequestReader*>(userp)->exceptionPtr = std::current_exception();
    return CURLE_WRITE_ERROR;
  }

  return realsize;
}

// _____________________________________________________________________________
size_t RequestReader::writeCbIds(void* contents, size_t size, size_t nmemb,
                                 void* userp) {
  size_t realsize = size * nmemb;
  try {
    static_cast<RequestReader*>(userp)->parseIds(
        static_cast<const char*>(contents), realsize);
  } catch (...) {
    static_cast<RequestReader*>(userp)->exceptionPtr = std::current_exception();
    return CURLE_WRITE_ERROR;
  }

  return realsize;
}

// _____________________________________________________________________________
void RequestReader::parseIds(const char* c, size_t size) {
  // TODO: just a rough approximation
  checkMem(size, _maxMemory);

  for (size_t i = 0; i < size; i++) {
    if (_raw.size() < 10000) _raw.push_back(c[i]);
    _curId.bytes[_curByte] = c[i];
    _curByte = (_curByte + 1) % 8;

    _curIdCol = _curIdCol % (_geomFields + _valFields.size());

    if (_curByte == 0) {
      if (_curIdCol < _geomFields) {
        // geometry ID
        _ids[_curIdCol].push_back({_curId.val, _ids[_curIdCol].size()});
      } else {
        // value

        uint8_t type = (_curId.val & (uint64_t(15) << 60)) >> 60;
        if (type == 3) {
          // 3 = double in qlever
          uint64_t rawBits = (_curId.val << 4);
          double val = 0;
          std::memcpy(&val, &rawBits, sizeof(val));
          size_t valCol = _curIdCol - _geomFields;
          _vals[valCol].push_back(val);
        } else if (type == 2) {
          // 2 = int in qlever
          uint64_t rawBits = (_curId.val << 4) >> 4;
          int64_t val = 0;
          std::memcpy(&val, &rawBits, sizeof(val));
          size_t valCol = _curIdCol - _geomFields;
          _vals[valCol].push_back(val);
        }
      }
      _curIdCol += 1;
    }
  }
}

// _____________________________________________________________________________
void RequestReader::parse(const char* c, size_t size) {
  // TODO: just a rough approximation
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
  if (res.back() == '/') res.pop_back();

  return res;
}

// _____________________________________________________________________________
std::string petrimaps::canonizeURL(const std::string& inURL) {
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

  CURLcode res = curl_easy_perform(curl);
  if (res != CURLE_OK) {
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
    curl_easy_cleanup(curl);
    std::stringstream ss;
    ss << "Could not canonize URL " << inURL;
    throw std::runtime_error(ss.str());
  }

  std::string ret(effective);

  curl_easy_cleanup(curl);
  return normalizeURL(ret);
}
