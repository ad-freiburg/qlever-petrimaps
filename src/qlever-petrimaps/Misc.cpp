// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <stdint.h>

#include <cstring>
#include <string>
#include <vector>

#include "qlever-petrimaps/Misc.h"
#include "util/log/Log.h"

using petrimaps::RequestReader;

// _____________________________________________________________________________
std::vector<std::string> RequestReader::requestColumns(const std::string& query) {
  CURLcode res;
  char errbuf[CURL_ERROR_SIZE];

  std::string resString;

  if (_curl) {
    auto url = queryUrl(query) + "&action=tsv_export";
    curl_easy_setopt(_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, RequestReader::writeStringCb);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, &resString);
    curl_easy_setopt(_curl, CURLOPT_SSL_VERIFYPEER, false);
    curl_easy_setopt(_curl, CURLOPT_SSL_VERIFYHOST, false);
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, 0);

    // accept any compression supported
    curl_easy_setopt(_curl, CURLOPT_ACCEPT_ENCODING, "");
    curl_easy_setopt(_curl, CURLOPT_ERRORBUFFER, errbuf);
    res = curl_easy_perform(_curl);

    long httpCode = 0;
    curl_easy_getinfo(_curl, CURLINFO_RESPONSE_CODE, &httpCode);

    if (httpCode != 200) {
      std::stringstream ss;
      ss << "QLever backend returned status code " << httpCode;
      ss << "\n";
      ss << _raw;
      throw std::runtime_error(ss.str());
    }

    if (exceptionPtr) std::rethrow_exception(exceptionPtr);

  } else {
    std::stringstream ss;
    ss << "[REQUESTREADER] Failed to perform curl request.\n";
    throw std::runtime_error(ss.str());
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
    curl_easy_setopt(_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, RequestReader::writeCbIds);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, this);
    curl_easy_setopt(_curl, CURLOPT_SSL_VERIFYPEER, false);
    curl_easy_setopt(_curl, CURLOPT_SSL_VERIFYHOST, false);
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, 0);

    // set headers
    struct curl_slist* headers = 0;
    headers = curl_slist_append(headers, "Accept: application/octet-stream");
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, headers);

    // accept any compression supported
    curl_easy_setopt(_curl, CURLOPT_ACCEPT_ENCODING, "");
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
                                size_t (*writeCb)(void*, size_t, size_t,
                                                  void*), void* ptr) {
  CURLcode res;
  char errbuf[CURL_ERROR_SIZE];

  _raw.clear();
  _raw.reserve(10000);

  if (_curl) {
    auto url = queryUrl(query);
    curl_easy_setopt(_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, writeCb);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, ptr);
    curl_easy_setopt(_curl, CURLOPT_SSL_VERIFYPEER, false);
    curl_easy_setopt(_curl, CURLOPT_SSL_VERIFYHOST, false);
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, 0);

    // set headers
    struct curl_slist* headers = 0;
    headers = curl_slist_append(headers, "Accept: text/tab-separated-values");
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, headers);

    // accept any compression supported
    curl_easy_setopt(_curl, CURLOPT_ACCEPT_ENCODING, "");
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
size_t RequestReader::writeStringCb(void* contents, size_t size, size_t nmemb,
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

    if (_curByte == 0) {
      _ids.push_back({_curId.val, _ids.size()});
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
