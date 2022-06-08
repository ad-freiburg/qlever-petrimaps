// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <string>
#include <cstring>
#include <vector>
#include <stdint.h>
#include "qlever-petrimaps/Misc.h"
#include "util/log/Log.h"

using petrimaps::RequestReader;

// _____________________________________________________________________________
void RequestReader::requestIds(const std::string& query) {
  CURLcode res;
  char errbuf[CURL_ERROR_SIZE];

  if (_curl) {
    auto url = queryUrl(query);
    curl_easy_setopt(_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, RequestReader::writeCbIds);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, this);
    curl_easy_setopt(_curl, CURLOPT_SSL_VERIFYPEER, false);
    curl_easy_setopt(_curl, CURLOPT_SSL_VERIFYHOST, false);

    // set headers
    struct curl_slist* headers = 0;
    headers = curl_slist_append(headers, "Accept: application/octet-stream");
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, headers);

    // accept any compression supported
    curl_easy_setopt(_curl, CURLOPT_ACCEPT_ENCODING, "");
    curl_easy_setopt(_curl, CURLOPT_ERRORBUFFER, errbuf);
    res = curl_easy_perform(_curl);

    if (exceptionPtr) std::rethrow_exception(exceptionPtr);
  } else {
    LOG(ERROR) << "[REQUESTREADER] Failed to perform curl request.";
    return;
  }

  if (res != CURLE_OK) {
    size_t len = strlen(errbuf);
    if (len > 0) {
      LOG(ERROR) << "[REQUESTREADER] " << errbuf;
     } else {
      LOG(ERROR) << "[REQUESTREADER] " << curl_easy_strerror(res);
    }
  }
}

// _____________________________________________________________________________
void RequestReader::requestRows(const std::string& query) {
  CURLcode res;
  char errbuf[CURL_ERROR_SIZE];

  if (_curl) {
    auto url = queryUrl(query);
    curl_easy_setopt(_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, RequestReader::writeCb);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, this);
    curl_easy_setopt(_curl, CURLOPT_SSL_VERIFYPEER, false);
    curl_easy_setopt(_curl, CURLOPT_SSL_VERIFYHOST, false);

    // set headers
    struct curl_slist* headers = 0;
    headers = curl_slist_append(headers, "Accept: text/tab-separated-values");
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, headers);

    // accept any compression supported
    curl_easy_setopt(_curl, CURLOPT_ACCEPT_ENCODING, "");
    curl_easy_setopt(_curl, CURLOPT_ERRORBUFFER, errbuf);
    res = curl_easy_perform(_curl);

    if (exceptionPtr) std::rethrow_exception(exceptionPtr);
  } else {
    LOG(ERROR) << "[REQUESTREADER] Failed to perform curl request.";
    return;
  }

  if (res != CURLE_OK) {
    size_t len = strlen(errbuf);
    if (len > 0) {
      LOG(ERROR) << "[REQUESTREADER] " << errbuf;
     } else {
      LOG(ERROR) << "[REQUESTREADER] " << curl_easy_strerror(res);
    }
  }
}


// _____________________________________________________________________________
std::string RequestReader::queryUrl(const std::string& query) const {
  std::string esc = curl_easy_escape(_curl, query.c_str(), query.size());

  return _backendUrl + "/?send=18446744073709551615" +
         "&query=" + esc;
}

// _____________________________________________________________________________
size_t RequestReader::writeCb(void* contents, size_t size, size_t nmemb,
                              void* userp) {
  size_t realsize = size * nmemb;
  try  {
    static_cast<RequestReader*>(userp)->parse(static_cast<const char*>(contents),
                                              realsize);
  } catch(...) {
    static_cast<RequestReader*>(userp)->exceptionPtr = std::current_exception();
    return CURLE_WRITE_ERROR;
  }

  return realsize;
}

// _____________________________________________________________________________
size_t RequestReader::writeCbIds(void* contents, size_t size, size_t nmemb,
                                 void* userp) {
  size_t realsize = size * nmemb;
  try  {
    static_cast<RequestReader*>(userp)->parseIds(
        static_cast<const char*>(contents), realsize);
  } catch(...) {
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
    _curId.bytes[_curByte] = c[i];
    _curByte = (_curByte + 1) % 8;

    if (_curByte == 0) {
      ids.push_back({_curId.val, ids.size()});
    }
  }
}

// _____________________________________________________________________________
void RequestReader::parse(const char* c, size_t size) {
  // TODO: just a rough approximation
  checkMem(size, _maxMemory);

  const char* start = c;
  while (c < start + size) {
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
          cols.push_back({_colNames[_curCol], _dangling});

          if (*c == '\n') {
            _curRow++;
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
