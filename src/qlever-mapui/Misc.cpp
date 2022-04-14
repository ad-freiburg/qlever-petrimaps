// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <string>
#include <vector>
#include <stdint.h>
#include "qlever-mapui/Misc.h"
#include "util/log/Log.h"

using mapui::RequestReader;

// _____________________________________________________________________________
void RequestReader::requestIds(const std::string& query) {
  if (_curl) {
    auto url = queryUrl(query);
    curl_easy_setopt(_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, RequestReader::writeCbIds);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, this);

    // set headers
    struct curl_slist* headers = 0;
    headers = curl_slist_append(headers, "Accept: application/octet-stream");
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, headers);

    // accept any compression supported
    curl_easy_setopt(_curl, CURLOPT_ACCEPT_ENCODING, "");
    curl_easy_perform(_curl);
  }
}

// _____________________________________________________________________________
void RequestReader::requestRows(const std::string& query) {
  if (_curl) {
    auto url = queryUrl(query);
    curl_easy_setopt(_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, RequestReader::writeCb);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, this);

    // set headers
    struct curl_slist* headers = 0;
    headers = curl_slist_append(headers, "Accept: text/tab-separated-values");
    curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, headers);

    // accept any compression supported
    curl_easy_setopt(_curl, CURLOPT_ACCEPT_ENCODING, "");
    curl_easy_perform(_curl);
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
  static_cast<RequestReader*>(userp)->parse(static_cast<const char*>(contents),
                                            realsize);
  return realsize;
}

// _____________________________________________________________________________
size_t RequestReader::writeCbIds(void* contents, size_t size, size_t nmemb,
                                 void* userp) {
  size_t realsize = size * nmemb;
  static_cast<RequestReader*>(userp)->parseIds(
      static_cast<const char*>(contents), realsize);
  return realsize;
}

// _____________________________________________________________________________
void RequestReader::parseIds(const char* c, size_t size) {
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
          _dangling += *c;
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
