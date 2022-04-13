// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <curl/curl.h>
#include <stdint.h>
#include <string>
#include <vector>

#ifndef MAPUI_MISC_H_
#define MAPUI_MISC_H_

const static size_t I_OFFSET = 4611686018427387904;

namespace mapui {

enum ParseState { IN_HEADER, IN_ROW };

union ID {
  uint64_t val;
  uint8_t bytes[8];
};

struct RequestReader {
  explicit RequestReader(const std::string& backendUrl)
      : _backendUrl(backendUrl), _curl(curl_easy_init()) {}
  ~RequestReader() {
    if (_curl) curl_easy_cleanup(_curl);
  }

  void requestIds(const std::string& qurl);
  void requestRows(const std::string& qurl);
  void parse(const char*, size_t size);
  void parseIds(const char*, size_t size);

  static size_t writeCb(void* contents, size_t size, size_t nmemb, void* userp);
  static size_t writeCbIds(void* contents, size_t size, size_t nmemb,
                           void* userp);

  std::string queryUrl(const std::string& query) const;

  std::string _backendUrl;
  CURL* _curl;

  std::vector<std::string> _colNames;
  size_t _curCol = 0;
  size_t _curRow = 0;

  std::string _dangling;
  ParseState _state = IN_HEADER;

  std::vector<std::pair<std::string, std::string>> cols;

  uint8_t _curByte = 0;
  ID _curId;
  size_t _received = 0;
  std::vector<std::pair<uint64_t, uint64_t>> ids;
};

}  // namespace mapui

#endif  // MAPUI_MISC_H_
