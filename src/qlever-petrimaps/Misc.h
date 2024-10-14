// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <curl/curl.h>
#include <stdint.h>

#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "util/Misc.h"

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

class OutOfMemoryError : public std::exception {
 public:
  explicit OutOfMemoryError(size_t want, size_t have, size_t max) {
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

inline void checkMem(size_t want, size_t max) {
  size_t currentSize = util::getCurrentRSS();

  if (currentSize + want > max) {
    throw OutOfMemoryError(want, currentSize, max);
  }
}

struct RequestReader {
  explicit RequestReader(const std::string& backendUrl, size_t maxMemory)
      : _backendUrl(backendUrl),
        _curl(curl_easy_init()),
        _maxMemory(maxMemory) {}
  ~RequestReader() {
    if (_curl) curl_easy_cleanup(_curl);
  }

  std::vector<std::string> requestColumns(const std::string& query);
  void requestIds(const std::string& qurl);
  void requestRows(const std::string& qurl);
  void requestRows(const std::string& query,
                   size_t (*writeCb)(void*, size_t, size_t, void*), void* ptr);
  void parse(const char*, size_t size);
  void parseIds(const char*, size_t size);

  static size_t writeStringCb(void* contents, size_t size, size_t nmemb,
                              void* userp);
  static size_t writeCb(void* contents, size_t size, size_t nmemb, void* userp);
  static size_t writeCbIds(void* contents, size_t size, size_t nmemb,
                           void* userp);

  std::string queryUrl(const std::string& query) const;

  std::string _backendUrl;
  CURL* _curl;

  std::vector<std::string> _colNames;
  size_t _curCol = 0;
  size_t _curRow = 0;

  std::string _dangling, _raw;
  ParseState _state = IN_HEADER;

  std::vector<std::vector<std::pair<std::string, std::string>>> rows;
  std::vector<std::pair<std::string, std::string>> curCols;

  uint8_t _curByte = 0;
  ID _curId;
  size_t _received = 0;
  std::vector<IdMapping> _ids;
  size_t _maxMemory;
  std::exception_ptr exceptionPtr;
};

}  // namespace petrimaps

#endif  // PETRIMAPS_MISC_H_
