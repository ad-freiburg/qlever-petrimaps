// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <regex>

#include "SPARQLRequestor.h"

using petrimaps::SPARQLRequestor;

// _____________________________________________________________________________
void SPARQLRequestor::request(const std::string& query) {
  std::lock_guard<std::mutex> guard(_m);

  if (_ready) {
    // nothing to do
    return;
  }
  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }

  _query = query;
  _ready = false;
  _objects.clear();
  _clusterObjects.clear();

  RequestReader reader(_cache->getBackendURL(), _maxMemory);

  LOG(INFO) << "[REQUESTOR] Requesting IDs for query " << query;
  reader.requestIds(prepQuery(query));

  LOG(INFO) << "[REQUESTOR] Done, have " << reader.ids.size()
            << " ids in total.";

  // join with geoms from GeomCache

  // sort by qlever id
  LOG(INFO) << "[REQUESTOR] Sorting results by qlever ID...";
  std::sort(reader.ids.begin(), reader.ids.end());
  LOG(INFO) << "[REQUESTOR] ... done";

  LOG(INFO) << "[REQUESTOR] Retrieving geoms from cache...";

  // (geom id, result row)
  const auto& ret = _cache->getRelObjects(reader.ids);
  _objects = ret.first;
  _numObjects = ret.second;
  LOG(INFO) << "[REQUESTOR] ... done, got " << _objects.size() << " objects.";

  LOG(INFO) << "[REQUESTOR] Calculating bounding box of result...";

  util::geo::FBox pointBbox;
  util::geo::DBox lineBbox;
  createBboxes(pointBbox, lineBbox);

  LOG(INFO) << "[REQUESTOR] ... done";
  LOG(INFO) << "[REQUESTOR] Point BBox: " << util::geo::getWKT(pointBbox);
  LOG(INFO) << "[REQUESTOR] Line BBox: " << util::geo::getWKT(lineBbox);
  LOG(INFO) << "[REQUESTOR] Building grid...";

  createGrid(pointBbox, lineBbox);

  _ready = true;

  LOG(INFO) << "[REQUESTOR] ...done";
}

// _____________________________________________________________________________
std::vector<std::pair<std::string, std::string>> SPARQLRequestor::requestRow(uint64_t row) const {
  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }

  RequestReader reader(_cache->getBackendURL(), _maxMemory);
  LOG(INFO) << "[REQUESTOR] Requesting single row " << row << " for query "
            << _query;
  auto query = prepQueryRow(_query, row);

  LOG(INFO) << "[REQUESTOR] Row query is " << query;

  reader.requestRows(query);
  if (reader.rows.size() == 0) return {};
  return reader.rows[0];
}

// _____________________________________________________________________________
void SPARQLRequestor::requestRows(std::function<void(std::vector<std::vector<std::pair<std::string, std::string>>>)> cb) const {
  if (!_cache->ready()) {
    throw std::runtime_error("Geom cache not ready");
  }
  RequestReader reader(_cache->getBackendURL(), _maxMemory);
  LOG(INFO) << "[REQUESTOR] Requesting rows for query " << _query;

  ReaderCbPair cbPair{&reader, cb};

  reader.requestRows(
      _query,
      [](void* contents, size_t size, size_t nmemb, void* ptr) {
        size_t realsize = size * nmemb;
        auto pr = static_cast<ReaderCbPair*>(ptr);
        try {
          // clear rows
          pr->reader->rows = {};
          pr->reader->parse(static_cast<const char*>(contents), realsize);
          pr->cb(pr->reader->rows);
        } catch (...) {
          pr->reader->exceptionPtr = std::current_exception();
          return static_cast<size_t>(CURLE_WRITE_ERROR);
        }

        return realsize;
      },
      &cbPair);
}

// _____________________________________________________________________________
std::string SPARQLRequestor::prepQuery(std::string query) const {
  // only use last column
  std::regex expr("select[^{]*(\\?[A-Z0-9_\\-+]*)+[^{]*\\s*\\{",
                  std::regex_constants::icase);

  // only remove columns the first (=outer) SELECT statement
  query = std::regex_replace(query, expr, "SELECT $1 WHERE {$&",
                             std::regex_constants::format_first_only) + "}";

  if (util::toLower(query).find("limit") == std::string::npos) {
    query += " LIMIT 18446744073709551615";
  }

  return query;
}

// _____________________________________________________________________________
std::string SPARQLRequestor::prepQueryRow(std::string query, uint64_t row) const {
  // replace first select
  std::regex expr("select[^{]*\\?[A-Z0-9_\\-+]*+[^{]*\\s*\\{",
                  std::regex_constants::icase);

  query = std::regex_replace(query, expr, "SELECT * {$&",
                             std::regex_constants::format_first_only) + "}";
  query += "OFFSET " + std::to_string(row) + " LIMIT 1";
  return query;
}