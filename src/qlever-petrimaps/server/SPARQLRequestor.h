// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_SERVER_SPARQLREQUESTOR_H_
#define PETRIMAPS_SERVER_SPARQLREQUESTOR_H_

#include "qlever-petrimaps/server/Requestor.h"
#include "qlever-petrimaps/SPARQLCache.h"

namespace petrimaps {

struct ReaderCbPair {
  RequestReader* reader;
  std::function<void(
      std::vector<std::vector<std::pair<std::string, std::string>>>)>
      cb;
};

class SPARQLRequestor : public Requestor {
 public:
    SPARQLRequestor() {
      _maxMemory = -1;
    };
    SPARQLRequestor(std::shared_ptr<const SPARQLCache> cache, size_t maxMemory) {
      Requestor::_cache = cache;
      _cache = cache;
      _maxMemory = maxMemory;
      _createdAt = std::chrono::system_clock::now();
    };

    void request(const std::string& query);
    std::vector<std::pair<std::string, std::string>> requestRow(uint64_t row) const;

    void requestRows(
      std::function<
          void(std::vector<std::vector<std::pair<std::string, std::string>>>)>
          cb) const;
 
 private:
    std::string _backendUrl;
    std::shared_ptr<const SPARQLCache> _cache;
    std::string _query;

    std::string prepQuery(std::string query) const;
    std::string prepQueryRow(std::string query, uint64_t row) const;

 protected:
    
};
} // namespace petrimaps

#endif  // PETRIMAPS_SERVER_SPARQLREQUESTOR_H_