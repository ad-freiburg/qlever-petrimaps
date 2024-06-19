// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_SERVER_GEOJSONREQUESTOR_H_
#define PETRIMAPS_SERVER_GEOJSONREQUESTOR_H_

#include "qlever-petrimaps/server/Requestor.h"
#include "qlever-petrimaps/GeoJSONCache.h"

namespace petrimaps {

class GeoJSONRequestor : public Requestor {
 public:
   GeoJSONRequestor() {
      _maxMemory = -1;
   };
   GeoJSONRequestor(std::shared_ptr<const GeoJSONCache> cache, size_t maxMemory) {
      Requestor::_cache = cache;
      _cache = cache;
      _maxMemory = maxMemory;
      _createdAt = std::chrono::system_clock::now();
   };

   void request();
   std::vector<std::pair<std::string, std::string>> requestRow(uint64_t row) const;
   void requestRows(std::function<void(std::vector<std::vector<std::pair<std::string, std::string>>>)> cb) const;
 
 private:
   std::shared_ptr<const GeoJSONCache> _cache;

 protected:
    
};
} // namespace petrimaps

#endif  // PETRIMAPS_SERVER_GEOJSONREQUESTOR_H_