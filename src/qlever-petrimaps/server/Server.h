// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_SERVER_SERVER_H_
#define PETRIMAPS_SERVER_SERVER_H_

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <png.h>
#include "qlever-petrimaps/GeomCache.h"
#include "qlever-petrimaps/server/Requestor.h"
#include "util/http/Server.h"

namespace petrimaps {

typedef std::map<std::string, std::string> Params;

enum MapStyle { HEATMAP, OBJECTS };

class Server : public util::http::Handler {
 public:
  explicit Server(size_t maxMemory, const std::string& cacheDir,
                  int cacheLifetime, size_t autoThreshold);

  virtual util::http::Answer handle(const util::http::Req& request,
                                    int connection) const;

 private:
  static std::string parseUrl(std::string u, std::string pl, Params* params);

  util::http::Answer handleHeatMapReq(const Params& pars, int sock) const;
  util::http::Answer handleQueryReq(const Params& pars) const;
  util::http::Answer handleGeoJSONReq(const Params& pars) const;
  util::http::Answer handleClearSessReq(const Params& pars) const;
  util::http::Answer handlePosReq(const Params& pars) const;
  util::http::Answer handleLoadReq(const Params& pars) const;

  util::http::Answer handleExportReq(const Params& pars, int sock) const;
  util::http::Answer handleLoadStatusReq(const Params& pars) const;

  void createCache(const std::string& backend) const;
  std::string loadCache(const std::string& backend) const;

  void clearSession(const std::string& id) const;
  void clearSessions() const;
  void clearOldSessions() const;

  std::string getSessionId() const;

  double getLoadStatusPercent() const;

  static void pngWriteRowCb(png_structp png_ptr, png_uint_32 row, int pass);
  void writePNG(const unsigned char* data, size_t w, size_t h, int sock) const;

  void drawPoint(std::vector<uint32_t>& points, std::vector<double>& points2,
                 int px, int py, int w, int h, MapStyle style,
                 size_t num) const;
  void drawLine(unsigned char* image, int x0, int y0, int x1, int y1, int w,
                int h) const;

  size_t _maxMemory;

  std::string _cacheDir;

  int _cacheLifetime;
  size_t _autoThreshold;

  // Load Status
  mutable size_t _totalSize = 0;

  mutable std::mutex _m;

  mutable std::map<std::string, std::shared_ptr<GeomCache>> _caches;
  mutable std::map<std::string, std::shared_ptr<Requestor>> _rs;
  mutable std::map<std::string, std::string> _queryCache;
};
}  // namespace petrimaps

#endif  // PETRIMAPS_SERVER_SERVER_H_
