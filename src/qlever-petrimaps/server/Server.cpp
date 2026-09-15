// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <png.h>
#include <sys/socket.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <codecvt>
#include <csignal>
#include <locale>
#include <memory>
#include <random>
#include <regex>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

// clang-format off
#include "3rdparty/heatmap.h"
#include "3rdparty/colorschemes/Blues.h"
#include "3rdparty/colorschemes/Greens.h"
#include "3rdparty/colorschemes/Greys.h"
#include "3rdparty/colorschemes/Oranges.h"
#include "3rdparty/colorschemes/Purples.h"
#include "3rdparty/colorschemes/RdGy.h"
#include "3rdparty/colorschemes/RdYlBu.h"
#include "3rdparty/colorschemes/RdYlGn.h"
#include "3rdparty/colorschemes/Reds.h"
#include "3rdparty/colorschemes/Spectral.h"
#include "3rdparty/colorschemes/YlOrRd.h"
#include "3rdparty/colorschemes/gray.h"
#include "3rdparty/json.hpp"
// clang-format on
#include "qlever-petrimaps/build.h"
#include "qlever-petrimaps/example.h"
#include "qlever-petrimaps/index.h"
#include "qlever-petrimaps/server/RenderContext.h"
#include "qlever-petrimaps/server/Requestor.h"
#include "qlever-petrimaps/server/Server.h"
#include "qlever-petrimaps/style.h"
#include "util/Misc.h"
#include "util/String.h"
#include "util/geo/Geo.h"
#include "util/geo/output/GeoJsonOutput.cpp"
#include "util/http/Server.h"
#include "util/log/Log.h"
#ifdef _OPENMP
#include <omp.h>
#else
#define omp_get_thread_num() 0
#endif

using nlohmann::json;
using petrimaps::GeomCacheConfig;
using petrimaps::Params;
using petrimaps::RequestorConfig;
using petrimaps::Server;
using util::geo::contains;
using util::geo::densify;
using util::geo::DLine;
using util::geo::DPoint;
using util::geo::extendBox;
using util::geo::intersection;
using util::geo::intersects;
using util::geo::latLngToWebMerc;
using util::geo::LineSegment;
using util::geo::webMercToLatLng;

const static double THRESHOLD = 200;
static std::atomic<size_t> _curRow;

// _____________________________________________________________________________
static void sendRaw(int sock, const std::string& buff) {
  // write buff to sock, blocking until everything has been written
  size_t writes = 0;

  while (writes != buff.size()) {
    int64_t out =
        send(sock, buff.c_str() + writes, buff.size() - writes, MSG_NOSIGNAL);
    if (out < 0) {
      if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR) continue;
      throw std::runtime_error("Failed to write to socket");
    }
    writes += out;
  }
}

// _____________________________________________________________________________
static bool printWKTFeature(std::stringstream& ss, const std::string& wkt,
                            const util::json::Val& dict, bool hadFeature) {
  const char* s = wkt.c_str();

  if (*s == '"') s++;  // drop " at beginning

  auto wktType = util::geo::getWKTType(s, &s);

  if (wktType == util::geo::WKTType::NONE) return hadFeature;

  if (hadFeature) ss << ",";

  GeoJsonOutput geoJsonOut(ss, true);

  if (wktType == util::geo::WKTType::POLYGON) {
    geoJsonOut.print(util::geo::polygonFromWKT<double>(s, 0), dict);
  }
  if (wktType == util::geo::WKTType::MULTIPOLYGON) {
    geoJsonOut.print(util::geo::multiPolygonFromWKT<double>(s, 0), dict);
  }
  if (wktType == util::geo::WKTType::POINT) {
    geoJsonOut.print(util::geo::pointFromWKT<double>(s, 0), dict);
  }
  if (wktType == util::geo::WKTType::MULTIPOINT) {
    geoJsonOut.print(util::geo::multiPointFromWKT<double>(s, 0), dict);
  }
  if (wktType == util::geo::WKTType::LINESTRING) {
    geoJsonOut.print(util::geo::lineFromWKT<double>(s, 0), dict);
  }
  if (wktType == util::geo::WKTType::MULTILINESTRING) {
    geoJsonOut.print(util::geo::multiLineFromWKT<double>(s, 0), dict);
  }
  if (wktType == util::geo::WKTType::COLLECTION) {
    geoJsonOut.print(util::geo::collectionFromWKT<double>(s, 0), dict);
  }

  return true;
}

// _____________________________________________________________________________
Server::Server(size_t maxMemory, const std::string& cacheDir, int cacheLifetime,
               size_t autoThreshold,
               std::map<std::string, GeomCacheConfig> cacheConfigs,
               const std::string& accessToken)
    : _maxMemory(maxMemory),
      _cacheDir(cacheDir),
      _cacheLifetime(cacheLifetime),
      _autoThreshold(autoThreshold),
      _cacheConfigs(cacheConfigs),
      _accessToken(accessToken) {
  std::thread t(&Server::clearOldSessions, this);
  t.detach();
}

// _____________________________________________________________________________
util::http::Answer Server::handle(const util::http::Req& req, int con) const {
  // ignore SIGPIPE
  signal(SIGPIPE, SIG_IGN);

  util::http::Answer a;
  try {
    Params params;
    auto cmd = parseUrl(req.url, req.payload, &params);

    if (cmd == "/") {
      a = handleIndexReq(params, con);
    } else if (cmd == "/example") {
      a = handleExamplePageReq(params, con);
    } else if (cmd == "/touch") {
      a = handleTouchReq(params, req.params, con);
    } else if (cmd == "/query") {
      LOG(INFO) << "Query request from " << remoteAddress(con, req.params);
      a = handleQueryReq(params, req.params, con);
    } else if (cmd == "/clearsession") {
      a = handleClearSessReq(params, req.params, con);
    } else if (cmd == "/clearsessions") {
      a = handleClearSessReq(params, req.params, con);
    } else if (cmd == "/loadstatus") {
      a = handleLoadStatusReq(params, req.params, con);
    } else if (cmd == "/build.js") {
      a = util::http::Answer(
          "200 OK", std::string(build_js, build_js + sizeof build_js /
                                                         sizeof build_js[0]));
      a.params["Content-Type"] = "application/javascript; charset=utf-8";
      a.params["Cache-Control"] = "public, max-age=10000";
    } else if (cmd == "/build.css") {
      a = util::http::Answer(
          "200 OK",
          std::string(build_css,
                      build_css + sizeof build_css / sizeof build_css[0]));
      a.params["Content-Type"] = "text/css; charset=utf-8";
      a.params["Cache-Control"] = "public, max-age=10000";
    } else if (cmd == "/heatmap") {
      a = handleHeatMapReq(params, con);
    } else if (cmd.find("/tms/") == 0) {
      std::string tmsPath = cmd.substr(5);
      auto parts = util::split(tmsPath, '/');

      if (parts.size() != 5) {
        throw std::invalid_argument("Invalid TMS request.");
      }
      if (parts[4].size() < 5 ||
          parts[4].substr(parts[4].size() - 4) != ".png") {
        throw std::invalid_argument("Invalid TMS request.");
      }

      params["layers"] = parts[0];
      params["styles"] = parts[1];
      params["x"] = parts[2];
      params["y"] = parts[3];
      params["z"] = parts[4].substr(0, parts[4].size() - 4);

      a = handleTMSReq(params, con);
    } else if (cmd == "/wmts") {
      a = handleWMTSReq(params, con);
    } else if (cmd == "/wfs") {
      a = handleWFSReq(params, req.params, con);
    } else if (cmd.find("/wmts/") == 0) {
      std::string wmtsPath = cmd.substr(6);
      auto parts = util::split(wmtsPath, '/');

      if (parts.size() != 6) {
        throw std::invalid_argument("Invalid RESTful WMTS request.");
      }
      if (parts[5].size() < 5 ||
          parts[5].substr(parts[5].size() - 4) != ".png") {
        throw std::invalid_argument("Invalid RESTful WMTS request.");
      }

      params["service"] = "wmts";
      params["request"] = "gettile";
      params["version"] = "1.0.0";
      params["layer"] = parts[0];
      params["style"] = parts[1];
      params["format"] = "image/png";
      params["tilematrixset"] = parts[2];
      params["tilematrix"] = parts[3];
      params["tilerow"] = parts[4];
      params["tilecol"] = parts[5].substr(0, parts[5].size() - 4);

      a = handleWMTSGetTileReq(params, con);
    } else {
      a = util::http::Answer("404 Not Found", "dunno");
    }
  } catch (const std::runtime_error& e) {
    a = util::http::Answer("400 Bad Request", e.what());
    LOG(ERROR) << e.what();
  } catch (const std::invalid_argument& e) {
    a = util::http::Answer("400 Bad Request", e.what());
    LOG(ERROR) << e.what();
  } catch (const OutOfMemoryError& e) {
    a = util::http::Answer("507 Insufficient Storage", e.what());
    LOG(ERROR) << e.what();
  } catch (const std::exception& e) {
    a = util::http::Answer("500 Internal Server Error", e.what());
    LOG(ERROR) << e.what();
  } catch (...) {
    a = util::http::Answer("500 Internal Server Error",
                           "Internal Server Error.");
    LOG(ERROR) << "Unknown failure occured.";
  }

  a.params["Access-Control-Allow-Origin"] = "*";
  a.params["Server"] = "qlever-petrimaps";

  return a;
}

// _____________________________________________________________________________
util::http::Answer Server::handleHeatMapReq(const Params& pars,
                                            int sock) const {
  // ignore SIGPIPE
  signal(SIGPIPE, SIG_IGN);

  if (pars.count("width") == 0 || pars.find("width")->second.empty())
    throw std::invalid_argument("No width (?width=) specified.");
  if (pars.count("height") == 0 || pars.find("height")->second.empty())
    throw std::invalid_argument("No height (?height=) specified.");

  if (pars.count("bbox") == 0 || pars.find("bbox")->second.empty())
    throw std::invalid_argument("No bbox specified.");
  auto box = util::split(pars.find("bbox")->second, ',');

  if (pars.count("layers") == 0 || pars.find("layers")->second.empty())
    throw std::invalid_argument("No layer specified.");
  std::string layersPar = pars.find("layers")->second;

  std::string sessionId;
  std::string geomField;

  auto layers = util::split(layersPar, ',');
  if (layers.size() > 1)
    throw std::invalid_argument("Multiple layers not supported");
  if (layers.size() == 0)
    throw std::invalid_argument("No layer specified");

  if (layers.size()) {
    auto parts = util::split(layers[0], ':');
    if (parts.size() != 2)
      throw std::invalid_argument("Invalid layer '" + layers[0] + "' specified");
    sessionId = parts[0];
    geomField = parts[1];
  }

  MapStyle style = HEATMAP;
  auto colorScheme = heatmap_cs_Spectral_mixed_exp;

  int objColorR = 0, objColorG = 0, objColorB = 0;

  LayerConfig lcfg;
  size_t lid = 0;

  std::shared_ptr<Requestor> r;
  {
    std::lock_guard<std::mutex> guard(_m);
    bool has = _rs.count(sessionId);
    if (!has) {
      LOG(ERROR) << "Session " << sessionId << " not found!";
      throw std::invalid_argument("Session not found");
    }
    r = _rs[sessionId];
  }

  if (!r->ready()) {
    LOG(ERROR) << "Session " << sessionId << " not ready!";
    throw std::invalid_argument("Session not ready.");
  }

  if (pars.count("styles") != 0 && !pars.find("styles")->second.empty()) {
    auto styleId = pars.find("styles")->second;
    lid = r->getLidById(styleId);
  }

  if (box.size() != 4) throw std::invalid_argument("Invalid request.");

  LOG(INFO) << "[SERVER] Begin heatmap generation for session " << sessionId << " on geom field " << geomField;;

  double x1 = std::atof(box[0].c_str());
  double y1 = std::atof(box[1].c_str());
  double x2 = std::atof(box[2].c_str());
  double y2 = std::atof(box[3].c_str());

  double mercW = fabs(x2 - x1);
  double mercH = fabs(y2 - y1);

  auto bbox = DBox({x1, y1}, {x2, y2});
  auto fbbox = FBox({x1, y1}, {x2, y2});

  double orx = bbox.getLowerLeft().getX();
  double ory = bbox.getLowerLeft().getY();

  int w = atoi(pars.find("width")->second.c_str());
  int h = atoi(pars.find("height")->second.c_str());

  if (w <= 0 || w > 3000) throw std::invalid_argument("Invalid request");
  if (h <= 0 || h > 3000) throw std::invalid_argument("Invalid request");

  double res = mercH / h;

  lcfg = r->getLayers()[lid];

  if (lcfg.geomField != geomField) throw std::invalid_argument("Style not defined for geom field '" + geomField + "', but for '" + lcfg.geomField + "'");

  if (lcfg.style == "objects") style = OBJECTS;
  if (lcfg.style == "raster") style = RASTER;
  if (lcfg.style == "auto" && res < THRESHOLD &&
      r->getNumObjects(lid) > _autoThreshold)
    style = OBJECTS;

  if (style == OBJECTS) {
    if (lcfg.color.size() == 6) {
      objColorR = hexToInt(lcfg.color[0]) * 16 + hexToInt(lcfg.color[1]);
      objColorG = hexToInt(lcfg.color[2]) * 16 + hexToInt(lcfg.color[3]);
      objColorB = hexToInt(lcfg.color[4]) * 16 + hexToInt(lcfg.color[5]);
    }
  }

  if (style == HEATMAP) {
    if (lcfg.colorscheme == "spectralexp")
      colorScheme = heatmap_cs_Spectral_mixed_exp;
    if (lcfg.colorscheme == "spectral") colorScheme = heatmap_cs_Spectral_mixed;
    if (lcfg.colorscheme == "RdYlGn") colorScheme = heatmap_cs_RdYlGn_mixed;
    if (lcfg.colorscheme == "RdYlGnexp")
      colorScheme = heatmap_cs_RdYlGn_mixed_exp;
    if (lcfg.colorscheme == "w2b") colorScheme = heatmap_cs_w2b_opaque;
    if (lcfg.colorscheme == "b2w") colorScheme = heatmap_cs_b2w_opaque;
    if (lcfg.colorscheme == "RdYlBu") colorScheme = heatmap_cs_RdYlBu_mixed;
    if (lcfg.colorscheme == "RdGy") colorScheme = heatmap_cs_RdGy_mixed;
    if (lcfg.colorscheme == "YlOrRd") colorScheme = heatmap_cs_YlOrRd_mixed;
    if (lcfg.colorscheme == "Blues") colorScheme = heatmap_cs_Blues_mixed;
    if (lcfg.colorscheme == "Greens") colorScheme = heatmap_cs_Greens_mixed;
    if (lcfg.colorscheme == "Greys") colorScheme = heatmap_cs_Greys_mixed;
    if (lcfg.colorscheme == "Oranges") colorScheme = heatmap_cs_Oranges_mixed;
    if (lcfg.colorscheme == "Reds") colorScheme = heatmap_cs_Reds_mixed;

    if (lcfg.colorscheme == "RdYlBuexp")
      colorScheme = heatmap_cs_RdYlBu_mixed_exp;
    if (lcfg.colorscheme == "RdGyexp") colorScheme = heatmap_cs_RdGy_mixed_exp;
    if (lcfg.colorscheme == "YlOrRdexp")
      colorScheme = heatmap_cs_YlOrRd_mixed_exp;
    if (lcfg.colorscheme == "Bluesexp")
      colorScheme = heatmap_cs_Blues_mixed_exp;
    if (lcfg.colorscheme == "Greensexp")
      colorScheme = heatmap_cs_Greens_mixed_exp;
    if (lcfg.colorscheme == "Greysexp")
      colorScheme = heatmap_cs_Greys_mixed_exp;
    if (lcfg.colorscheme == "Orangesexp")
      colorScheme = heatmap_cs_Oranges_mixed_exp;
    if (lcfg.colorscheme == "Redsexp") colorScheme = heatmap_cs_Reds_mixed_exp;
  }

  if (style == RASTER) {
    if (lcfg.colorscheme == "spectral")
      colorScheme = heatmap_cs_Spectral_discrete;
    if (lcfg.colorscheme == "RdYlGn") colorScheme = heatmap_cs_RdYlGn_discrete;
    if (lcfg.colorscheme == "RdYlBu") colorScheme = heatmap_cs_RdYlBu_discrete;
    if (lcfg.colorscheme == "RdGy") colorScheme = heatmap_cs_RdGy_discrete;
    if (lcfg.colorscheme == "YlOrRd") colorScheme = heatmap_cs_YlOrRd_discrete;
    if (lcfg.colorscheme == "Blues") colorScheme = heatmap_cs_Blues_discrete;
    if (lcfg.colorscheme == "Greens") colorScheme = heatmap_cs_Greens_discrete;
    if (lcfg.colorscheme == "Greys") colorScheme = heatmap_cs_Greys_discrete;
    if (lcfg.colorscheme == "Oranges")
      colorScheme = heatmap_cs_Oranges_discrete;
    if (lcfg.colorscheme == "Reds") colorScheme = heatmap_cs_Reds_discrete;
  }

  checkMem(sizeof(float) * w * h, _maxMemory);
  double realCellSize = r->getPointGrid(lid).getCellWidth();
  double virtCellSize = res * 1.5;

  size_t NUM_THREADS = std::thread::hardware_concurrency();

  size_t subCellSize = (size_t)ceil(realCellSize / virtCellSize);

  LOG(INFO) << "[SERVER] Query resolution: " << res;
  LOG(INFO) << "[SERVER] Virt cell size: " << virtCellSize;
  LOG(INFO) << "[SERVER] Num virt cells: " << subCellSize * subCellSize;

  checkMem(sizeof(unsigned char) * w * h * 4 +
               sizeof(unsigned char) * w * h * 4 * NUM_THREADS * 2,
           _maxMemory);
  RenderContext rcontext(w, h, orx, ory, mercW, mercH, style, lcfg.objectStyle,
                         NUM_THREADS);

  // POINTS
  if (intersects(r->getPointGrid(lid).getBBox(), fbbox)) {
    LOG(INFO) << "[SERVER] Looking up display points...";
    if (res < THRESHOLD) {
      std::vector<ID_TYPE> ret;

      // duplicates are not possible with points, so no sorting here
      r->getPointGrid(lid).get(fbbox, &ret);

      for (size_t j = 0; j < ret.size(); j++) {
        size_t oid = ret[j];

        if (r->isCluster(lid, oid) && style == OBJECTS) {
          size_t refOid = r->getCluster(lid, oid).first;

          FPoint p = r->getPoint(lid, refOid);
          if (!contains(p, fbbox)) continue;

          const auto& cp = r->clusterGeom(lid, oid, res);

          auto px = RenderContext::mercToPx(cp, orx, ory, mercW, mercH, w, h);
          auto ppx = RenderContext::mercToPx(p, orx, ory, mercW, mercH, w, h);

          rcontext.drawPoint(0, px.getX(), px.getY(), r->getVal(lid, oid), 0,
                             0);
          rcontext.drawLineSegment(px.getX(), px.getY(), ppx.getX(), ppx.getY(),
                                   w, h);
        } else {
          if (r->isCluster(lid, oid)) oid = r->getCluster(lid, oid).first;

          FPoint p = r->getPoint(lid, oid);
          if (!contains(p, fbbox)) continue;

          auto px = RenderContext::mercToPx(p, orx, ory, mercW, mercH, w, h);

          if (style == RASTER) {
            auto rasterMeta =
                r->getRasterMetas(lid, oid);
            rcontext.drawPoint(0, px.getX(), px.getY(), r->getVal(lid, oid),
                               rasterMeta.first, rasterMeta.second);
          } else {
            rcontext.drawPoint(0, px.getX(), px.getY(), r->getVal(lid, oid), 0,
                               0);
          }
        }
      }
    } else {
      // they intersect, we checked this above
      auto iBox = intersection(r->getPointGrid(lid).getBBox(), fbbox);
      const auto& grid = r->getPointGrid(lid);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
      for (size_t x = grid.getCellXFromX(iBox.getLowerLeft().getX());
           x <= grid.getCellXFromX(iBox.getUpperRight().getX()); x++) {
        for (size_t y = grid.getCellYFromY(iBox.getLowerLeft().getY());
             y <= grid.getCellYFromY(iBox.getUpperRight().getY()); y++) {
          if (x >= grid.getXWidth() || y >= grid.getYHeight()) continue;
          size_t tid = omp_get_thread_num();

          auto cell = grid.getCell(x, y);
          if (!cell || cell->size() == 0) continue;
          const auto& cellBox = grid.getBox(x, y);

          if (subCellSize == 1) {
            auto px = RenderContext::mercToPx(cellBox.getLowerLeft(), orx, ory,
                                              mercW, mercH, w, h);

            // TODO: just setting rasterWidth to 1x1 here is not correct
            rcontext.drawPoint(tid, px.getX(), px.getY(), grid.getCellSum(x, y),
                               1, 1);
          } else {
            for (auto oid : *cell) {
              if (r->isCluster(lid, oid)) oid = r->getCluster(lid, oid).first;

              FPoint p = r->getPoint(lid, oid);
              auto px =
                  RenderContext::mercToPx(p, orx, ory, mercW, mercH, w, h);

              if (style == RASTER) {
                auto rasterMeta =
                    r->getRasterMetas(lid, oid);
                rcontext.drawPoint(tid, px.getX(), px.getY(),
                                   r->getVal(lid, oid), rasterMeta.first,
                                   rasterMeta.second);
              } else {
                rcontext.drawPoint(tid, px.getX(), px.getY(),
                                   r->getVal(lid, oid), 0, 0);
              }
            }
          }
        }
      }
    }
  }

  // LINES
  const auto& lgrid = r->getLineGrid(lid);

  if (intersects(lgrid.getBBox(), fbbox)) {
    LOG(INFO) << "[SERVER] Looking up display lines...";
    if (res < THRESHOLD) {
      std::vector<ID_TYPE> ret;

      // retrieve line points
      lgrid.get(fbbox, &ret);

      // sort to avoid duplicates
      std::sort(ret.begin(), ret.end());

      for (size_t idx = 0; idx < ret.size(); idx++) {
        if (idx > 0 && ret[idx] == ret[idx - 1]) continue;
        auto lineId = r->getObjects(lid)[ret[idx]].first;
        auto oid = r->getObjects(lid)[ret[idx]].second;
        if (!util::geo::intersects(r->getLineBBox(lineId - I_OFFSET), bbox))
          continue;

        if (r->isArea(lineId - I_OFFSET) &&
            !r->isInnerArea(lineId - I_OFFSET)) {
          rcontext.drawArea(0, r->extractLineGeom(lineId - I_OFFSET, 3 * res),
                            r->getVal(lid, oid));
        } else if (r->isArea(lineId - I_OFFSET) &&
                   r->isInnerArea(lineId - I_OFFSET)) {
          rcontext.drawArea(0, r->extractLineGeom(lineId - I_OFFSET, 3 * res),
                            r->getVal(lid, oid), true, true);
        } else {
          if (!r->lineIntersects(lineId, bbox)) continue;
          rcontext.drawLine(0, r->extractLineGeom(lineId - I_OFFSET, 3 * res),
                            r->getVal(lid, oid));
        }
      }
    } else {
      const auto& lpgrid = r->getLinePointGrid(lid);
      const auto& agrid = r->getAreaGrid(lid);
      auto iBox = intersection(lpgrid.getBBox(), fbbox);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
      for (size_t x = lpgrid.getCellXFromX(iBox.getLowerLeft().getX());
           x <= lpgrid.getCellXFromX(iBox.getUpperRight().getX()); x++) {
        for (size_t y = lpgrid.getCellYFromY(iBox.getLowerLeft().getY());
             y <= lpgrid.getCellYFromY(iBox.getUpperRight().getY()); y++) {
          if (x >= lpgrid.getXWidth() || y >= lpgrid.getYHeight()) continue;
          size_t tid = omp_get_thread_num();

          auto cell = lpgrid.getCell(x, y);
          if (!cell || cell->size() == 0) continue;
          const auto& cellBox = lpgrid.getBox(x, y);

          if (subCellSize == 1) {
            auto pix = RenderContext::mercToPx(cellBox.getLowerLeft(), orx, ory,
                                               mercW, mercH, w, h);
            rcontext.drawLinePoint(tid, pix.getX(), pix.getY(),
                                   lpgrid.getCellSum(x, y), 1,
                                   1);
          } else {
            for (const auto& p : *cell) {
              int px = ((cellBox.getLowerLeft().getX() + p.getX() * 256 -
                         bbox.getLowerLeft().getX()) /
                        mercW) *
                       w;
              int py = h - ((cellBox.getLowerLeft().getY() + p.getY() * 256 -
                             bbox.getLowerLeft().getY()) /
                            mercH) *
                               h;
              rcontext.drawLinePoint(tid, px, py, 1, 1, 1);
            }
          }
        }
      }

      std::vector<ID_TYPE> ret;

      // retrieve very large areas for fill
      agrid.get(fbbox, &ret);

      // sort to avoid duplicates
      std::sort(ret.begin(), ret.end());

      for (size_t idx = 0; idx < ret.size(); idx++) {
        if (idx > 0 && ret[idx] == ret[idx - 1]) continue;
        auto lineId = r->getObjects(lid)[ret[idx]].first;
        auto oid = r->getObjects(lid)[ret[idx]].second;
        auto geom = r->extractLineGeom(lineId - I_OFFSET, res);
        if (r->isInnerArea(lineId - I_OFFSET)) {
          rcontext.drawArea(0, geom, r->getVal(lid, oid), true, true);
        } else {
          rcontext.drawArea(0, geom, r->getVal(lid, oid), true);
        }
      }
    }
  }
  LOG(INFO) << "[SERVER] Adding points to heatmap...";
  heatmap_t* hm = heatmap_new(w, h);
  heatmap_t* hmInterior = heatmap_new(w, h);
  hm->max = r->getValRange(lid).second;

  rcontext.writeHeatmap(hm);

  if (style == OBJECTS) {
    rcontext.writeInteriorObjects(hmInterior);
  }
  LOG(INFO) << "[SERVER] ...done";

  LOG(INFO) << "[SERVER] Rendering heatmap...";

  if (style == RASTER) {
    heatmap_render_to(hm, colorScheme, &rcontext.getImage()[0]);
  } else if (style == OBJECTS) {
    unsigned char fillColors[] = {
        0,         0,
        0,         0,
        0,         0,
        0,         0,
        objColorR, objColorG,
        objColorB, 255 * lcfg.objectStyle.fillOpacity * 0.06,
        objColorR, objColorG,
        objColorB, 255 * lcfg.objectStyle.fillOpacity * 0.12,
        objColorR, objColorG,
        objColorB, 255 * lcfg.objectStyle.fillOpacity * 0.25,
        objColorR, objColorG,
        objColorB, 255 * lcfg.objectStyle.fillOpacity * 0.5,
        objColorR, objColorG,
        objColorB, 255 * lcfg.objectStyle.fillOpacity * 0.65,
        objColorR, objColorG,
        objColorB, 255 * lcfg.objectStyle.fillOpacity * 0.8,
        objColorR, objColorG,
        objColorB, 255 * lcfg.objectStyle.fillOpacity * 0.9,
        objColorR, objColorG,
        objColorB, 255 * lcfg.objectStyle.fillOpacity};
    heatmap_colorscheme_t fillColorScheme = {
        fillColors, sizeof(fillColors) / sizeof(fillColors[0]) / 4};

    unsigned char borderColors2[] = {
        0,         0,
        0,         0,
        0,         0,
        0,         0,
        objColorR, objColorG,
        objColorB, 0,
        objColorR, objColorG,
        objColorB, 0,
        objColorR, objColorG,
        objColorB, 0,
        objColorR, objColorG,
        objColorB, 0,
        objColorR, objColorG,
        objColorB, 0,
        objColorR, objColorG,
        objColorB, 0,
        objColorR, objColorG,
        objColorB, 0,
        objColorR, objColorG,
        objColorB, std::max(1.0, 255 * lcfg.objectStyle.lineOpacity)};
    heatmap_colorscheme_t borderColor2Scheme = {
        borderColors2, sizeof(borderColors2) / sizeof(borderColors2[0]) / 4};

    unsigned char borderColors[] = {
        0,         0,         0,         0,
        objColorR, objColorG, objColorB, 128 * lcfg.objectStyle.lineOpacity,
        objColorR, objColorG, objColorB, 169 * lcfg.objectStyle.lineOpacity,
        objColorR, objColorG, objColorB, 192 * lcfg.objectStyle.lineOpacity,
        objColorR, objColorG, objColorB, 255 * lcfg.objectStyle.lineOpacity,
        objColorR, objColorG, objColorB, 255 * lcfg.objectStyle.lineOpacity,
        objColorR, objColorG, objColorB, 255 * lcfg.objectStyle.lineOpacity,
        objColorR, objColorG, objColorB, 255 * lcfg.objectStyle.lineOpacity,
        objColorR, objColorG, objColorB, 255 * lcfg.objectStyle.lineOpacity,
        objColorR, objColorG, objColorB, 255 * lcfg.objectStyle.lineOpacity};
    heatmap_colorscheme_t borderColorScheme = {
        borderColors, sizeof(borderColors) / sizeof(borderColors[0]) / 4};

    heatmap_render_saturated_to(hm, &borderColorScheme, 1,
                                &rcontext.getImage()[0]);

    heatmap_render_saturated_to(hmInterior, &fillColorScheme, 1,
                                &rcontext.getImage()[0]);

    heatmap_render_saturated_to(hm, &borderColor2Scheme, 1,
                                &rcontext.getImage()[0]);
  } else {
    heatmap_render_to(hm, colorScheme, &rcontext.getImage()[0]);
  }
  heatmap_free(hm);
  heatmap_free(hmInterior);

  LOG(INFO) << "[SERVER] ...done";
  LOG(INFO) << "[SERVER] Generating PNG...";

  auto aw = util::http::Answer("200 OK", "");
  aw.params["Content-Type"] = "image/png";
  aw.params["Content-Encoding"] = "identity";
  aw.params["Server"] = "qlever-petrimaps";
  aw.raw = true;

  // we do not set the Content-Length header here, but serve until
  // we are done. In particular, we do not need to send our data in chunks, as
  // specified by https://www.rfc-editor.org/rfc/rfc7230#section-3.3.3
  // point 7

  std::stringstream ss;

  ss << "HTTP/1.1 " << aw.status << "\r\n";

  for (const auto& kv : aw.params)
    ss << kv.first << ": " << kv.second << "\r\n";

  ss << "\r\n";

  std::string buff = ss.str();

  size_t writes = 0;

  while (writes != buff.size()) {
    int64_t out =
        send(sock, buff.c_str() + writes, buff.size() - writes, MSG_NOSIGNAL);
    if (out < 0) {
      if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR) continue;
      throw std::runtime_error("Failed to write to socket");
    }
    writes += out;
  }

  writePNG(&rcontext.getImage()[0], w, h, sock);

  LOG(INFO) << "[SERVER] ...done";

  return aw;
}
std::string lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return s;
}

// _____________________________________________________________________________
const std::string* getParamCaseInsensitive(const Params& pars,
                                           const std::string& key) {
  for (const auto& entry : pars) {
    if (lower(entry.first) == lower(key)) {
      return &entry.second;
    }
  }
  return nullptr;
}
// _____________________________________________________________________________
util::http::Answer Server::handleWMTSReq(const Params& pars, int sock) const {
  const std::string* requestParam = getParamCaseInsensitive(pars, "request");
  if (requestParam == nullptr || requestParam->empty()) {
    throw std::invalid_argument("No WMTS request specified.");
  }

  std::string request = lower(*requestParam);

  if (request == "gettile") {
    return handleWMTSGetTileReq(pars, sock);
  }

  if (request == "getcapabilities") {
    return handleWMTSGetCapabilitiesReq(pars);
  }

  throw std::invalid_argument("Unsupported WMTS request.");
}
// _____________________________________________________________________________
util::http::Answer Server::handleWFSReq(const Params& pars,
                                        const HeaderParams& headerPars,
                                        int sock) const {
  const std::string* requestParam = getParamCaseInsensitive(pars, "request");
  if (requestParam == nullptr || requestParam->empty()) {
    throw std::invalid_argument("No WFS request specified.");
  }

  std::string request = lower(*requestParam);

  if (request == "getcapabilities") {
    return handleWFSGetCapabilitiesReq(pars);
  }

  if (request == "getfeature") {
    return handleWFSGetFeatureReq(pars, headerPars, sock);
  }

  if (request == "describefeaturetype") {
    return handleWFSDescribeFeatureTypeReq(pars);
  }

  throw std::invalid_argument("Unsupported WFS request.");
}
// _____________________________________________________________________________
util::http::Answer Server::handleWMTSGetTileReq(const Params& pars,
                                                int sock) const {
  UNUSED(sock);

  const std::string* serviceParam = getParamCaseInsensitive(pars, "service");
  if (serviceParam == nullptr || serviceParam->empty()) {
    throw std::invalid_argument("No WMTS service specified.");
  }

  if (lower(*serviceParam) != "wmts") {
    throw std::invalid_argument("Invalid WMTS service.");
  }

  const std::string* versionParam = getParamCaseInsensitive(pars, "version");
  if (versionParam == nullptr || versionParam->empty()) {
    throw std::invalid_argument("No WMTS version specified.");
  }

  if (*versionParam != "1.0.0") {
    throw std::invalid_argument("Unsupported WMTS version.");
  }

  const std::string* layerParam = getParamCaseInsensitive(pars, "layer");
  if (layerParam == nullptr || layerParam->empty()) {
    throw std::invalid_argument("No WMTS layer specified.");
  }

  const std::string* styleParam = getParamCaseInsensitive(pars, "style");
  if (styleParam == nullptr || styleParam->empty()) {
    throw std::invalid_argument("No WMTS style specified.");
  }

  const std::string* formatParam = getParamCaseInsensitive(pars, "format");
  if (formatParam == nullptr || formatParam->empty()) {
    throw std::invalid_argument("No WMTS format specified.");
  }

  if (lower(*formatParam) != "image/png") {
    throw std::invalid_argument("Unsupported WMTS format.");
  }

  const std::string* tileMatrixSetParam =
      getParamCaseInsensitive(pars, "tilematrixset");
  if (tileMatrixSetParam == nullptr || tileMatrixSetParam->empty()) {
    throw std::invalid_argument("No WMTS TileMatrixSet specified.");
  }

  if (lower(*tileMatrixSetParam) != "webmercatorquad") {
    throw std::invalid_argument("Unsupported WMTS TileMatrixSet.");
  }

  const std::string* tileMatrixParam =
      getParamCaseInsensitive(pars, "tilematrix");
  if (tileMatrixParam == nullptr || tileMatrixParam->empty()) {
    throw std::invalid_argument("No WMTS TileMatrix specified.");
  }

  const std::string* tileRowParam = getParamCaseInsensitive(pars, "tilerow");
  if (tileRowParam == nullptr || tileRowParam->empty()) {
    throw std::invalid_argument("No WMTS TileRow specified.");
  }

  const std::string* tileColParam = getParamCaseInsensitive(pars, "tilecol");
  if (tileColParam == nullptr || tileColParam->empty()) {
    throw std::invalid_argument("No WMTS TileCol specified.");
  }

  std::string id = *layerParam;
  std::string styleStr = *styleParam;
  std::string heatLayer = getHeatLayer(id);

  int x = atoi(tileColParam->c_str());
  int y = atoi(tileRowParam->c_str());
  int z = atoi(tileMatrixParam->c_str());

  std::string bbox = getWebMercatorTileBbox(x, y, z);

  Params heatPars = pars;
  heatPars["layers"] = heatLayer;
  heatPars["styles"] = styleStr;
  heatPars["bbox"] = bbox;
  heatPars["width"] = "256";
  heatPars["height"] = "256";

  // tmp: log request parameters
  LOG(INFO) << "[SERVER] WMTS GetTile request: layer=" << id
            << " style=" << styleStr << " tileMatrix=" << z << " tileRow=" << y
            << " tileCol=" << x;
  LOG(INFO) << " bbox=" << bbox;

  return handleHeatMapReq(heatPars, sock);
}

// _____________________________________________________________________________
util::http::Answer Server::handleWMTSGetCapabilitiesReq(
    const Params& pars) const {
  const std::string* serviceParam = getParamCaseInsensitive(pars, "service");
  if (serviceParam == nullptr || serviceParam->empty()) {
    throw std::invalid_argument("No WMTS service specified.");
  }

  if (lower(*serviceParam) != "wmts") {
    throw std::invalid_argument("Invalid WMTS service.");
  }

  const std::string* versionParam = getParamCaseInsensitive(pars, "version");
  if (versionParam == nullptr || versionParam->empty()) {
    throw std::invalid_argument("No WMTS version specified.");
  }

  if (*versionParam != "1.0.0") {
    throw std::invalid_argument("Unsupported WMTS version.");
  }

  const double WEBMERC_MAX = 20037508.342789244;
  const double WEBMERC_MIN = -20037508.342789244;
  const double INITIAL_RESOLUTION = (WEBMERC_MAX - WEBMERC_MIN) / 256.0;
  const int MAX_ZOOM = 30;

  auto formatStyleNumber = [](double value) {
    std::ostringstream out;
    out << value;
    return out.str();
  };

  std::vector<std::pair<std::string, std::vector<std::string>>> wmtsLayers;
  {
    std::lock_guard<std::mutex> guard(_m);

    for (const auto& entry : _rs) {
      const std::string& sessionId = entry.first;
      const auto& reqor = entry.second;

      const auto layers = reqor->getLayers();
      for (const auto& layer : layers) {
        std::string layerId = sessionId + "-" + layer.id;
        std::vector<std::string> styles;

        if (layer.style == "heatmap") {
          styles.push_back("heatmap-" + layer.colorscheme);
        } else if (layer.style == "objects") {
          styles.push_back("objects-" + layer.color);
        } else if (layer.style == "raster") {
          styles.push_back("raster-" + formatStyleNumber(layer.rasterW) + "x" +
                           formatStyleNumber(layer.rasterH) + "-" +
                           layer.colorscheme);
        } else if (layer.style == "auto") {
          styles.push_back("heatmap-" + layer.colorscheme);
          styles.push_back("objects-" + layer.color);
        } else {
          styles.push_back("heatmap-" + layer.colorscheme);
        }

        wmtsLayers.emplace_back(layerId, styles);
      }
    }
  }

  LOG(INFO) << "[SERVER] WMTS GetCapabilities with " << wmtsLayers.size()
            << " layers.";

  std::stringstream xml;

  xml << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
  xml << "<Capabilities "
      << "xmlns=\"http://www.opengis.net/wmts/1.0\" "
      << "xmlns:ows=\"http://www.opengis.net/ows/1.1\" "
      << "xmlns:xlink=\"http://www.w3.org/1999/xlink\" "
      << "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
      << "version=\"1.0.0\">\n ";

  xml << "  <ows:ServiceIdentification>\n"
      << "    <ows:Title>qlever-petrimaps WMTS Service</ows:Title>\n"
      << "    <ows:Abstract>WMTS service for qlever-petrimaps</ows:Abstract>\n"
      << "    <ows:ServiceType>OGC WMTS</ows:ServiceType>\n"
      << "    <ows:ServiceTypeVersion>1.0.0</ows:ServiceTypeVersion>\n"
      << "  </ows:ServiceIdentification>\n";

  xml << "  <ows:OperationsMetadata>\n";
  xml << "    <ows:Operation name=\"GetCapabilities\">\n";
  xml << "      <ows:DCP>\n";
  xml << "        <ows:HTTP>\n";
  xml << "          <ows:Get xlink:href=\"/wmts\" />\n";
  xml << "        </ows:HTTP>\n";
  xml << "      </ows:DCP>\n";
  xml << "    </ows:Operation>\n";
  xml << "    <ows:Operation name=\"GetTile\">\n";
  xml << "      <ows:DCP>\n";
  xml << "        <ows:HTTP>\n";
  xml << "          <ows:Get xlink:href=\"/wmts\" />\n";
  xml << "        </ows:HTTP>\n";
  xml << "      </ows:DCP>\n";
  xml << "    </ows:Operation>\n";
  xml << "  </ows:OperationsMetadata>\n";

  xml << "  <Contents>\n";

  for (const auto& layerEntry : wmtsLayers) {
    const auto& layerId = layerEntry.first;
    const auto& styles = layerEntry.second;

    std::string escapedLayerId = xmlEscape(layerId);
    std::string encodedLayerId = xmlEscape(urlEncode(layerId));

    xml << "    <Layer>\n";
    xml << "      <ows:Title>" << escapedLayerId << "</ows:Title>\n";
    xml << "      <ows:Identifier>" << encodedLayerId << "</ows:Identifier>\n";
    for (size_t i = 0; i < styles.size(); i++) {
      std::string encodedStyle = xmlEscape(urlEncode(styles[i]));
      xml << "      <Style isDefault=\"" << (i == 0 ? "true" : "false")
          << "\">\n";
      xml << "        <ows:Identifier>" << encodedStyle
          << "</ows:Identifier>\n";
      xml << "      </Style>\n";
    }
    xml << "      <Format>image/png</Format>\n";
    xml << "      <TileMatrixSetLink>\n";
    xml << "        <TileMatrixSet>WebMercatorQuad</TileMatrixSet>\n";
    xml << "      </TileMatrixSetLink>\n";

    xml << "      <ResourceURL format=\"image/png\" resourceType=\"tile\" "
        << "template=\"/wmts?service=wmts&amp;request=GetTile&amp;version=1.0.0"
        << "&amp;layer=" << encodedLayerId << "&amp;style={Style}"
        << "&amp;format=image/png"
        << "&amp;tilematrixset=WebMercatorQuad"
        << "&amp;tilematrix={TileMatrix}"
        << "&amp;tilerow={TileRow}"
        << "&amp;tilecol={TileCol}\" />\n";
    xml << "    </Layer>\n";
  }

  xml << "    <TileMatrixSet>\n";
  xml << "      <ows:Identifier>WebMercatorQuad</ows:Identifier>\n";
  xml << "      "
         "<ows:SupportedCRS>urn:ogc:def:crs:EPSG::3857</ows:SupportedCRS>\n";

  for (int z = 0; z <= MAX_ZOOM; z++) {
    uint64_t matrixSize = 1ULL << z;
    double resolution = INITIAL_RESOLUTION / static_cast<double>(matrixSize);
    double scaleDenominator = resolution / 0.00028;
    // 0.28 mm pixel size as per OGC standard

    xml << "      <TileMatrix>\n";
    xml << "        <ows:Identifier>" << z << "</ows:Identifier>\n";
    xml << "        <ScaleDenominator>" << std::setprecision(15)
        << scaleDenominator << "</ScaleDenominator>\n";
    xml << "        <TopLeftCorner>" << WEBMERC_MIN << " " << WEBMERC_MAX
        << "</TopLeftCorner>\n";
    xml << "        <TileWidth>256</TileWidth>\n";
    xml << "        <TileHeight>256</TileHeight>\n";
    xml << "        <MatrixWidth>" << matrixSize << "</MatrixWidth>\n";
    xml << "        <MatrixHeight>" << matrixSize << "</MatrixHeight>\n";
    xml << "      </TileMatrix>\n";
  }

  xml << "    </TileMatrixSet>\n";
  xml << "  </Contents>\n";

  xml << "</Capabilities>\n";

  util::http::Answer answ("200 OK", xml.str());
  answ.params["Content-Type"] = "application/xml; charset=UTF-8";
  answ.params["Cache-Control"] = "no-cache";

  return answ;
}

// _____________________________________________________________________________
util::http::Answer Server::handleWFSGetCapabilitiesReq(
    const Params& pars) const {
  const std::string* serviceParam = getParamCaseInsensitive(pars, "service");
  if (serviceParam == nullptr || serviceParam->empty()) {
    throw std::invalid_argument("No WFS service specified.");
  }

  if (lower(*serviceParam) != "wfs") {
    throw std::invalid_argument("Invalid WFS service.");
  }
  const std::string* versionParam = getParamCaseInsensitive(pars, "version");
  if (versionParam == nullptr || versionParam->empty()) {
    throw std::invalid_argument("No WFS version specified.");
  }

  if (lower(*versionParam) != "2.0.0") {
    throw std::invalid_argument("Unsupported WFS version.");
  }

  std::vector<std::string> wfsTypeNames;
  {
    std::lock_guard<std::mutex> guard(_m);

    for (const auto& entry : _rs) {
      const std::string& sessionId = entry.first;
      wfsTypeNames.push_back("session_" + sessionId);
    }
  }

  LOG(INFO) << "[SERVER] WFS GetCapabilities with " << wfsTypeNames.size()
            << " layers.";

  std::stringstream xml;

  xml << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
  xml << "<WFS_Capabilities "
      << "xmlns=\"http://www.opengis.net/wfs/2.0\" "
      << "xmlns:wfs=\"http://www.opengis.net/wfs/2.0\" "
      << "xmlns:ows=\"http://www.opengis.net/ows/1.1\" "
      << "xmlns:xlink=\"http://www.w3.org/1999/xlink\" "
      << "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
      << "version=\"2.0.0\">\n";

  xml << "  <ows:ServiceIdentification>\n"
      << "    <ows:Title>qlever-petrimaps WFS Service</ows:Title>\n"
      << "    <ows:Abstract>WFS service for qlever-petrimaps</ows:Abstract>\n"
      << "    <ows:ServiceType>WFS</ows:ServiceType>\n"
      << "    <ows:ServiceTypeVersion>2.0.0</ows:ServiceTypeVersion>\n"
      << "  </ows:ServiceIdentification>\n";

  xml << "  <ows:OperationsMetadata>\n";
  xml << "    <ows:Operation name=\"GetCapabilities\">\n";
  xml << "      <ows:DCP>\n";
  xml << "        <ows:HTTP>\n";
  xml << "          <ows:Get xlink:href=\"/wfs\" />\n";
  xml << "        </ows:HTTP>\n";
  xml << "      </ows:DCP>\n";
  xml << "    </ows:Operation>\n";
  xml << "    <ows:Operation name=\"DescribeFeatureType\">\n";
  xml << "      <ows:DCP>\n";
  xml << "        <ows:HTTP>\n";
  xml << "          <ows:Get xlink:href=\"/wfs\" />\n";
  xml << "        </ows:HTTP>\n";
  xml << "      </ows:DCP>\n";
  xml << "      <ows:Parameter name=\"outputFormat\">\n";
  xml << "        <ows:AllowedValues>\n";
  xml << "          <ows:Value>text/xml; subtype=gml/3.2</ows:Value>\n";
  xml << "          <ows:Value>application/gml+xml; version=3.2</ows:Value>\n";
  xml << "        </ows:AllowedValues>\n";
  xml << "       </ows:Parameter>\n";
  xml << "      </ows:Operation>\n";
  xml << "    <ows:Operation name=\"GetFeature\">\n";
  xml << "      <ows:DCP>\n";
  xml << "        <ows:HTTP>\n";
  xml << "          <ows:Get xlink:href=\"/wfs\" />\n";
  xml << "        </ows:HTTP>\n";
  xml << "      </ows:DCP>\n";
  xml << "      <ows:Parameter name=\"outputFormat\">\n";
  xml << "        <ows:AllowedValues>\n";
  xml << "          <ows:Value>application/json</ows:Value>\n";
  xml << "        </ows:AllowedValues>\n";
  xml << "      </ows:Parameter>\n";
  xml << "      <ows:Parameter name=\"count\">\n";
  xml << "        <ows:AllowedValues>\n";
  xml << "          <ows:AnyValue />\n";
  xml << "        </ows:AllowedValues>\n";
  xml << "      </ows:Parameter>\n";
  xml << "      <ows:Parameter name=\"startindex\">\n";
  xml << "        <ows:AllowedValues>\n";
  xml << "          <ows:AnyValue />\n";
  xml << "        </ows:AllowedValues>\n";
  xml << "      </ows:Parameter>\n";
  xml << "      <ows:Parameter name=\"bbox\">\n";
  xml << "        <ows:AllowedValues>\n";
  xml << "          <ows:AnyValue />\n";
  xml << "        </ows:AllowedValues>\n";
  xml << "      </ows:Parameter>\n";
  xml << "      <ows:Parameter name=\"srsName\">\n";
  xml << "        <ows:AllowedValues>\n";
  xml << "          <ows:Value>EPSG:4326</ows:Value>\n";
  xml << "          <ows:Value>urn:ogc:def:crs:EPSG::4326</ows:Value>\n";
  xml << "          <ows:Value>EPSG:3857</ows:Value>\n";
  xml << "          <ows:Value>urn:ogc:def:crs:EPSG::3857</ows:Value>\n";
  xml << "        </ows:AllowedValues>\n";
  xml << "      </ows:Parameter>\n";
  xml << "      <ows:Parameter name=\"id\">\n";
  xml << "        <ows:AllowedValues>\n";
  xml << "          <ows:AnyValue />\n";
  xml << "        </ows:AllowedValues>\n";
  xml << "      </ows:Parameter>\n";
  xml << "      <ows:Parameter name=\"x\">\n";
  xml << "        <ows:AllowedValues>\n";
  xml << "          <ows:AnyValue />\n";
  xml << "        </ows:AllowedValues>\n";
  xml << "      </ows:Parameter>\n";
  xml << "      <ows:Parameter name=\"y\">\n";
  xml << "        <ows:AllowedValues>\n";
  xml << "          <ows:AnyValue />\n";
  xml << "        </ows:AllowedValues>\n";
  xml << "      </ows:Parameter>\n";
  xml << "      <ows:Parameter name=\"rad\">\n";
  xml << "        <ows:AllowedValues>\n";
  xml << "          <ows:AnyValue />\n";
  xml << "        </ows:AllowedValues>\n";
  xml << "      </ows:Parameter>\n";
  xml << "      <ows:Parameter name=\"width\">\n";
  xml << "        <ows:AllowedValues>\n";
  xml << "          <ows:AnyValue />\n";
  xml << "        </ows:AllowedValues>\n";
  xml << "      </ows:Parameter>\n";
  xml << "      <ows:Parameter name=\"height\">\n";
  xml << "        <ows:AllowedValues>\n";
  xml << "          <ows:AnyValue />\n";
  xml << "        </ows:AllowedValues>\n";
  xml << "      </ows:Parameter>\n";
  xml << "    </ows:Operation>\n";
  xml << "  </ows:OperationsMetadata>\n";

  xml << "  <FeatureTypeList>\n";
  for (const auto& typeName : wfsTypeNames) {
    std::string escapedTypeName = xmlEscape(typeName);

    xml << "    <FeatureType>\n";
    xml << "      <Name>" << escapedTypeName << "</Name>\n";
    xml << "      <Title>" << escapedTypeName << "</Title>\n";
    xml << "      <DefaultCRS>urn:ogc:def:crs:EPSG::4326</DefaultCRS>\n";
    xml << "      <OtherCRS>urn:ogc:def:crs:EPSG::3857</OtherCRS>\n";
    xml << "      <OutputFormats>\n";
    xml << "        <Format>application/json</Format>\n";
    xml << "      </OutputFormats>\n";
    xml << "    </FeatureType>\n";
  }
  xml << "  </FeatureTypeList>\n";

  xml << "</WFS_Capabilities>\n";

  util::http::Answer answ("200 OK", xml.str());
  answ.params["Content-Type"] = "application/xml; charset=UTF-8";
  answ.params["Cache-Control"] = "no-cache";

  return answ;
}

// _____________________________________________________________________________
util::http::Answer Server::handleWFSDescribeFeatureTypeReq(
    const Params& pars) const {
  const std::string* serviceParam = getParamCaseInsensitive(pars, "service");
  if (serviceParam == nullptr || serviceParam->empty()) {
    throw std::invalid_argument("No WFS service specified.");
  }

  if (lower(*serviceParam) != "wfs") {
    throw std::invalid_argument("Invalid WFS service.");
  }

  const std::string* versionParam = getParamCaseInsensitive(pars, "version");
  if (versionParam == nullptr || versionParam->empty()) {
    throw std::invalid_argument("No WFS version specified.");
  }

  if (lower(*versionParam) != "2.0.0") {
    throw std::invalid_argument("Unsupported WFS version.");
  }

  std::string typeName;
  const std::string* typeNamesParam =
      getParamCaseInsensitive(pars, "typenames");
  const std::string* typeNameParam = getParamCaseInsensitive(pars, "typename");

  if (typeNamesParam != nullptr && !typeNamesParam->empty()) {
    typeName = *typeNamesParam;
  } else if (typeNameParam != nullptr && !typeNameParam->empty()) {
    typeName = *typeNameParam;
  } else {
    throw std::invalid_argument("No WFS typename specified.");
  }

  std::string sessionId = typeName;
  const std::string prefix = "session_";
  if (sessionId.rfind(prefix, 0) == 0) {
    sessionId = sessionId.substr(prefix.size());
  }

  {
    std::lock_guard<std::mutex> guard(_m);
    if (!_rs.count(typeName)) {
      throw std::invalid_argument("WFS type name not found.");
    }
  }
  std::string schemaTypeName = "session_" + sessionId;
  std::string escapedTypeName = xmlEscape(schemaTypeName);

  std::stringstream xml;
  xml << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
  xml << "<xsd:schema "
      << "xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" "
      << "xmlns:gml=\"http://www.opengis.net/gml/3.2\" "
      << "xmlns:qpm=\"https://qlever.dev/qlever-petrimaps/wfs\" "
      << "targetNamespace=\"https://qlever.dev/qlever-petrimaps/wfs\" "
      << "elementFormDefault=\"qualified\" "
      << "version=\"2.0.0\">\n";
  xml << "  <xsd:import namespace=\"http://www.opengis.net/gml/3.2\" />\n";
  xml << "  <xsd:complexType name=\"ObjectType\">\n";
  xml << "    <xsd:complexContent>\n";
  xml << "      <xsd:extension base=\"gml:AbstractFeatureType\">\n";
  xml << "        <xsd:sequence>\n";
  xml << "          <xsd:element name=\"geometry\" "
      << "type=\"gml:GeometryPropertyType\" minOccurs=\"0\" />\n";
  xml << "          <xsd:element name=\"gid\" "
      << "type=\"xsd:unsignedLong\" minOccurs=\"0\" />\n";
  xml << "          <xsd:element name=\"featureID\" "
      << "type=\"xsd:string\" minOccurs=\"0\" />\n";
  xml << "        </xsd:sequence>\n";
  xml << "      </xsd:extension>\n";
  xml << "    </xsd:complexContent>\n";
  xml << "  </xsd:complexType>\n";
  xml << "  <xsd:element name=\"" << escapedTypeName
      << "\" type=\"qpm:ObjectType\" "
      << "substitutionGroup=\"gml:AbstractFeature\" />\n";
  xml << "</xsd:schema>\n";

  util::http::Answer answ("200 OK", xml.str());
  answ.params["Content-Type"] = "text/xml; charset=UTF-8";
  answ.params["Cache-Control"] = "no-cache";
  return answ;
}

// _____________________________________________________________________________
util::http::Answer Server::handleWFSGetFeatureReq(
    const Params& pars, const HeaderParams& headerPars, int sock) const {
  auto remoteAddr = remoteAddress(sock, headerPars);
  const std::string* serviceParam = getParamCaseInsensitive(pars, "service");
  if (serviceParam == nullptr || serviceParam->empty()) {
    throw std::invalid_argument("No WFS service specified.");
  }

  if (lower(*serviceParam) != "wfs") {
    throw std::invalid_argument("Invalid WFS service.");
  }

  const std::string* versionParam = getParamCaseInsensitive(pars, "version");
  if (versionParam == nullptr || versionParam->empty()) {
    throw std::invalid_argument("No WFS version specified.");
  }

  if (lower(*versionParam) != "2.0.0") {
    throw std::invalid_argument("Unsupported WFS version.");
  }

  std::string typeName, sessionId, geomField;
  const std::string* typeNamesParam =
      getParamCaseInsensitive(pars, "typenames");
  const std::string* typeNameParam = getParamCaseInsensitive(pars, "typename");

  if (typeNamesParam != nullptr && !typeNamesParam->empty()) {
    typeName = *typeNamesParam;
  } else if (typeNameParam != nullptr && !typeNameParam->empty()) {
    typeName = *typeNameParam;
  } else {
    throw std::invalid_argument("No WFS typename specified.");
  }

  auto parts = util::split(typeName, ':');
  if (parts.size() != 2)
    throw std::invalid_argument("Invalid type name '" + typeName + "' specified");

  sessionId = parts[0];
  geomField = parts[1];
  std::shared_ptr<Requestor> reqor;

  bool found = false;
  {
    std::lock_guard<std::mutex> guard(_m);
    if (_rs.count(sessionId)) {
      reqor = _rs.at(sessionId);
      found = true;
    }
  }

  if (!found) {
    throw std::invalid_argument("WFS type name not found.");
  }

  if (!reqor->ready()) {
    throw std::invalid_argument("Session not ready.");
  }

  const auto fields = reqor->getLayers();
  if (fields.empty()) {
    throw std::invalid_argument("No fields found for WFS type name.");
  }

  size_t lid = reqor->getLidByGeomField(geomField);

  auto layerCfg = reqor->getLayers()[lid];

  auto parseIntParam = [](const std::string& value, const std::string& name) {
    if (value.empty()) {
      throw std::invalid_argument("Invalid WFS " + name + " specified.");
    }

    size_t pos = 0;
    try {
      size_t parsed = std::stoull(value, &pos);

      if (pos != value.size()) {
        throw std::invalid_argument("Invalid WFS " + name + " specified.");
      }

      return parsed;
    } catch (...) {
      throw std::invalid_argument("Invalid WFS " + name + " specified.");
    }
  };

  size_t total = reqor->getNumObjects(lid);
  size_t startIndex = 0;

  const std::string* startIndexParam =
      getParamCaseInsensitive(pars, "startindex");
  if (startIndexParam != nullptr && !startIndexParam->empty()) {
    startIndex = parseIntParam(*startIndexParam, "startindex");
  }

  if (startIndex > total) {
    startIndex = total;
  }

  bool hasBbox = false;
  bool fullExport = false;
  DBox dbbox;
  FBox fbbox;

  const std::string* bboxParam = getParamCaseInsensitive(pars, "bbox");
  const std::string* gidParam = getParamCaseInsensitive(pars, "gid");
  const std::string* countParam = getParamCaseInsensitive(pars, "count");

  if (bboxParam != nullptr && !bboxParam->empty()) {
    auto bboxParts = util::split(*bboxParam, ',');

    const std::string* srsParam = getParamCaseInsensitive(pars, "srsName");
    if (srsParam == nullptr) {
      srsParam = getParamCaseInsensitive(pars, "crs");
    }
    std::string srsName = srsParam != nullptr ? lower(*srsParam) : "epsg:4326";
    if (bboxParts.size() != 4 && bboxParts.size() != 5) {
      throw std::invalid_argument("Invalid WFS BBOX specified.");
    }

    double minX;
    double minY;
    double maxX;
    double maxY;

    if (srsName == "epsg:3857" || srsName == "urn:ogc:def:crs:epsg::3857") {
      minX = std::atof(bboxParts[0].c_str());
      minY = std::atof(bboxParts[1].c_str());
      maxX = std::atof(bboxParts[2].c_str());
      maxY = std::atof(bboxParts[3].c_str());
    } else {
      double minLon = std::atof(bboxParts[0].c_str());
      double minLat = std::atof(bboxParts[1].c_str());
      double maxLon = std::atof(bboxParts[2].c_str());
      double maxLat = std::atof(bboxParts[3].c_str());

      auto lowerLeft = latLngToWebMerc<double>(minLat, minLon);
      auto upperRight = latLngToWebMerc<double>(maxLat, maxLon);

      minX = lowerLeft.getX();
      minY = lowerLeft.getY();
      maxX = upperRight.getX();
      maxY = upperRight.getY();
    }

    double normMinX = std::min(minX, maxX);
    double normMinY = std::min(minY, maxY);
    double normMaxX = std::max(minX, maxX);
    double normMaxY = std::max(minY, maxY);

    fbbox = FBox({static_cast<float>(normMinX), static_cast<float>(normMinY)},
                 {static_cast<float>(normMaxX), static_cast<float>(normMaxY)});
    dbbox = DBox({normMinX, normMinY}, {normMaxX, normMaxY});

    hasBbox = true;
  }

  std::vector<size_t> featureIds;

  if (hasBbox) {
    // select by bounding box
    std::unordered_set<ID_TYPE> candidates;

    if (intersects(reqor->getPointGrid(lid).getBBox(), fbbox)) {
      reqor->getPointGrid(lid).get(fbbox, &candidates);
    }

    if (intersects(reqor->getLineGrid(lid).getBBox(), fbbox)) {
      reqor->getLineGrid(lid).get(fbbox, &candidates);
    }

    for (const auto& cand : candidates) {
      auto oid = reqor->getObjects(lid)[cand].second;
      auto geomId = reqor->getObjects(lid)[cand].first;

      if (reqor->isCluster(lid, oid)) oid = reqor->getCluster(lid, oid).first;

      bool include = false;

      if (geomId < I_OFFSET) {
        auto p = reqor->getPoint(lid, oid);
        include = contains(p, fbbox);
      } else {
        size_t lineId = geomId - I_OFFSET;

        if (reqor->isArea(lineId)) {
          const auto& dline = reqor->extractLineGeom(lineId);
          include = util::geo::intersects(dbbox, util::geo::DPolygon(dline));
        } else  {
          include = reqor->lineIntersects(lineId, dbbox);
        }
      }

      if (include) {
        featureIds.push_back(oid);
      }
    }
  } else if (gidParam != nullptr && !gidParam->empty()) {
    // select by ID
    const size_t gid = parseIntParam(*gidParam, "gid");
    const size_t selectableTotal =
        reqor->getObjects(lid).size() + reqor->getDynamicPoints(lid).size();
    if (gid >= selectableTotal) {
      throw std::invalid_argument("Invalid WFS gid specified.");
    }
    featureIds = {gid};
  } else {
    fullExport = true;
  }

  size_t featureStart = std::min(startIndex, featureIds.size());
  size_t featureEnd = featureIds.size();

  if (countParam != nullptr && !countParam->empty()) {
    size_t count = parseIntParam(*countParam, "count");
    if (count < featureIds.size() - featureStart) {
      featureEnd = featureStart + count;
    }
  }

  auto answ = util::http::Answer("200 OK", "");
  answ.params["Content-Encoding"] = "identity";
  answ.params["Content-Type"] = "application/json; charset=UTF-8";
  answ.params["Cache-Control"] = "no-cache";
  answ.params["Server"] = "qlever-petrimaps";
  answ.params["Content-Disposition"] = "attachment;filename:\"export.json\"";

  // we do not set the Content-Length header here, but serve until
  // we are done. In particular, we do not need to send our data in chunks, as
  // specified by https://www.rfc-editor.org/rfc/rfc7230#section-3.3.3
  // point 7

  std::stringstream head;
  head << "HTTP/1.1 " << answ.status << "\r\n";
  for (const auto& kv : answ.params)
    head << kv.first << ": " << kv.second << "\r\n";

  head << "\r\n";
  head << "{\"type\":\"FeatureCollection\",\"features\":[";

  sendRaw(sock, head.str());

  bool first = false;

  if (fullExport) {
    size_t oid = 0;
    reqor->requestRows(
        [sock, &first, &oid, &sessionId, &layerCfg](
            std::vector<std::vector<std::pair<std::string, std::string>>>
                rows) {
          std::stringstream json;
          json << std::setprecision(10);

          for (const auto& row : rows) {
            if (row.empty()) continue;

            util::json::Val dict;

            size_t geomField = row.size() - 1;

            for (size_t i = 0; i < row.size(); i++) {
              if (row[i].first == layerCfg.geomField) {
                geomField = i;
                // skip WKT field here, is redundant in GeoJSON
                continue;
              }
              dict.dict[row[i].first] = row[i].second;
            }

            dict.dict["gid"] = oid;
            dict.dict["featureID"] = sessionId + "::" + std::to_string(oid);

            if (row[geomField].second.size()) {
              first = printWKTFeature(json, row[geomField].second, dict, first);
              json << "\n";
            }

            oid++;
          }

          if (json.str().size() != 0) sendRaw(sock, json.str());
        },
        remoteAddr);
  } else {
    for (size_t idx = featureStart; idx < featureEnd; idx++) {
      size_t oid = featureIds[idx];
      std::string featureId = sessionId + "::" + std::to_string(oid);

      util::json::Val dict;
      dict.dict["gid"] = oid;
      dict.dict["featureID"] = featureId;

      size_t row = reqor->getRow(lid, oid);

      for (const auto& col : reqor->requestRow(row, remoteAddr)) {
        if (col.first == layerCfg.geomField) {
          // skip WKT field here, is redundant in GeoJSON
          continue;
        }
        dict.dict[col.first] = col.second;
      }

      auto res = reqor->getGeom(lid, oid, 0);

      std::stringstream json;

      if (first) json << ",";
      first = true;

      if ((res.poly.size() != 0) + (res.point.size() != 0) +
              (res.line.size() != 0) >
          1) {
        util::geo::Collection<double> col;
        col.push_back(res.poly);
        col.push_back(res.line);
        col.push_back(res.point);

        GeoJsonOutput out(json, true);
        out.printLatLng(col, dict);
      } else if (res.poly.size()) {
        GeoJsonOutput out(json, true);
        out.printLatLng(res.poly, dict);
      } else if (res.line.size()) {
        GeoJsonOutput out(json, true);
        out.printLatLng(res.line, dict);
      } else if (res.point.size()) {
        GeoJsonOutput out(json, true);
        out.printLatLng(res.point, dict);
      }

      sendRaw(sock, json.str());
    }
  }

  sendRaw(sock, "]}");

  answ.raw = true;
  return answ;
}

// _____________________________________________________________________________
std::string Server::getHeatLayer(const std::string& layer) const {
  std::string heatLayer = layer;

  if (layer.find('-') == std::string::npos) {
    std::shared_ptr<Requestor> reqor;
    {
      std::lock_guard<std::mutex> guard(_m);
      if (!_rs.count(layer)) {
        throw std::invalid_argument("Session not found.");
      }
      reqor = _rs[layer];
    }

    const auto layers = reqor->getLayers();
    if (layers.empty()) {
      throw std::invalid_argument("No fields found for session.");
    }

    heatLayer = layer + "-" + layers[0].id;
  }
  return heatLayer;
}

// _____________________________________________________________________________
uint64_t Server::validateTileCoordinates(int x, int y, int z) {
  if (x < 0 || y < 0 || z < 0)
    throw std::invalid_argument("Invalid tile coordinates.");

  if (z >= 31) throw std::invalid_argument("Zoom level too large.");

  uint64_t tilesPerAxis = 1ULL << z;
  if (static_cast<uint64_t>(x) >= tilesPerAxis ||
      static_cast<uint64_t>(y) >= tilesPerAxis) {
    throw std::invalid_argument("Tile coordinates out of ranges.");
  }
  return tilesPerAxis;
}

// _____________________________________________________________________________
std::string Server::getWebMercatorTileBbox(int x, int topOriginY, int z) {
  uint64_t tilesPerAxis = validateTileCoordinates(x, topOriginY, z);

  const double WEBMERC_MIN = -20037508.342789244;
  const double WEBMERC_MAX = 20037508.342789244;
  const double WORLD_SIZE = WEBMERC_MAX - WEBMERC_MIN;

  double tileSize = WORLD_SIZE / static_cast<double>(tilesPerAxis);

  double x1 = WEBMERC_MIN + x * tileSize;
  double x2 = WEBMERC_MIN + (x + 1) * tileSize;

  double yTop = WEBMERC_MAX - topOriginY * tileSize;
  double yBottom = WEBMERC_MAX - (topOriginY + 1) * tileSize;

  std::stringstream bboxSs;
  bboxSs << std::setprecision(15) << x1 << "," << yBottom << "," << x2 << ","
         << yTop;

  return bboxSs.str();
}

// _____________________________________________________________________________
std::string Server::xmlEscape(const std::string& value) {
  std::string escaped;
  for (char c : value) {
    switch (c) {
      case '&':
        escaped += "&amp;";
        break;
      case '<':
        escaped += "&lt;";
        break;
      case '>':
        escaped += "&gt;";
        break;
      case '"':
        escaped += "&quot;";
        break;
      case '\'':
        escaped += "&apos;";
        break;
      default:
        escaped += c;
        break;
    }
  }
  return escaped;
}

// _____________________________________________________________________________
std::string Server::urlEncode(const std::string& value) {
  std::stringstream encoded;

  for (unsigned char c : value) {
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' ||
        c == '~') {
      encoded << c;
    } else {
      encoded << '%' << std::uppercase << std::hex << std::setw(2)
              << std::setfill('0') << static_cast<int>(c) << std::nouppercase
              << std::dec;
    }
  }
  return encoded.str();
}

// _____________________________________________________________________________
util::http::Answer Server::handleTMSReq(const Params& pars, int sock) const {
  if (pars.count("layers") == 0 || pars.find("layers")->second.empty())
    throw std::invalid_argument("No layer id specified.");

  if (pars.count("styles") == 0 || pars.find("styles")->second.empty())
    throw std::invalid_argument("No style specified.");

  if (pars.count("x") == 0 || pars.find("x")->second.empty())
    throw std::invalid_argument("No x specified.");

  if (pars.count("y") == 0 || pars.find("y")->second.empty())
    throw std::invalid_argument("No y specified.");

  if (pars.count("z") == 0 || pars.find("z")->second.empty())
    throw std::invalid_argument("No z specified.");

  std::string id = pars.find("layers")->second;

  int x = atoi(pars.find("x")->second.c_str());
  int y = atoi(pars.find("y")->second.c_str());
  int z = atoi(pars.find("z")->second.c_str());

  uint64_t tilesPerAxis = validateTileCoordinates(x, y, z);
  int topOriginY = static_cast<int>(tilesPerAxis - 1 - y);
  std::string bbox = getWebMercatorTileBbox(x, topOriginY, z);

  Params heatPars = pars;
  heatPars["bbox"] = bbox;
  heatPars["width"] = "256";
  heatPars["height"] = "256";

  return handleHeatMapReq(heatPars, sock);
}

// _____________________________________________________________________________
util::http::Answer Server::handleTouchReq(const Params& pars,
                                          const HeaderParams& headerParams,
                                          int sock) const {
  auto remoteAddr = remoteAddress(sock, headerParams);

  if (pars.count("backend") == 0 || pars.find("backend")->second.empty())
    throw std::invalid_argument("No backend (?backend=) specified.");

  const std::string& backend = pars.find("backend")->second;

  std::string accessToken;
  if (headerParams.count("Authorization") != 0 &&
      !headerParams.find("Authorization")->second.empty()) {
    accessToken = headerParams.find("Authorization")->second;
  }

  std::string configJson;
  if (pars.find("cfg") != pars.end()) {
    configJson = pars.find("cfg")->second;
  }

  auto backendCfg =
      getGeomCacheConfig(backend, accessToken, configJson, remoteAddr);

  createCache(backendCfg);
  std::shared_ptr<GeomCache> cache = _caches[backendCfg.backend];

  std::stringstream ss;
  ss << "{\"config\":";
  ss << backendCfg.toJSON();
  ss << ", \"loaded\": " << cache->getLoadStatusPercent(true);
  ss << "}";

  auto answ = util::http::Answer("200 OK", ss.str());
  answ.params["Content-Type"] = "application/json; charset=utf-8";

  return answ;
}
// _____________________________________________________________________________
util::http::Answer Server::handleWFSPickFeatureReq(const Params& pars,
                                                   const HeaderParams& headers,
                                                   int sock) const {
  return handleNearestFeatureReq(pars, headers, sock, true);
}
// _____________________________________________________________________________
util::http::Answer Server::handleNearestFeatureReq(const Params& pars,
                                                   const HeaderParams& headers,
                                                   int sock,
                                                   bool isWfsRequest) const {
  auto remoteAddr = remoteAddress(sock, headers);

  if (pars.count("x") == 0 || pars.find("x")->second.empty())
    throw std::invalid_argument("No x coord (?x=) specified.");
  float x = std::atof(pars.find("x")->second.c_str());

  if (pars.count("y") == 0 || pars.find("y")->second.empty())
    throw std::invalid_argument("No y coord (?y=) specified.");
  float y = std::atof(pars.find("y")->second.c_str());

  if (pars.count("rad") == 0 || pars.find("rad")->second.empty())
    throw std::invalid_argument("No rad (?rad=) specified.");
  float rad = std::atof(pars.find("rad")->second.c_str());

  if (pars.count("width") == 0 || pars.find("width")->second.empty())
    throw std::invalid_argument("No width (?width=) specified.");
  if (pars.count("height") == 0 || pars.find("height")->second.empty())
    throw std::invalid_argument("No height (?height=) specified.");

  if (pars.count("bbox") == 0 || pars.find("bbox")->second.empty())
    throw std::invalid_argument("No bbox specified.");
  auto box = util::split(pars.find("bbox")->second, ',');

  std::string typeName, sessionId, geomField;
  const std::string* typeNamesParam =
      getParamCaseInsensitive(pars, "typenames");
  const std::string* typeNameParam = getParamCaseInsensitive(pars, "typename");

  if (typeNamesParam != nullptr && !typeNamesParam->empty()) {
    typeName = *typeNamesParam;
  } else if (typeNameParam != nullptr && !typeNameParam->empty()) {
    typeName = *typeNameParam;
  } else {
    throw std::invalid_argument("No WFS typename specified.");
  }

  auto parts = util::split(typeName, ':');
  if (parts.size() != 2)
    throw std::invalid_argument("Invalid type name '" + typeName + "' specified");

  sessionId = parts[0];
  geomField = parts[1];

  if (box.size() != 4) throw std::invalid_argument("Invalid request.");
  if (isWfsRequest) {
    const std::string* srsParam = getParamCaseInsensitive(pars, "srsName");
    if (srsParam == nullptr) {
      srsParam = getParamCaseInsensitive(pars, "crs");
    }

    std::string srsName = srsParam != nullptr ? lower(*srsParam) : "epsg:3857";

    if (srsName != "epsg:3857" && srsName != "urn:ogc:def:crs:epsg::3857") {
      throw std::invalid_argument("WFS pick requires EPSG:3857 coordinates.");
    }
  }

  double x1 = std::atof(box[0].c_str());
  double y1 = std::atof(box[1].c_str());
  double x2 = std::atof(box[2].c_str());
  double y2 = std::atof(box[3].c_str());
  double mercH = fabs(y2 - y1);

  auto fbbox = FBox({x1, y1}, {x2, y2});

  int h = atoi(pars.find("height")->second.c_str());

  if (h <= 0 || h > 3000) throw std::invalid_argument("Invalid request");

  double reso = mercH / h;

  // res of -1 means dont render clusters
  if (reso >= THRESHOLD) reso = -1;

  LOG(DEBUG) << "[SERVER] WFS pick at " << x << ", " << y;

  std::shared_ptr<Requestor> reqor;
  {
    std::lock_guard<std::mutex> guard(_m);
    bool has = _rs.count(sessionId);
    if (!has) {
      LOG(ERROR) << "Session " << sessionId << " not found!";
      throw std::invalid_argument("Session not found");
    }
    reqor = _rs[sessionId];
  }

  if (!reqor->ready()) {
    throw std::invalid_argument("Session not ready.");
  }

  size_t lid = reqor->getLidByGeomField(geomField);

  // as soon as we are ready, the reqor can be read concurrently

  LOG(INFO) << "Looking up nearest geometry...";
  auto res = reqor->getNearest(lid, {x, y}, rad, reso, fbbox, remoteAddr);
  LOG(INFO) << "Got nearest geometry...";

  if (isWfsRequest) {
    std::stringstream json;
    json << "{\"type\":\"FeatureCollection\",\"features\":[";

    if (res.has) {
      util::json::Val dict;

      dict.dict["id"] = std::to_string(res.id);
      dict.dict["geomfield"] = reqor->getLayers()[res.fieldId].geomField;

      auto ll = webMercToLatLng<float>(res.pos.getX(), res.pos.getY());
      dict.dict["popup_lat"] = std::to_string(ll.getY());
      dict.dict["popup_lng"] = std::to_string(ll.getX());

      for (const auto& kv : res.cols) {
        dict.dict[kv.first] = kv.second;
      }

      if ((res.poly.size() != 0) + (res.point.size() != 0) +
              (res.line.size() != 0) >
          1) {
        util::geo::Collection<double> col;
        col.push_back(res.poly);
        col.push_back(res.line);
        col.push_back(res.point);

        GeoJsonOutput out(json, true);
        out.printLatLng(col, dict);
      } else if (res.poly.size()) {
        GeoJsonOutput out(json, true);
        out.printLatLng(res.poly, dict);
      } else if (res.line.size()) {
        GeoJsonOutput out(json, true);
        out.printLatLng(res.line, dict);
      } else {
        GeoJsonOutput out(json, true);
        out.printLatLng(res.point, dict);
      }
    }
    json << "]}";

    auto answ = util::http::Answer("200 OK", json.str());
    answ.params["Content-Type"] = "application/json; charset=utf-8";
    return answ;
  }

  std::stringstream json;

  json << "[";

  if (res.has) {
    json << "{\"id\" :" << res.id;
    json << ",\"geomfield\" :\"" << reqor->getLayers()[res.fieldId].geomField
         << "\"";
    json << ",\"attrs\" : [";

    bool first = true;

    for (const auto& kv : res.cols) {
      if (!first) {
        json << ",";
      }
      json << "[\"" << util::jsonStringEscape(kv.first) << "\",\""
           << util::jsonStringEscape(kv.second) << "\"]";

      first = false;
    }

    auto ll = webMercToLatLng<float>(res.pos.getX(), res.pos.getY());

    json << "]";
    json << std::setprecision(10) << ",\"ll\":{\"lat\" : " << ll.getY()
         << ",\"lng\":" << ll.getX() << "}";

    if ((res.poly.size() != 0) + (res.point.size() != 0) +
            (res.line.size() != 0) >
        1) {
      util::geo::Collection<double> col;
      col.push_back(res.poly);
      col.push_back(res.line);
      col.push_back(res.point);

      json << ",\"geom\":";
      GeoJsonOutput out(json);
      out.printLatLng(col, {});
    } else if (res.poly.size()) {
      json << ",\"geom\":";
      GeoJsonOutput out(json);
      out.printLatLng(res.poly, {});
    } else if (res.line.size()) {
      json << ",\"geom\":";
      GeoJsonOutput out(json);
      out.printLatLng(res.line, {});
    } else {
      json << ",\"geom\":";
      GeoJsonOutput out(json);
      out.printLatLng(res.point, {});
    }

    json << "}";
  }

  json << "]";

  auto answ = util::http::Answer("200 OK", json.str());
  answ.params["Content-Type"] = "application/json; charset=utf-8";

  return answ;
}

// _____________________________________________________________________________
util::http::Answer Server::handleClearSessReq(const Params& pars,
                                              const HeaderParams& headerParams,
                                              int) const {
  std::string id;
  if (pars.count("id") != 0 && !pars.find("id")->second.empty())
    id = pars.find("id")->second;

  std::string accessToken;
  if (headerParams.count("Authorization") != 0 &&
      !headerParams.find("Authorization")->second.empty()) {
    accessToken = headerParams.find("Authorization")->second;
  }

  if (accessToken != _accessToken)
    throw std::invalid_argument("Invalid access token");

  {
    std::lock_guard<std::mutex> guard(_m);
    if (id.size())
      clearSession(id);
    else
      clearSessions();
  }

  auto answ = util::http::Answer("200 OK", "{}");
  answ.params["Content-Type"] = "application/json; charset=utf-8";

  return answ;
}

// _____________________________________________________________________________
util::http::Answer Server::handleExamplePageReq(const Params&, int) const {
  std::string html =
      std::string(example_html,
                  example_html + sizeof example_html / sizeof example_html[0]);

  auto a = util::http::Answer("200 OK", html);
  a.params["Content-Type"] = "text/html; charset=utf-8";

  return a;
}

// _____________________________________________________________________________
util::http::Answer Server::handleIndexReq(const Params& pars, int) const {
  std::stringstream ss;
  ss << "window.postParams =";

  util::json::Writer w(&ss);
  w.obj();
  for (const auto& param : pars) {
    w.key(param.first);
    w.val(param.second);
  }
  w.closeAll();
  std::string html = std::string(
      index_html, index_html + sizeof index_html / sizeof index_html[0]);

  util::replace(html, "<!-- PETRIMAPS_INLINE_SCRIPT -->", ss.str());

  auto a = util::http::Answer("200 OK", html);
  a.params["Content-Type"] = "text/html; charset=utf-8";

  return a;
}

// _____________________________________________________________________________
util::http::Answer Server::handleQueryReq(const Params& pars,
                                          const HeaderParams& headers,
                                          int sock) const {
  if (pars.count("backend") == 0 || pars.find("backend")->second.empty())
    throw std::invalid_argument("No backend (?backend=) specified.");

  auto remoteAddr = remoteAddress(sock, headers);

  RequestorConfig rcfg;

  const std::string& backend = pars.find("backend")->second;

  auto backendCfg = getGeomCacheConfig(backend, "", "", remoteAddr);

  if (pars.count("cfg") != 0 && !pars.find("cfg")->second.empty()) {
    rcfg = getRequestorCfgFromJSON(pars.find("cfg")->second);
  } else if (pars.count("query") != 0 && !pars.find("query")->second.empty()) {
    rcfg =
        getDefaultRequestorCfg(backendCfg.backend, pars.find("query")->second);
  }

  if (rcfg.query.size() == 0)
    throw std::invalid_argument("No query specified.");

  LOG(INFO) << "[SERVER] Queried backend is " << backendCfg.backend;
  LOG(INFO) << "[SERVER] Query is:\n" << rcfg.query;

  createCache(backendCfg);
  std::string indexHash = loadCache(backendCfg);

  std::string queryId = backend + "$" + indexHash + "$" + rcfg.getHash();

  std::shared_ptr<Requestor> reqor;
  std::string sessionId;

  {
    std::lock_guard<std::mutex> guard(_m);
    if (_queryCache.count(queryId)) {
      sessionId = _queryCache[queryId];
      reqor = _rs[sessionId];
    } else {
      reqor = std::shared_ptr<Requestor>(
          new Requestor(_caches[backendCfg.backend], rcfg, _maxMemory));

      sessionId = getSessionId();

      _rs[sessionId] = reqor;
      if (util::toLower(rcfg.query).find("rand()") == std::string::npos)
        _queryCache[queryId] = sessionId;
    }
  }

  try {
    reqor->request(remoteAddr);
  } catch (OutOfMemoryError& ex) {
    LOG(ERROR) << ex.what() << backendCfg.backend;

    // delete cache, is now in unready state
    {
      std::lock_guard<std::mutex> guard(_m);
      clearSession(sessionId);
    }

    auto answ = util::http::Answer("406 Not Acceptable", ex.what());
    answ.params["Content-Type"] = "application/json; charset=utf-8";
    return answ;
  }

  util::geo::FBox bbox;

  for (size_t lid = 0; lid < reqor->getNumLayers(); lid++) {
    bbox = extendBox(reqor->getPointGrid(lid).getBBox(), bbox);
    bbox = extendBox(reqor->getLineGrid(lid).getBBox(), bbox);
  }

  size_t numObjs = reqor->getNumObjects();

  auto ll = bbox.getLowerLeft();
  auto ur = bbox.getUpperRight();

  double llX = ll.getX();
  double llY = ll.getY();
  double urX = ur.getX();
  double urY = ur.getY();

  std::stringstream json;
  json << std::fixed << "{\"qid\" : \"" << sessionId << "\",\"bounds\":[["
       << llX << "," << llY << "],[" << urX << "," << urY << "]]"
       << ",\"numobjects\":" << numObjs << ",\"layers\": [";

  bool first = false;
  for (size_t lid = 0; lid < reqor->getNumLayers(); lid++) {
    const auto& layer = reqor->getLayers()[lid];
    if (first) json << ",";
    first = true;
    json << "{";
    json << "\"id\":\"" << layer.id << "\",";
    json << "\"geomfield\":\"" << layer.geomField << "\",";
    json << "\"name\":\"" << layer.name << "\",";
    json << "\"group\":\"" << layer.group << "\",";
    json << "\"color\":\"" << layer.color << "\",";
    json << "\"colorscheme\":\"" << layer.colorscheme << "\",";
    json << "\"numobjects\":\"" << reqor->getNumObjects(lid) << "\",";
    json << "\"style\":\"" << layer.style << "\",";
    json << "\"toggle\":\"" << layer.toggle << "\"";
    if (layer.rasterW != 0 && layer.rasterH != 0)
      json << ",\"rasterw\":" << layer.rasterW
           << ", \"rasterh\":" << layer.rasterH;
    json << "}";
  }

  json << "]}";

  auto answ = util::http::Answer("200 OK", json.str());
  answ.params["Content-Type"] = "application/json; charset=utf-8";

  return answ;
}

// _____________________________________________________________________________
std::string Server::parseUrl(std::string u, std::string pl,
                             std::map<std::string, std::string>* params) {
  auto parts = util::split(u, '?', 2);

  if (parts.size() > 1) {
    auto kvs = util::split(parts[1], '&');
    for (const auto& kv : kvs) {
      auto kvp = util::split(kv, '=', 2);
      if (kvp.size() == 0) continue;
      if (kvp.size() == 1) kvp.push_back("");
      (*params)[util::urlDecode(kvp[0])] = util::urlDecode(kvp[1]);
    }
  }

  // also parse post data
  auto kvs = util::split(pl, '&');
  for (const auto& kv : kvs) {
    auto kvp = util::split(kv, '=', 2);
    if (kvp.size() == 0) continue;
    if (kvp.size() == 1) kvp.push_back("");
    (*params)[util::urlDecode(kvp[0])] = util::urlDecode(kvp[1]);
  }

  return util::urlDecode(parts.front());
}

// _____________________________________________________________________________
void Server::pngWriteRowCb(png_structp, png_uint_32 row, int) { _curRow = row; }

// _____________________________________________________________________________
inline void pngWriteCb(png_structp png_ptr, png_bytep data, png_size_t length) {
  int sock = *((int*)png_get_io_ptr(png_ptr));

  size_t writes = 0;

  while (writes != length) {
    int64_t out = send(sock, reinterpret_cast<char*>(data) + writes,
                       length - writes, MSG_NOSIGNAL);
    if (out < 0) {
      if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR) continue;
      break;
    }
    writes += out;
  }
}

// _____________________________________________________________________________
inline void pngWarnCb(png_structp, png_const_charp error_msg) {
  LOG(WARN) << "[SERVER] (libpng) " << error_msg;
}

// _____________________________________________________________________________
inline void pngErrorCb(png_structp, png_const_charp error_msg) {
  LOG(ERROR) << "[SERVER] (libpng) " << error_msg;
}

// _____________________________________________________________________________
void Server::writePNG(const unsigned char* data, size_t w, size_t h,
                      int sock) const {
  png_structp png_ptr = png_create_write_struct(PNG_LIBPNG_VER_STRING, nullptr,
                                                pngErrorCb, pngWarnCb);
  if (!png_ptr) return;

  png_infop info_ptr = png_create_info_struct(png_ptr);
  if (!info_ptr) {
    png_destroy_write_struct(&png_ptr, (png_infopp) nullptr);
    return;
  }

  if (setjmp(png_jmpbuf(png_ptr))) {
    png_destroy_write_struct(&png_ptr, &info_ptr);
    return;
  }

  // Handle Load Status
  _totalSize = h;
  _curRow = 0;

  png_set_write_status_fn(png_ptr, pngWriteRowCb);
  png_set_write_fn(png_ptr, &sock, pngWriteCb, 0);
  png_set_filter(png_ptr, 0, PNG_FILTER_NONE | PNG_FILTER_VALUE_NONE);
  png_set_compression_level(png_ptr, 7);

  static const int bit_depth = 8;
  static const int color_type = PNG_COLOR_TYPE_RGB_ALPHA;
  static const int interlace_type = PNG_INTERLACE_NONE;
  png_set_IHDR(png_ptr, info_ptr, w, h, bit_depth, color_type, interlace_type,
               PNG_COMPRESSION_TYPE_DEFAULT, PNG_FILTER_TYPE_DEFAULT);

  checkMem(h * sizeof(png_bytep), _maxMemory);
  png_bytep* row_pointers =
      (png_byte**)png_malloc(png_ptr, h * sizeof(png_bytep));

  for (size_t y = 0; y < h; ++y) {
    row_pointers[y] = const_cast<png_bytep>(data + y * w * 4);
  }

  png_set_rows(png_ptr, info_ptr, row_pointers);
  png_write_png(png_ptr, info_ptr, PNG_TRANSFORM_IDENTITY, nullptr);

  png_free(png_ptr, row_pointers);
  png_destroy_write_struct(&png_ptr, &info_ptr);
}

// _____________________________________________________________________________
void Server::clearSession(const std::string& id) const {
  if (_rs.count(id)) {
    LOG(INFO) << "[SERVER] Clearing session " << id;
    _rs.erase(id);

    for (auto it = _queryCache.cbegin(); it != _queryCache.cend();) {
      if (it->second == id) {
        it = _queryCache.erase(it);
      } else {
        ++it;
      }
    }
  }
}

// _____________________________________________________________________________
void Server::clearSessions() const {
  LOG(INFO) << "[SERVER] Clearing all sessions...";
  _rs.clear();
  _queryCache.clear();
}

// _____________________________________________________________________________
void Server::clearOldSessions() const {
  while (true) {
    std::this_thread::sleep_for(std::chrono::minutes(_cacheLifetime));

    std::vector<std::string> toDel;

    {
      std::lock_guard<std::mutex> guard(_m);
      for (const auto& i : _rs) {
        if (std::chrono::duration_cast<std::chrono::minutes>(
                std::chrono::system_clock::now() - i.second->createdAt())
                .count() >= _cacheLifetime) {
          toDel.push_back(i.first);
        }
      }
    }

    std::lock_guard<std::mutex> guard(_m);
    for (const auto& id : toDel) {
      clearSession(id);
    }
  }
}

// _____________________________________________________________________________
util::http::Answer Server::handleLoadStatusReq(const Params& pars,
                                               const HeaderParams& headers,
                                               int sock) const {
  if (pars.count("backend") == 0 || pars.find("backend")->second.empty())
    throw std::invalid_argument("No backend (?backend=) specified.");

  auto remoteAddr = remoteAddress(sock, headers);

  const std::string& backend = pars.find("backend")->second;

  auto backendCfg = getGeomCacheConfig(backend, "", "", remoteAddr);

  createCache(backendCfg);
  std::shared_ptr<GeomCache> cache = _caches[backendCfg.backend];

  // We have 3 loading stages:
  // 1) Filling geometry cache / reading cache from disk
  // 2) Fetching geometries
  // 3) Rendering result
  // 1) + 2) by GeomCache, 3) by Server
  // => Merge load status
  // 1) + 2) = 95%, 3) = 5%

  double geomCachePercent = 0.95;
  double serverPercent = 0.05;
  double geomCacheLoadStatusPercent = cache->getLoadStatusPercent(true);
  double serverLoadStatusPercent = getLoadStatusPercent();
  double totalPercent = geomCachePercent * geomCacheLoadStatusPercent +
                        serverPercent * serverLoadStatusPercent;

  int loadStatusStage = cache->getLoadStatusStage();
  size_t totalProgress = cache->getTotalProgress();
  size_t currentProgress = cache->getCurrentProgress();

  std::stringstream json;
  json << "{\"percent\": " << totalPercent << ", \"stage\": " << loadStatusStage
       << ", \"totalProgress\": " << totalProgress
       << ", \"currentProgress\": " << currentProgress << "}";
  util::http::Answer ans = util::http::Answer("200 OK", json.str());

  return ans;
}

// _____________________________________________________________________________
std::string Server::getFreeLayerId() const {
  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> d(
      1, std::numeric_limits<int>::max());

  return std::to_string(d(rng));
}

// _____________________________________________________________________________
std::string Server::getSessionId() const {
  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> d(
      1, std::numeric_limits<int>::max());

  return std::to_string(d(rng));
}

// _____________________________________________________________________________
double Server::getLoadStatusPercent() const {
  if (_totalSize == 0) return 0.0;

  double percent = _curRow / static_cast<double>(_totalSize) * 100.0;

  return percent;
}

// _____________________________________________________________________________
void Server::createCache(const GeomCacheConfig& cfg) const {
  std::shared_ptr<GeomCache> cache;

  {
    std::lock_guard<std::mutex> guard(_m);
    if (_caches.count(cfg.backend)) {
      cache = _caches[cfg.backend];
      // always set config
      if (cache->setConfig(cfg)) {
        LOG(INFO) << "Updating config for backend '" << cfg.backend
                  << "', new fill query is:\n"
                  << cfg.fillQuery;
      }
    } else {
      cache = std::shared_ptr<GeomCache>(new GeomCache(cfg, _maxMemory));
      _caches[cfg.backend] = cache;
    }
  }
}

// _____________________________________________________________________________
std::string Server::loadCache(const GeomCacheConfig& cfg) const {
  std::shared_ptr<GeomCache> cache = _caches[cfg.backend];

  try {
    return cache->load(_cacheDir);
  } catch (...) {
    std::lock_guard<std::mutex> guard(_m);

    auto it = _caches.find(cfg.backend);
    if (it != _caches.end()) _caches.erase(it);

    throw;
  }
}

// _____________________________________________________________________________
RequestorConfig Server::getRequestorCfgFromJSON(
    const std::string& jsonStr) const {
  RequestorConfig ret;

  try {
    nlohmann::json data = nlohmann::json::parse(jsonStr);

    if (data.is_object()) {
      for (const auto& cfg : data.items()) {
        if (cfg.key() == "query") {
          ret.query = cfg.value().get<std::string>();
        }
        if (cfg.key() == "layers") {
          for (const auto& layer : cfg.value().items()) {
            if (!layer.value().is_object()) {
              std::stringstream ss;
              ss << "Could not parse requestor config '" << jsonStr << "'";
              throw std::runtime_error(ss.str());
            }
            LayerConfig curField;
            if (layer.value().contains("id")) curField.id = layer.value()["id"];
            if (layer.value().contains("geomfield"))
              curField.geomField = layer.value()["geomfield"];
            if (layer.value().contains("name"))
              curField.name = layer.value()["name"];
            if (layer.value().contains("weightfield"))
              curField.valueField = layer.value()["weightfield"];
            if (layer.value().contains("rasterfield"))
              curField.rasterMetaField = layer.value()["rasterfield"];
            if (layer.value().contains("toggle"))
              curField.toggle = layer.value()["toggle"];
            if (layer.value().contains("rasterw"))
              curField.rasterW = layer.value()["rasterw"].get<double>();
            if (layer.value().contains("rasterh"))
              curField.rasterH = layer.value()["rasterh"].get<double>();
            if (layer.value().contains("color"))
              curField.color = layer.value()["color"].get<std::string>();
            if (layer.value().contains("colorscheme"))
              curField.colorscheme =
                  layer.value()["colorscheme"].get<std::string>();
            if (layer.value().contains("style"))
              curField.style = layer.value()["style"].get<std::string>();
            if (layer.value().contains("linew"))
              curField.objectStyle.lineWidth =
                  layer.value()["linew"].get<double>();
            if (layer.value().contains("fillopacity"))
              curField.objectStyle.fillOpacity =
                  layer.value()["fillopacity"].get<double>();
            if (layer.value().contains("lineopacity"))
              curField.objectStyle.lineOpacity =
                  layer.value()["lineopacity"].get<double>();
            if (layer.value().contains("pointradius"))
              curField.objectStyle.pointRadius =
                  layer.value()["pointradius"].get<double>();
            if (layer.value().contains("group"))
              curField.group = layer.value()["group"].get<std::string>();
            if (curField.name.size() == 0) curField.name = curField.geomField;

            // always assign an ID
            if (curField.id.size() == 0) curField.id = getFreeLayerId();
            ret.layers.push_back(curField);
          }
        }
      }
    }
  } catch (const std::runtime_error& e) {
    LOG(ERROR) << "[SERVER] " << e.what();
  }

  return ret;
}

// _____________________________________________________________________________
GeomCacheConfig Server::getGeomCacheCfgFromJSON(
    const std::string& backend, const std::string& jsonStr) const {
  GeomCacheConfig ret;

  try {
    nlohmann::json data = nlohmann::json::parse(jsonStr);

    if (data.is_object()) {
      for (const auto& i : data.items()) {
        if (i.key() == "fillQuery") {
          ret.fillQuery = i.value().get<std::string>();
        }
        if (i.key() == "rasterMetaQuery") {
          ret.rasterMetaQuery = i.value().get<std::string>();
        }
      }
    }
  } catch (const std::runtime_error& e) {
    LOG(ERROR) << "[SERVER] " << e.what();
  }

  ret.backend = backend;

  return ret;
}
// _____________________________________________________________________________
GeomCacheConfig Server::getGeomCacheConfig(
    const std::string& backendUrl, const std::string& accessToken,
    const std::string& configJson, const std::string& remoteAddr) const {
  std::string canonizedBackend;

  // first check if we have it cached
  {
    std::lock_guard<std::mutex> guard(_m);
    auto i = _canonizedURLCache.find(backendUrl);
    if (i != _canonizedURLCache.end()) {
      canonizedBackend = i->second;
    }
  }

  if (canonizedBackend.size() == 0) {
    // if not cache, perform the canonizeURL request lock-free
    canonizedBackend = canonizeURL(backendUrl, remoteAddr);

    // only lock for writing
    std::lock_guard<std::mutex> guard(_m);
    _canonizedURLCache[backendUrl] = canonizedBackend;
  }

  std::lock_guard<std::mutex> guard(_m);
  auto cfg = _cacheConfigs.find(canonizedBackend);
  if (cfg != _cacheConfigs.end()) {
    if (configJson.size()) {
      // always update, is no-op if already set
      auto backendCfg = getGeomCacheCfgFromJSON(canonizedBackend, configJson);
      if (_cacheConfigs[canonizedBackend] != backendCfg) {
        if (accessToken != _accessToken)
          throw std::invalid_argument("Invalid access token");
        _cacheConfigs[canonizedBackend] = backendCfg;
      }
    }

    return cfg->second;
  }

  if (accessToken != _accessToken) {
    std::stringstream ss;
    ss << "Backend not configured: " << backendUrl;
    throw std::runtime_error(ss.str());
  }

  if (configJson.size()) {
    _cacheConfigs[canonizedBackend] =
        getGeomCacheCfgFromJSON(canonizedBackend, configJson);
  } else {
    _cacheConfigs[canonizedBackend] = {
        canonizedBackend, petrimaps::getFillQuery(canonizedBackend)};
  }
  return _cacheConfigs[canonizedBackend];
}

// _____________________________________________________________________________
RequestorConfig Server::getDefaultRequestorCfg(const std::string& backend,
                                               const std::string& query) const {
  RequestorConfig ret;
  ret.query = query;

  auto cols = Requestor::getColumns(backend, query);
  if (cols.size() == 0) return ret;

  const std::vector<std::string> heatmapStyles{
      "spectralexp", "spectral",   "RdYlGn", "RdYlGnexp", "RdYlBu", "RdYlBuexp",
      "w2b",         "b2w",        "RdGy",   "RdGyexp",   "YlOrRd", "YlOrRdexp",
      "Blues",       "Bluesexp",   "Greens", "Greensexp", "Greys",  "Greysexp",
      "Oranges",     "Orangesexp", "Reds",   "Redsexp"};

  LayerConfig autoLayer;
  autoLayer.geomField = cols.back();
  autoLayer.id = "auto";
  autoLayer.name = "Auto";
  autoLayer.group = "Auto";
  autoLayer.color = "3388ff";
  autoLayer.style = "auto";

  LayerConfig objectLayer;
  objectLayer.geomField = cols.back();
  objectLayer.id = "objects";
  objectLayer.name = "Objects";
  objectLayer.group = "Objects";
  objectLayer.color = "3388ff";
  objectLayer.style = "objects";

  ret.layers.push_back(autoLayer);
  ret.layers.push_back(objectLayer);

  for (const auto& heatmapStyle : heatmapStyles) {
    LayerConfig heatLayer;
    heatLayer.geomField = cols.back();
    heatLayer.group = "Heatmap";
    heatLayer.id = std::string("heatmap-") + heatmapStyle;
    heatLayer.name = heatmapStyle;
    heatLayer.colorscheme = heatmapStyle;
    heatLayer.style = "heatmap";
    ret.layers.push_back(heatLayer);
  }

  return ret;
}

// _____________________________________________________________________________
int Server::hexToInt(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  return 0;
}
