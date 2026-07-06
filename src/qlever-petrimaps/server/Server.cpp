// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <png.h>
#include <sys/socket.h>

#include <algorithm>
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
using util::geo::LineSegment;
using util::geo::webMercToLatLng;

const static double THRESHOLD = 200;
static std::atomic<size_t> _curRow;

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
    } else if (cmd == "/geojson") {
      LOG(INFO) << "Geojson request from " << remoteAddress(con, req.params);
      a = handleGeoJSONReq(params, req.params, con);
    } else if (cmd == "/clearsession") {
      a = handleClearSessReq(params, req.params, con);
    } else if (cmd == "/clearsessions") {
      a = handleClearSessReq(params, req.params, con);
    } else if (cmd == "/pos") {
      LOG(INFO) << "Position request from " << remoteAddress(con, req.params);
      a = handlePosReq(params, req.params, con);
    } else if (cmd == "/export") {
      LOG(INFO) << "Export request from " << remoteAddress(con, req.params);
      a = handleExportReq(params, req.params, con);
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
  std::string layers = pars.find("layers")->second;

  std::string id;
  std::string field;

  auto parts = util::split(layers, '-');
  if (parts.size()) id = parts[0];
  if (parts.size() > 1) field = parts[1];

  MapStyle style = HEATMAP;
  auto colorScheme = heatmap_cs_Spectral_mixed_exp;
  double rasterWidth = 10;
  double rasterHeight = 10;

  int objColorR = 0, objColorG = 0, objColorB = 0;

  if (pars.count("styles") != 0 && !pars.find("styles")->second.empty()) {
    auto parts = util::split(pars.find("styles")->second, '-');

    if (parts[0] == "objects") style = OBJECTS;
    if (parts[0] == "raster") style = RASTER;

    if (style == RASTER && parts.size() > 1) {
      // in web mercator units (pseudometers)!
      auto xy = util::split(parts[1], 'x');
      if (xy.size() > 1) {
        rasterWidth = ::atof(xy[0].c_str());
        rasterHeight = ::atof(xy[1].c_str());
      }
    }

    if (style == OBJECTS && parts.size() > 1) {
      if (parts[1].size() == 6) {
        objColorR = hexToInt(parts[1][0]) * 16 + hexToInt(parts[1][1]);
        objColorG = hexToInt(parts[1][2]) * 16 + hexToInt(parts[1][3]);
        objColorB = hexToInt(parts[1][4]) * 16 + hexToInt(parts[1][5]);
      }
    }

    if (style == HEATMAP && parts.size() > 1) {
      if (parts[1] == "spectralexp")
        colorScheme = heatmap_cs_Spectral_mixed_exp;
      if (parts[1] == "spectral") colorScheme = heatmap_cs_Spectral_mixed;
      if (parts[1] == "RdYlGn") colorScheme = heatmap_cs_RdYlGn_mixed;
      if (parts[1] == "RdYlGnexp") colorScheme = heatmap_cs_RdYlGn_mixed_exp;
      if (parts[1] == "w2b") colorScheme = heatmap_cs_w2b_opaque;
      if (parts[1] == "b2w") colorScheme = heatmap_cs_b2w_opaque;
      if (parts[1] == "RdYlBu") colorScheme = heatmap_cs_RdYlBu_mixed;
      if (parts[1] == "RdGy") colorScheme = heatmap_cs_RdGy_mixed;
      if (parts[1] == "YlOrRd") colorScheme = heatmap_cs_YlOrRd_mixed;
      if (parts[1] == "Blues") colorScheme = heatmap_cs_Blues_mixed;
      if (parts[1] == "Greens") colorScheme = heatmap_cs_Greens_mixed;
      if (parts[1] == "Greys") colorScheme = heatmap_cs_Greys_mixed;
      if (parts[1] == "Oranges") colorScheme = heatmap_cs_Oranges_mixed;
      if (parts[1] == "Reds") colorScheme = heatmap_cs_Reds_mixed;

      if (parts[1] == "RdYlBuexp") colorScheme = heatmap_cs_RdYlBu_mixed_exp;
      if (parts[1] == "RdGyexp") colorScheme = heatmap_cs_RdGy_mixed_exp;
      if (parts[1] == "YlOrRdexp") colorScheme = heatmap_cs_YlOrRd_mixed_exp;
      if (parts[1] == "Bluesexp") colorScheme = heatmap_cs_Blues_mixed_exp;
      if (parts[1] == "Greensexp") colorScheme = heatmap_cs_Greens_mixed_exp;
      if (parts[1] == "Greysexp") colorScheme = heatmap_cs_Greys_mixed_exp;
      if (parts[1] == "Orangesexp") colorScheme = heatmap_cs_Oranges_mixed_exp;
      if (parts[1] == "Redsexp") colorScheme = heatmap_cs_Reds_mixed_exp;
    }

    if (style == RASTER && parts.size() > 2) {
      if (parts[2] == "spectral") colorScheme = heatmap_cs_Spectral_discrete;
      if (parts[2] == "RdYlGn") colorScheme = heatmap_cs_RdYlGn_discrete;
      if (parts[2] == "RdYlBu") colorScheme = heatmap_cs_RdYlBu_discrete;
      if (parts[2] == "RdGy") colorScheme = heatmap_cs_RdGy_discrete;
      if (parts[2] == "YlOrRd") colorScheme = heatmap_cs_YlOrRd_discrete;
      if (parts[2] == "Blues") colorScheme = heatmap_cs_Blues_discrete;
      if (parts[2] == "Greens") colorScheme = heatmap_cs_Greens_discrete;
      if (parts[2] == "Greys") colorScheme = heatmap_cs_Greys_discrete;
      if (parts[2] == "Oranges") colorScheme = heatmap_cs_Oranges_discrete;
      if (parts[2] == "Reds") colorScheme = heatmap_cs_Reds_discrete;
    }
  }

  if (box.size() != 4) throw std::invalid_argument("Invalid request.");

  std::shared_ptr<Requestor> r;
  {
    std::lock_guard<std::mutex> guard(_m);
    bool has = _rs.count(id);
    if (!has) {
      LOG(ERROR) << "Session " << id << " not found!";
      throw std::invalid_argument("Session not found");
    }
    r = _rs[id];
  }

  if (!r->ready()) {
    LOG(ERROR) << "Session " << id << " not ready!";
    throw std::invalid_argument("Session not ready.");
  }

  LOG(INFO) << "[SERVER] Begin heat for session " << id;

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

  size_t fid = r->getFieldId(field);

  checkMem(sizeof(float) * w * h, _maxMemory);
  double realCellSize = r->getPointGrid(fid).getCellWidth();
  double virtCellSize = res * 2.5;

  size_t NUM_THREADS = std::thread::hardware_concurrency();

  size_t subCellSize = (size_t)ceil(realCellSize / virtCellSize);

  LOG(INFO) << "[SERVER] Query resolution: " << res;
  LOG(INFO) << "[SERVER] Virt cell size: " << virtCellSize;
  LOG(INFO) << "[SERVER] Num virt cells: " << subCellSize * subCellSize;

  checkMem(sizeof(unsigned char) * w * h * 4 +
               sizeof(unsigned char) * w * h * 4 * NUM_THREADS * 2,
           _maxMemory);
  RenderContext rcontext(w, h, orx, ory, mercW, mercH, style, NUM_THREADS);

  // POINTS
  if (intersects(r->getPointGrid(fid).getBBox(), fbbox)) {
    LOG(INFO) << "[SERVER] Looking up display points...";
    if (res < THRESHOLD) {
      std::vector<ID_TYPE> ret;

      // duplicates are not possible with points, so no sorting here
      r->getPointGrid(fid).get(fbbox, &ret);

      for (size_t j = 0; j < ret.size(); j++) {
        size_t oid = ret[j];

        if (r->isCluster(fid, oid) && style == OBJECTS) {
          size_t refOid = r->getCluster(fid, oid).first;

          FPoint p = r->getPoint(fid, refOid);
          if (!contains(p, fbbox)) continue;

          const auto& cp = r->clusterGeom(fid, oid, res);

          auto px = RenderContext::mercToPx(cp, orx, ory, mercW, mercH, w, h);
          auto ppx = RenderContext::mercToPx(p, orx, ory, mercW, mercH, w, h);

          rcontext.drawPoint(0, px.getX(), px.getY(), r->getVal(fid, oid), 0, 0,
                             1);
          rcontext.drawLineSegment(px.getX(), px.getY(), ppx.getX(), ppx.getY(),
                                   w, h);
        } else {
          if (r->isCluster(fid, oid)) oid = r->getCluster(fid, oid).first;

          FPoint p = r->getPoint(fid, oid);
          if (!contains(p, fbbox)) continue;

          auto px = RenderContext::mercToPx(p, orx, ory, mercW, mercH, w, h);

          if (style == RASTER) {
            auto rasterMeta =
                r->getRasterMetas(fid, oid, {rasterWidth, rasterHeight});
            rcontext.drawPoint(0, px.getX(), px.getY(), r->getVal(fid, oid),
                               rasterMeta.first, rasterMeta.second, 1);
          } else {
            rcontext.drawPoint(0, px.getX(), px.getY(), r->getVal(fid, oid), 0,
                               0, 1);
          }
        }
      }
    } else {
      // they intersect, we checked this above
      auto iBox = intersection(r->getPointGrid(fid).getBBox(), fbbox);
      const auto& grid = r->getPointGrid(fid);

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
                               1, 1, 1);
          } else {
            for (auto oid : *cell) {
              if (r->isCluster(fid, oid)) oid = r->getCluster(fid, oid).first;

              FPoint p = r->getPoint(fid, oid);
              auto px =
                  RenderContext::mercToPx(p, orx, ory, mercW, mercH, w, h);

              if (style == RASTER) {
                auto rasterMeta =
                    r->getRasterMetas(fid, oid, {rasterWidth, rasterHeight});
                rcontext.drawPoint(tid, px.getX(), px.getY(),
                                   r->getVal(fid, oid), rasterMeta.first,
                                   rasterMeta.second, 1);
              } else {
                rcontext.drawPoint(tid, px.getX(), px.getY(),
                                   r->getVal(fid, oid), 0, 0, 1);
              }
            }
          }
        }
      }
    }
  }

  // LINES
  const auto& lgrid = r->getLineGrid(fid);

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
        auto lineId = r->getObjects(fid)[ret[idx]].first;
        auto oid = r->getObjects(fid)[ret[idx]].second;
        if (!util::geo::intersects(r->getLineBBox(lineId - I_OFFSET), bbox))
          continue;

        if (r->isArea(lineId - I_OFFSET) &&
            !r->isInnerArea(lineId - I_OFFSET)) {
          rcontext.drawArea(0, r->extractLineGeom(lineId - I_OFFSET, 3 * res),
                            r->getVal(fid, oid));
        } else if (r->isArea(lineId - I_OFFSET) &&
                   r->isInnerArea(lineId - I_OFFSET)) {
          rcontext.drawArea(0, r->extractLineGeom(lineId - I_OFFSET, 3 * res),
                            r->getVal(fid, oid), true, true);
        } else {
          if (!r->lineIntersects(lineId, bbox)) continue;
          rcontext.drawLine(0, r->extractLineGeom(lineId - I_OFFSET, 3 * res),
                            r->getVal(fid, oid));
        }
      }
    } else {
      const auto& lpgrid = r->getLinePointGrid(fid);
      const auto& agrid = r->getAreaGrid(fid);
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
            rcontext.drawPoint(tid, pix.getX(), pix.getY(),
                               lpgrid.getCellSum(x, y), rasterWidth,
                               rasterHeight, 0);
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
              rcontext.drawPoint(tid, px, py, 1, rasterWidth, rasterHeight, 0);
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
        auto lineId = r->getObjects(fid)[ret[idx]].first;
        auto oid = r->getObjects(fid)[ret[idx]].second;
        auto geom = r->extractLineGeom(lineId - I_OFFSET, res);
        if (r->isInnerArea(lineId - I_OFFSET)) {
          rcontext.drawArea(0, geom, r->getVal(fid, oid), true, true);
        } else {
          rcontext.drawArea(0, geom, r->getVal(fid, oid), true);
        }
      }
    }
  }

  LOG(INFO) << "[SERVER] Adding points to heatmap...";
  heatmap_t* hm = heatmap_new(w, h);
  heatmap_t* hmInterior = heatmap_new(w, h);
  hm->max = r->getValRange(fid).second;

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
        0,         0,         0,         0,         0,         0,
        0,         0,         objColorR, objColorG, objColorB, 8,
        objColorR, objColorG, objColorB, 16,        objColorR, objColorG,
        objColorB, 32,        objColorR, objColorG, objColorB, 64,
        objColorR, objColorG, objColorB, 80,        objColorR, objColorG,
        objColorB, 96,        objColorR, objColorG, objColorB, 112,
        objColorR, objColorG, objColorB, 127};
    heatmap_colorscheme_t fillColorScheme = {
        fillColors, sizeof(fillColors) / sizeof(fillColors[0]) / 4};

    heatmap_render_saturated_to(hmInterior, &fillColorScheme, 1,
                                &rcontext.getImage()[0]);

    unsigned char borderColors[] = {
        0,         0,         0,         0,         0,         0,
        0,         0,         objColorR, objColorG, objColorB, 64,
        objColorR, objColorG, objColorB, 64,        objColorR, objColorG,
        objColorB, 64,        objColorR, objColorG, objColorB, 128,
        objColorR, objColorG, objColorB, 160,       objColorR, objColorG,
        objColorB, 192,       objColorR, objColorG, objColorB, 224,
        objColorR, objColorG, objColorB, 255};
    heatmap_colorscheme_t borderColorScheme = {
        borderColors, sizeof(borderColors) / sizeof(borderColors[0]) / 4};

    heatmap_render_saturated_to(hm, &borderColorScheme, 1,
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

// _____________________________________________________________________________
util::http::Answer Server::handleGeoJSONReq(const Params& pars,
                                            const HeaderParams& headers,
                                            int sock) const {
  auto remoteAddr = remoteAddress(sock, headers);

  if (pars.count("id") == 0 || pars.find("id")->second.empty())
    throw std::invalid_argument("No session id (?id=) specified.");
  auto id = pars.find("id")->second;

  if (pars.count("rad") == 0 || pars.find("rad")->second.empty())
    throw std::invalid_argument("No rad (?rad=) specified.");
  auto rad = std::atof(pars.find("rad")->second.c_str());

  if (pars.count("gid") == 0 || pars.find("gid")->second.empty())
    throw std::invalid_argument("No geom id (?gid=) specified.");
  size_t gid = std::atoi(pars.find("gid")->second.c_str());

  if (pars.count("layer") == 0 || pars.find("layer")->second.empty())
    throw std::invalid_argument("No layer (?layer=) specified.");
  std::string layer = pars.find("layer")->second.c_str();

  bool noExport = pars.count("export") == 0 ||
                  pars.find("export")->second.empty() ||
                  !std::atoi(pars.find("export")->second.c_str());

  LOG(INFO) << "[SERVER] GeoJSON request for " << gid;

  std::shared_ptr<Requestor> reqor;

  {
    std::lock_guard<std::mutex> guard(_m);
    bool has = _rs.count(id);
    if (!has) {
      LOG(ERROR) << "Session " << id << " not found!";
      throw std::invalid_argument("Session not found");
    }
    reqor = _rs[id];
  }

  if (!reqor->ready()) {
    throw std::invalid_argument("Session not ready.");
  }

  size_t fid = reqor->getFieldId(layer);

  // as soon as we are ready, the reqor can be read concurrently
  auto res = reqor->getGeom(fid, gid, rad);

  util::json::Val dict;

  if (!noExport) {
    size_t row;
    if (gid < reqor->getObjects(fid).size()) {
      row = reqor->getObjects(fid)[gid].second;
    } else if (gid - reqor->getObjects(fid).size() <
               reqor->getDynamicPoints(fid).size()) {
      row = reqor->getDynamicPoints(fid)[gid - reqor->getObjects(fid).size()]
                .second;
    } else {
      throw std::invalid_argument("Invalid request.");
    }

    for (auto col : reqor->requestRow(row, remoteAddr)) {
      dict.dict[col.first] = col.second;
    }
  }

  std::stringstream json;

  if ((res.poly.size() != 0) + (res.point.size() != 0) +
          (res.line.size() != 0) >
      1) {
    util::geo::Collection<double> col;
    col.push_back(res.poly);
    col.push_back(res.line);
    col.push_back(res.point);

    GeoJsonOutput out(json);
    out.printLatLng(col, dict);
  } else if (res.poly.size()) {
    GeoJsonOutput out(json);
    out.printLatLng(res.poly, dict);
  } else if (res.line.size()) {
    GeoJsonOutput out(json);
    out.printLatLng(res.line, dict);
  } else {
    GeoJsonOutput out(json);
    out.printLatLng(res.point, dict);
  }

  auto answ = util::http::Answer("200 OK", json.str());
  answ.params["Content-Type"] = "application/json; charset=utf-8";

  if (!noExport) {
    answ.params["Content-Disposition"] = "attachment;filename:\"export.json\"";
  }

  return answ;
}

// _____________________________________________________________________________
util::http::Answer Server::handlePosReq(const Params& pars,
                                        const HeaderParams& headers,
                                        int sock) const {
  auto remoteAddr = remoteAddress(sock, headers);

  if (pars.count("x") == 0 || pars.find("x")->second.empty())
    throw std::invalid_argument("No x coord (?x=) specified.");
  float x = std::atof(pars.find("x")->second.c_str());

  if (pars.count("y") == 0 || pars.find("y")->second.empty())
    throw std::invalid_argument("No y coord (?y=) specified.");
  float y = std::atof(pars.find("y")->second.c_str());

  if (pars.count("id") == 0 || pars.find("id")->second.empty())
    throw std::invalid_argument("No session id (?id=) specified.");
  auto id = pars.find("id")->second;

  if (pars.count("rad") == 0 || pars.find("rad")->second.empty())
    throw std::invalid_argument("No rad (?rad=) specified.");
  auto rad = std::atof(pars.find("rad")->second.c_str());

  if (pars.count("width") == 0 || pars.find("width")->second.empty())
    throw std::invalid_argument("No width (?width=) specified.");
  if (pars.count("height") == 0 || pars.find("height")->second.empty())
    throw std::invalid_argument("No height (?height=) specified.");

  if (pars.count("bbox") == 0 || pars.find("bbox")->second.empty())
    throw std::invalid_argument("No bbox specified.");
  auto box = util::split(pars.find("bbox")->second, ',');

  if (box.size() != 4) throw std::invalid_argument("Invalid request.");

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

  LOG(DEBUG) << "[SERVER] Click at " << x << ", " << y;

  std::shared_ptr<Requestor> reqor;
  {
    std::lock_guard<std::mutex> guard(_m);
    bool has = _rs.count(id);
    if (!has) {
      LOG(ERROR) << "Session " << id << " not found!";
      throw std::invalid_argument("Session not found");
    }
    reqor = _rs[id];
  }

  if (!reqor->ready()) {
    throw std::invalid_argument("Session not ready.");
  }
  // as soon as we are ready, the reqor can be read concurrently

  auto res = reqor->getNearest({x, y}, rad, reso, fbbox, remoteAddr);

  std::stringstream json;

  json << "[";

  if (res.has) {
    json << "{\"id\" :" << res.id;
    json << ",\"geomfield\" :\"" << reqor->getFields()[res.fieldId].geomField
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
  std::shared_ptr<GeomCache> cache = _caches[backend];

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

  // backwards compatibility
  if (pars.count("fields") != 0) {
    for (auto raw : util::split(pars.find("fields")->second, ';')) {
      auto parts = util::split(raw, ',');
      if (parts.size() == 0) continue;
      rcfg.fields.push_back({
          parts[0],                          // geomField
          getFreeLayerId(),                  // id
          "",                                // name
          parts.size() > 1 ? parts[1] : "",  // valueField
                                             // ..., rest defaults
      });
    }
  }

  if (pars.count("rasterw") != 0 && pars.count("rasterh") != 0) {
    double rasterW = ::atof(pars.find("rasterw")->second.c_str());
    double rasterH = ::atof(pars.find("rasterh")->second.c_str());

    // set the same rasterw and rasterh for all fields
    for (auto& fld : rcfg.fields) {
      fld.rasterW = rasterW;
      fld.rasterH = rasterH;
    }
  }

  if (pars.count("cfg") != 0 && !pars.find("cfg")->second.empty()) {
    rcfg = getRequestorCfgFromJSON(pars.find("cfg")->second);
  }

  if (pars.count("query") != 0 && !pars.find("query")->second.empty()) {
    rcfg.query = pars.find("query")->second;
  }

  if (rcfg.query.size() == 0)
    throw std::invalid_argument("No query specified.");

  const std::string& backend = pars.find("backend")->second;

  auto backendCfg = getGeomCacheConfig(backend, "", "", remoteAddr);

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

  for (size_t fid = 0; fid < reqor->getNumFields(); fid++) {
    bbox = extendBox(reqor->getPointGrid(fid).getBBox(), bbox);
    bbox = extendBox(reqor->getLineGrid(fid).getBBox(), bbox);
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
       << ",\"numobjects\":" << numObjs
       << ",\"autothreshold\":" << _autoThreshold << ",\"layers\": [";

  bool first = false;
  for (const auto& fld : reqor->getFields()) {
    if (first) json << ",";
    first = true;
    json << "{";
    json << "\"id\":\"" << fld.id << "\",";
    json << "\"geomfield\":\"" << fld.geomField << "\",";
    json << "\"name\":\"" << fld.name << "\",";
    json << "\"color\":\"" << fld.color << "\",";
    json << "\"colorscheme\":\"" << fld.colorscheme << "\",";
    json << "\"numobjects\":\""
         << reqor->getNumObjects(reqor->getFieldId(fld.geomField)) << "\",";
    json << "\"style\":\"" << fld.style << "\",";
    json << "\"toggle\":\"" << fld.toggle << "\"";
    if (fld.rasterW != 0 && fld.rasterH != 0)
      json << ",\"rasterw\":" << fld.rasterW << ", \"rasterh\":" << fld.rasterH;
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
util::http::Answer Server::handleExportReq(const Params& pars,
                                           const HeaderParams& headers,
                                           int sock) const {
  // ignore SIGPIPE
  signal(SIGPIPE, SIG_IGN);

  auto aw = util::http::Answer("200 OK", "");

  auto remoteAddr = remoteAddress(sock, headers);

  if (pars.count("id") == 0 || pars.find("id")->second.empty())
    throw std::invalid_argument("No session id (?id=) specified.");
  auto id = pars.find("id")->second;

  std::shared_ptr<Requestor> reqor;

  {
    std::lock_guard<std::mutex> guard(_m);
    bool has = _rs.count(id);
    if (!has) {
      LOG(ERROR) << "Session " << id << " not found!";
      throw std::invalid_argument("Session not found");
    }
    reqor = _rs[id];
  }

  if (!reqor->ready()) {
    throw std::invalid_argument("Session not ready.");
  }
  // as soon as we are ready, the reqor can be read concurrently

  aw.params["Content-Encoding"] = "identity";
  aw.params["Content-Type"] = "application/json";
  aw.params["Content-Disposition"] = "attachment;filename:\"export.json\"";
  aw.params["Server"] = "qlever-petrimaps";

  // we do not set the Content-Length header here, but serve until
  // we are done. In particular, we do not need to send our data in chunks, as
  // specified by https://www.rfc-editor.org/rfc/rfc7230#section-3.3.3
  // point 7

  std::stringstream ss;
  ss << "HTTP/1.1 " << aw.status << "\r\n";
  for (const auto& kv : aw.params)
    ss << kv.first << ": " << kv.second << "\r\n";

  ss << "\r\n";
  ss << "{\"type\":\"FeatureCollection\",\"features\":[";

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

  bool first = false;

  reqor->requestRows(
      [sock, &first](
          std::vector<std::vector<std::pair<std::string, std::string>>> rows) {
        std::stringstream ss;
        ss << std::setprecision(10);

        util::json::Val dict;

        for (const auto& row : rows) {
          // skip last entry, which is the WKT
          for (size_t i = 0; i < row.size() - 1; i++) {
            dict.dict[row[i].first] = row[i].second;
          }

          GeoJsonOutput geoJsonOut(ss, true);

          const char* s = row[row.size() - 1].second.c_str();

          if (*s == '"') s++;  // drop " at beginning

          auto wktType = util::geo::getWKTType(s, &s);

          if (wktType != util::geo::WKTType::NONE) {
            if (first) ss << ",";
            first = true;
          }

          if (wktType == util::geo::WKTType::POLYGON) {
            geoJsonOut.print(util::geo::polygonFromWKT<double>(s, 0), dict);
          }
          if (wktType == util::geo::WKTType::MULTIPOLYGON) {
            geoJsonOut.print(util::geo::multiPolygonFromWKT<double>(s, 0),
                             dict);
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
          ss << "\n";
        }

        std::string buff = ss.str();

        size_t writes = 0;

        while (writes != buff.size()) {
          int64_t out = send(sock, buff.c_str() + writes, buff.size() - writes,
                             MSG_NOSIGNAL);
          if (out < 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR)
              continue;
            throw std::runtime_error("Failed to write to socket");
          }
          writes += out;
        }
      },
      remoteAddr);

  buff = "]}";
  writes = 0;

  while (writes != buff.size()) {
    int64_t out =
        send(sock, buff.c_str() + writes, buff.size() - writes, MSG_NOSIGNAL);
    if (out < 0) {
      if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR) continue;
      throw std::runtime_error("Failed to write to socket");
    }
    writes += out;
  }

  aw.raw = true;
  return aw;
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

  std::multiset<std::string> geomFields;

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
            FieldConfig curField;
            if (layer.value().contains("id")) curField.id = layer.value()["id"];
            if (layer.value().contains("geomfield")) {
              curField.geomField = layer.value()["geomfield"];
              if (geomFields.count(curField.geomField)) {
                geomFields.insert(curField.geomField);
                curField.geomField =
                    std::string(layer.value()["geomfield"]) + ":" +
                    std::to_string(geomFields.count(curField.geomField));
              } else {
                geomFields.insert(curField.geomField);
              }
            }
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
            if (curField.name.size() == 0) curField.name = curField.geomField;
            if (curField.id.size() == 0) curField.id = getFreeLayerId();
            ret.fields.push_back(curField);
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
int Server::hexToInt(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  return 0;
}
