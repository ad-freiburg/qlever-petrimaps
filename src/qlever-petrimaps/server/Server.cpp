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
#include <set>
#include <unordered_set>
#include <vector>

#include "3rdparty/heatmap.h"
#include "3rdparty/colorschemes/Spectral.h"
#include "qlever-petrimaps/build.h"
#include "qlever-petrimaps/index.h"
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

using petrimaps::Params;
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
               size_t autoThreshold)
    : _maxMemory(maxMemory),
      _cacheDir(cacheDir),
      _cacheLifetime(cacheLifetime),
      _autoThreshold(autoThreshold) {
  std::thread t(&Server::clearOldSessions, this);
  t.detach();
}

// _____________________________________________________________________________
util::http::Answer Server::handle(const util::http::Req& req, int con) const {
  UNUSED(con);

  // ignore SIGPIPE
  signal(SIGPIPE, SIG_IGN);

  util::http::Answer a;
  try {
    Params params;
    auto cmd = parseUrl(req.url, req.payload, &params);

    if (cmd == "/") {
      a = util::http::Answer(
          "200 OK",
          std::string(index_html,
                      index_html + sizeof index_html / sizeof index_html[0]));
      a.params["Content-Type"] = "text/html; charset=utf-8";
    } else if (cmd == "/query") {
      a = handleQueryReq(params);
    } else if (cmd == "/geojson") {
      a = handleGeoJSONReq(params);
    } else if (cmd == "/clearsession") {
      a = handleClearSessReq(params);
    } else if (cmd == "/clearsessions") {
      a = handleClearSessReq(params);
    } else if (cmd == "/load") {
      a = handleLoadReq(params);
    } else if (cmd == "/pos") {
      a = handlePosReq(params);
    } else if (cmd == "/export") {
      a = handleExportReq(params, con);
    } else if (cmd == "/loadstatus") {
      a = handleLoadStatusReq(params);
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
    throw std::invalid_argument("No bbox specified.");
  std::string id = pars.find("layers")->second;

  MapStyle style = HEATMAP;
  if (pars.count("styles") != 0 && !pars.find("styles")->second.empty()) {
    if (pars.find("styles")->second == "objects") style = OBJECTS;
  }

  if (box.size() != 4) throw std::invalid_argument("Invalid request.");

  std::shared_ptr<Requestor> r;
  {
    std::lock_guard<std::mutex> guard(_m);
    bool has = _rs.count(id);
    if (!has) {
      throw std::invalid_argument("Session not found");
    }
    r = _rs[id];
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

  int w = atoi(pars.find("width")->second.c_str());
  int h = atoi(pars.find("height")->second.c_str());

  double res = mercH / h;

  heatmap_t* hm = heatmap_new(w, h);

  double realCellSize = r->getPointGrid().getCellWidth();
  double virtCellSize = res * 2.5;

  size_t NUM_THREADS = std::thread::hardware_concurrency();

  size_t subCellSize = (size_t)ceil(realCellSize / virtCellSize);

  LOG(INFO) << "[SERVER] Query resolution: " << res;
  LOG(INFO) << "[SERVER] Virt cell size: " << virtCellSize;
  LOG(INFO) << "[SERVER] Num virt cells: " << subCellSize * subCellSize;

  std::vector<unsigned char> image(w * h * 4);

  std::vector<std::vector<uint32_t>> points(NUM_THREADS);
  std::vector<std::vector<double>> points2(NUM_THREADS);

  // initialize vectors to 0
  for (size_t i = 0; i < NUM_THREADS; i++) points2[i].resize(w * h, 0);

  // POINTS
  if (intersects(r->getPointGrid().getBBox(), fbbox)) {
    LOG(INFO) << "[SERVER] Looking up display points...";
    if (res < THRESHOLD) {
      std::vector<ID_TYPE> ret;

      // duplicates are not possible with points
      r->getPointGrid().get(fbbox, &ret);

      for (size_t j = 0; j < ret.size(); j++) {
        size_t i = ret[j];

        const auto& objs = r->getObjects();
        const auto& dynPoints = r->getDynamicPoints();

        if (i >= objs.size() + dynPoints.size() && style == OBJECTS) {
          size_t cid = i - objs.size() - dynPoints.size();
          FPoint p;
          size_t oid = r->getClusters()[cid].first;

          if (oid >= objs.size())
            p = dynPoints[oid - objs.size()].first;
          else
            p = r->getPoint(objs[oid].first);

          if (!contains(p, fbbox)) continue;

          const auto& cp = r->clusterGeom(cid, res);

          int px = ((cp.getX() - bbox.getLowerLeft().getX()) / mercW) * w;
          int py = h - ((cp.getY() - bbox.getLowerLeft().getY()) / mercH) * h;

          int ppx = ((p.getX() - bbox.getLowerLeft().getX()) / mercW) * w;
          int ppy = h - ((p.getY() - bbox.getLowerLeft().getY()) / mercH) * h;

          drawPoint(points[0], points2[0], px, py, w, h, style, 1);
          drawLine(image.data(), ppx, ppy, px, py, w, h);
        } else {
          if (i >= objs.size() + dynPoints.size())
            i = r->getClusters()[i - objs.size() - dynPoints.size()].first;

          FPoint p;
          if (i < objs.size())
            p = r->getPoint(objs[i].first);
          else
            p = r->getDPoint(i - objs.size());

          if (!contains(p, fbbox)) continue;

          int px = ((p.getX() - bbox.getLowerLeft().getX()) / mercW) * w;
          int py = h - ((p.getY() - bbox.getLowerLeft().getY()) / mercH) * h;

          drawPoint(points[0], points2[0], px, py, w, h, style, 1);
        }
      }
    } else {
      // they intersect, we checked this above
      auto iBox = intersection(r->getPointGrid().getBBox(), fbbox);
      const auto& grid = r->getPointGrid();

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
      for (size_t x = grid.getCellXFromX(iBox.getLowerLeft().getX());
           x <= grid.getCellXFromX(iBox.getUpperRight().getX()); x++) {
        for (size_t y = grid.getCellYFromY(iBox.getLowerLeft().getY());
             y <= grid.getCellYFromY(iBox.getUpperRight().getY()); y++) {
          if (x >= grid.getXWidth() || y >= grid.getYHeight()) {
            continue;
          }

          auto cell = grid.getCell(x, y);
          if (!cell || cell->size() == 0) continue;
          const auto& cellBox = grid.getBox(x, y);

          if (subCellSize == 1) {
            int px =
                ((cellBox.getLowerLeft().getX() - bbox.getLowerLeft().getX()) /
                 mercW) *
                w;
            int py =
                h -
                ((cellBox.getLowerLeft().getY() - bbox.getLowerLeft().getY()) /
                 mercH) *
                    h;

            drawPoint(points[omp_get_thread_num()],
                      points2[omp_get_thread_num()], px, py, w, h, style,
                      cell->size());
          } else {
            for (auto i : *cell) {
              if (i >= r->getObjects().size() + r->getDynamicPoints().size()) {
                i = r->getClusters()[i - r->getObjects().size() -
                                     r->getDynamicPoints().size()]
                        .first;
              }

              FPoint p;
              if (i < r->getObjects().size())
                p = r->getPoint(r->getObjects()[i].first);
              else
                p = r->getDPoint(i - r->getObjects().size());

              int px = ((p.getX() - bbox.getLowerLeft().getX()) / mercW) * w;
              int py =
                  h - ((p.getY() - bbox.getLowerLeft().getY()) / mercH) * h;
              drawPoint(points[omp_get_thread_num()],
                        points2[omp_get_thread_num()], px, py, w, h, style, 1);
            }
          }
        }
      }
    }
  }

  // LINES
  const auto& lgrid = r->getLineGrid();

  if (intersects(lgrid.getBBox(), fbbox)) {
    LOG(INFO) << "[SERVER] Looking up display lines...";
    if (res < THRESHOLD) {
      std::vector<ID_TYPE> ret;

      lgrid.get(fbbox, &ret);

      // sort to avoid duplicates
      std::sort(ret.begin(), ret.end());

      for (size_t idx = 0; idx < ret.size(); idx++) {
        if (idx > 0 && ret[idx] == ret[idx - 1]) continue;
        auto lid = r->getObjects()[ret[idx]].first;
        const auto& lbox = r->getLineBBox(lid - I_OFFSET);
        if (!intersects(lbox, bbox)) continue;

        size_t gi = 0;

        size_t start = r->getLine(lid - I_OFFSET);
        size_t end = r->getLineEnd(lid - I_OFFSET);

        // ___________________________________
        bool isects = false;

        DPoint curPa, curPb;
        int s = 0;

        double mainX = 0;
        double mainY = 0;
        for (size_t i = start; i < end; i++) {
          // extract real geom
          const auto& cur = r->getLinePoints()[i];

          if (isMCoord(cur.getX())) {
            mainX = rmCoord(cur.getX());
            mainY = rmCoord(cur.getY());
            continue;
          }

          // skip bounding box at beginning
          gi++;
          if (gi < 3) continue;

          // extract real geometry
          const DPoint curP((mainX * M_COORD_GRANULARITY + cur.getX()) / 10.0,
                            (mainY * M_COORD_GRANULARITY + cur.getY()) / 10.0);
          if (s == 0) {
            curPa = curP;
            s++;
          } else if (s == 1) {
            curPb = curP;
            s++;
          }

          if (s == 2) {
            s = 1;
            if (intersects(LineSegment<double>(curPa, curPb), bbox)) {
              isects = true;
              break;
            }
            curPa = curPb;
          }
        }
        // ___________________________________

        if (!isects) continue;

        // the factor depends on the render thickness of the line, make
        // this configurable!
        const auto& denseLine =
            densify(r->extractLineGeom(lid - I_OFFSET), res);

        for (const auto& p : denseLine) {
          int px = ((p.getX() - bbox.getLowerLeft().getX()) / mercW) * w;
          int py = h - ((p.getY() - bbox.getLowerLeft().getY()) / mercH) * h;

          if (px >= 0 && py >= 0 && px < w && py < h) {
            if (points2[0][w * py + px] == 0) points[0].push_back(w * py + px);
            points2[0][py * w + px] += 1;
          }
        }
      }
    } else {
      const auto& lpgrid = r->getLinePointGrid();
      auto iBox = intersection(lpgrid.getBBox(), fbbox);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
      for (size_t x = lpgrid.getCellXFromX(iBox.getLowerLeft().getX());
           x <= lpgrid.getCellXFromX(iBox.getUpperRight().getX()); x++) {
        for (size_t y = lpgrid.getCellYFromY(iBox.getLowerLeft().getY());
             y <= lpgrid.getCellYFromY(iBox.getUpperRight().getY()); y++) {
          if (x >= lpgrid.getXWidth() || y >= lpgrid.getYHeight()) continue;

          auto cell = lpgrid.getCell(x, y);
          if (!cell || cell->size() == 0) continue;
          const auto& cellBox = lpgrid.getBox(x, y);

          if (subCellSize == 1) {
            int px =
                ((cellBox.getLowerLeft().getX() - bbox.getLowerLeft().getX()) /
                 mercW) *
                w;
            int py =
                h -
                ((cellBox.getLowerLeft().getY() - bbox.getLowerLeft().getY()) /
                 mercH) *
                    h;
            if (px >= 0 && py >= 0 && px < w && py < h) {
              if (points2[omp_get_thread_num()][w * py + px] == 0)
                points[omp_get_thread_num()].push_back(w * py + px);
              points2[omp_get_thread_num()][py * w + px] += cell->size();
            }
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
              if (px >= 0 && py >= 0 && px < w && py < h) {
                if (points2[omp_get_thread_num()][w * py + px] == 0)
                  points[omp_get_thread_num()].push_back(w * py + px);
                points2[omp_get_thread_num()][py * w + px] += 1;
              }
            }
          }
        }
      }
    }
  }

  LOG(INFO) << "[SERVER] Adding points to heatmap...";

  if (style == OBJECTS) {
    auto stamp = heatmap_stamp_gen(3);
    for (size_t i = 0; i < NUM_THREADS; i++) {
      for (const auto& p : points[i]) {
        size_t y = p / w;
        size_t x = p - (y * w);
        if (points2[i][p] > 0)
          heatmap_add_weighted_point_with_stamp(hm, x, y, 1, stamp);
      }
    }
    heatmap_stamp_free(stamp);
  } else {
    for (size_t i = 0; i < NUM_THREADS; i++) {
      for (const auto& p : points[i]) {
        size_t y = p / w;
        size_t x = p - (y * w);
        if (points2[i][p] > 0)
          heatmap_add_weighted_point(hm, x, y, points2[i][p]);
      }
    }
  }

  LOG(INFO) << "[SERVER] ...done";
  LOG(INFO) << "[SERVER] Rendering heatmap...";

  if (style == OBJECTS) {
    static const unsigned char discrete_data[] = {
        0,   0,   0,   0,   0,   0,   0,   0,   51,  136, 255, 16,  51,  136,
        255, 32,  51,  136, 255, 64,  51,  136, 255, 128, 51,  136, 255, 160,
        51,  136, 255, 192, 51,  136, 255, 224, 51,  136, 255, 255};
    static const heatmap_colorscheme_t discrete = {
        discrete_data, sizeof(discrete_data) / sizeof(discrete_data[0] / 4)};

    heatmap_render_saturated_to(hm, &discrete, 1, &image[0]);
  } else {
    heatmap_render_to(hm, heatmap_cs_Spectral_mixed_exp, &image[0]);
  }

  heatmap_free(hm);

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
  ss << "HTTP/1.1 200 OK" << aw.status << "\r\n";
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

  writePNG(&image[0], w, h, sock);

  LOG(INFO) << "[SERVER] ...done";

  return aw;
}

// _____________________________________________________________________________
util::http::Answer Server::handleGeoJSONReq(const Params& pars) const {
  if (pars.count("id") == 0 || pars.find("id")->second.empty())
    throw std::invalid_argument("No session id (?id=) specified.");
  auto id = pars.find("id")->second;

  if (pars.count("rad") == 0 || pars.find("rad")->second.empty())
    throw std::invalid_argument("No rad (?rad=) specified.");
  auto rad = std::atof(pars.find("rad")->second.c_str());

  if (pars.count("gid") == 0 || pars.find("gid")->second.empty())
    throw std::invalid_argument("No geom id (?gid=) specified.");
  size_t gid = std::atoi(pars.find("gid")->second.c_str());

  bool noExport = pars.count("export") == 0 ||
                  pars.find("export")->second.empty() ||
                  !std::atoi(pars.find("export")->second.c_str());

  LOG(INFO) << "[SERVER] GeoJSON request for " << gid;

  std::shared_ptr<Requestor> reqor;

  {
    std::lock_guard<std::mutex> guard(_m);
    bool has = _rs.count(id);
    if (!has) {
      throw std::invalid_argument("Session not found");
    }
    reqor = _rs[id];
  }

  if (!reqor->ready()) {
    throw std::invalid_argument("Session not ready.");
  }
  // as soon as we are ready, the reqor can be read concurrently

  auto res = reqor->getGeom(gid, rad);

  util::json::Val dict;

  if (!noExport) {
    size_t row;
    if (gid < reqor->getObjects().size())
      row = reqor->getObjects()[gid].second;
    else
      row = reqor->getDynamicPoints()[gid].second;

    for (auto col : reqor->requestRow(row)) {
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
util::http::Answer Server::handlePosReq(const Params& pars) const {
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

  MapStyle style = HEATMAP;
  if (pars.count("styles") != 0 && !pars.find("styles")->second.empty()) {
    if (pars.find("styles")->second == "objects") style = OBJECTS;
  }

  if (box.size() != 4) throw std::invalid_argument("Invalid request.");

  double x1 = std::atof(box[0].c_str());
  double y1 = std::atof(box[1].c_str());
  double x2 = std::atof(box[2].c_str());
  double y2 = std::atof(box[3].c_str());
  double mercH = fabs(y2 - y1);

  auto fbbox = FBox({x1, y1}, {x2, y2});

  int h = atoi(pars.find("height")->second.c_str());

  double reso = mercH / h;

  // res of -1 means dont render clusters
  if (style == HEATMAP || reso >= THRESHOLD) reso = -1;

  LOG(DEBUG) << "[SERVER] Click at " << x << ", " << y;

  std::shared_ptr<Requestor> reqor;
  {
    std::lock_guard<std::mutex> guard(_m);
    bool has = _rs.count(id);
    if (!has) {
      throw std::invalid_argument("Session not found");
    }
    reqor = _rs[id];
  }

  if (!reqor->ready()) {
    throw std::invalid_argument("Session not ready.");
  }
  // as soon as we are ready, the reqor can be read concurrently

  auto res = reqor->getNearest({x, y}, rad, reso, fbbox);

  std::stringstream json;

  json << "[";

  if (res.has) {
    json << "{\"id\" :" << res.id;
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
util::http::Answer Server::handleClearSessReq(const Params& pars) const {
  std::string id;
  if (pars.count("id") != 0 && !pars.find("id")->second.empty())
    id = pars.find("id")->second;

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
util::http::Answer Server::handleLoadReq(const Params& pars) const {
  if (pars.count("backend") == 0 || pars.find("backend")->second.empty())
    throw std::invalid_argument("No backend (?backend=) specified.");
  auto backend = pars.find("backend")->second;

  LOG(INFO) << "[SERVER] Queried backend is " << backend;

  createCache(backend);
  loadCache(backend);

  auto answ = util::http::Answer("200 OK", "{}");
  answ.params["Content-Type"] = "application/json; charset=utf-8";
  return answ;
}

// _____________________________________________________________________________
util::http::Answer Server::handleQueryReq(const Params& pars) const {
  if (pars.count("query") == 0 || pars.find("query")->second.empty())
    throw std::invalid_argument("No query (?q=) specified.");
  if (pars.count("backend") == 0 || pars.find("backend")->second.empty())
    throw std::invalid_argument("No backend (?backend=) specified.");
  auto query = pars.find("query")->second;
  auto backend = pars.find("backend")->second;

  LOG(INFO) << "[SERVER] Queried backend is " << backend;
  LOG(INFO) << "[SERVER] Query is:\n" << query;

  createCache(backend);
  std::string indexHash = loadCache(backend);

  std::string queryId = backend + "$" + indexHash + "$" + query;

  std::shared_ptr<Requestor> reqor;
  std::string sessionId;

  {
    std::lock_guard<std::mutex> guard(_m);
    if (_queryCache.count(queryId)) {
      sessionId = _queryCache[queryId];
      reqor = _rs[sessionId];
    } else {
      reqor = std::shared_ptr<Requestor>(
          new Requestor(_caches[backend], _maxMemory));

      sessionId = getSessionId();

      _rs[sessionId] = reqor;
      if (util::toLower(query).find("rand()") == std::string::npos)
        _queryCache[queryId] = sessionId;
    }
  }

  try {
    reqor->request(query);
  } catch (OutOfMemoryError& ex) {
    LOG(ERROR) << ex.what() << backend;

    // delete cache, is now in unready state
    {
      std::lock_guard<std::mutex> guard(_m);
      clearSession(sessionId);
    }

    auto answ = util::http::Answer("406 Not Acceptable", ex.what());
    answ.params["Content-Type"] = "application/json; charset=utf-8";
    return answ;
  }

  auto bbox = reqor->getPointGrid().getBBox();
  bbox = extendBox(reqor->getLineGrid().getBBox(), bbox);

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
       << ",\"autothreshold\":" << _autoThreshold << "}";

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
  throw std::runtime_error(error_msg);
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

    for (const auto& i : _rs) {
      if (std::chrono::duration_cast<std::chrono::minutes>(
              std::chrono::system_clock::now() - i.second->createdAt())
              .count() >= 1) {
        toDel.push_back(i.first);
      }
    }

    std::lock_guard<std::mutex> guard(_m);
    for (const auto& id : toDel) {
      clearSession(id);
    }
  }
}

// _____________________________________________________________________________
util::http::Answer Server::handleExportReq(const Params& pars, int sock) const {
  // ignore SIGPIPE
  signal(SIGPIPE, SIG_IGN);

  auto aw = util::http::Answer("200 OK", "");

  if (pars.count("id") == 0 || pars.find("id")->second.empty())
    throw std::invalid_argument("No session id (?id=) specified.");
  auto id = pars.find("id")->second;

  std::shared_ptr<Requestor> reqor;

  {
    std::lock_guard<std::mutex> guard(_m);
    bool has = _rs.count(id);
    if (!has) {
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
  ss << "HTTP/1.1 200 OK" << aw.status << "\r\n";
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

          std::string wkt = row[row.size() - 1].second;
          if (wkt.size()) wkt[0] = ' ';  // drop " at beginning

          try {
            auto geom = util::geo::polygonFromWKT<double>(wkt);
            if (first) ss << ",";
            geoJsonOut.print(geom, dict);
            first = true;
          } catch (std::runtime_error& e) {
          }
          try {
            auto geom = util::geo::multiPolygonFromWKT<double>(wkt);
            if (first) ss << ",";
            geoJsonOut.print(geom, dict);
            first = true;
          } catch (std::runtime_error& e) {
          }
          try {
            auto geom = util::geo::pointFromWKT<double>(wkt);
            if (first) ss << ",";
            geoJsonOut.print(geom, dict);
            first = true;
          } catch (std::runtime_error& e) {
          }
          try {
            auto geom = util::geo::multiPointFromWKT<double>(wkt);
            if (first) ss << ",";
            geoJsonOut.print(geom, dict);
            first = true;
          } catch (std::runtime_error& e) {
          }
          try {
            auto geom = util::geo::lineFromWKT<double>(wkt);
            if (first) ss << ",";
            geoJsonOut.print(geom, dict);
            first = true;
          } catch (std::runtime_error& e) {
          }
          try {
            auto geom = util::geo::multiLineFromWKT<double>(wkt);
            if (first) ss << ",";
            geoJsonOut.print(geom, dict);
            first = true;
          } catch (std::runtime_error& e) {
          }
          try {
            auto geom = util::geo::collectionFromWKT<double>(wkt);
            if (first) ss << ",";
            geoJsonOut.print(geom, dict);
            first = true;
          } catch (std::runtime_error& e) {
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
      });

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
util::http::Answer Server::handleLoadStatusReq(const Params& pars) const {
  if (pars.count("backend") == 0 || pars.find("backend")->second.empty())
    throw std::invalid_argument("No backend (?backend=) specified.");
  auto backend = pars.find("backend")->second;
  createCache(backend);
  std::shared_ptr<GeomCache> cache = _caches[backend];

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
void Server::drawPoint(std::vector<uint32_t>& points,
                       std::vector<double>& points2, int px, int py, int w,
                       int h, MapStyle style, size_t num) const {
  if (style == OBJECTS) {
    // for the raw style, increase the size of the points a bit
    for (int x = px - 2; x < px + 2; x++) {
      for (int y = py - 2; y < py + 2; y++) {
        if (x >= 0 && y >= 0 && x < w && y < h) {
          if (points2[w * y + x] == 0) points.push_back(w * y + x);
          points2[w * y + x] += num;
        }
      }
    }
  } else {
    if (px >= 0 && py >= 0 && px < w && py < h) {
      if (points2[w * py + px] == 0) points.push_back(w * py + px);
      points2[w * py + px] += num;
    }
  }
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
  if (_totalSize == 0) {
    return 0.0;
  }

  double percent = _curRow / static_cast<double>(_totalSize) * 100.0;
  assert(percent <= 100.0);

  return percent;
}

// _____________________________________________________________________________
void Server::createCache(const std::string& backend) const {
  std::shared_ptr<GeomCache> cache;

  {
    std::lock_guard<std::mutex> guard(_m);
    if (_caches.count(backend)) {
      cache = _caches[backend];
    } else {
      cache = std::shared_ptr<GeomCache>(new GeomCache(backend));
      _caches[backend] = cache;
    }
  }
}

// _____________________________________________________________________________
std::string Server::loadCache(const std::string& backend) const {
  std::shared_ptr<GeomCache> cache = _caches[backend];

  try {
    return cache->load(_cacheDir);
  } catch (...) {
    std::lock_guard<std::mutex> guard(_m);

    auto it = _caches.find(backend);
    if (it != _caches.end()) _caches.erase(it);

    throw;
  }
}

// _____________________________________________________________________________
void Server::drawLine(unsigned char* image, int x0, int y0, int x1, int y1,
                      int w, int h) const {
  // Bresenham
  int dx = abs(x1 - x0);
  int sx = x0 < x1 ? 1 : -1;
  int dy = -abs(y1 - y0);
  int sy = y0 < y1 ? 1 : -1;
  int error = dx + dy;

  while (true) {
    if (x0 >= 0 && y0 >= 0 && x0 < w && y0 < h) {
      size_t coord = y0 * w * 4 + x0 * 4;
      image[coord] = 51;
      image[coord + 1] = 136;
      image[coord + 2] = 255;
      image[coord + 3] = 150;
    }

    if (x0 == x1 && y0 == y1) break;

    if (2 * error >= dy) {
      if (x0 == x1) break;
      error += dy;
      x0 += sx;
    }
    if (2 * error <= dx) {
      if (y0 == y1) break;
      error += dx;
      y0 += sy;
    }
  }
}
