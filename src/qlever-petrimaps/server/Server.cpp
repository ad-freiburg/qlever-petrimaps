// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <omp.h>
#include <png.h>

#include <chrono>
#include <codecvt>
#include <csignal>
#include <locale>
#include <parallel/algorithm>
#include <random>
#include <set>
#include <unordered_set>
#include <vector>

#include "3rdparty/colorschemes/Spectral.h"
#include "3rdparty/heatmap.h"
#include "qlever-petrimaps/build.h"
#include "qlever-petrimaps/index.h"
#include "qlever-petrimaps/server/Requestor.h"
#include "qlever-petrimaps/server/Server.h"
#include "util/Misc.h"
#include "util/String.h"
#include "util/geo/Geo.h"
#include "util/geo/output/GeoJsonOutput.cpp"
#include "util/http/Server.h"
#include "util/log/Log.h"

using petrimaps::Params;
using petrimaps::Server;

// _____________________________________________________________________________
Server::Server(size_t maxMemory, const std::string& cacheDir, int cacheLifetime)
    : _maxMemory(maxMemory),
      _cacheDir(cacheDir),
      _cacheLifetime(cacheLifetime) {
  std::thread t(&Server::clearOldSessions, this);
  t.detach();
}

// _____________________________________________________________________________
util::http::Answer Server::handle(const util::http::Req& req, int con) const {
  UNUSED(con);
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
    } else if (cmd == "/build.js") {
      a = util::http::Answer(
          "200 OK", std::string(build_js, build_js + sizeof build_js /
                                                         sizeof build_js[0]));
      a.params["Content-Type"] = "application/javascript; charset=utf-8";
      a.params["Cache-Control"] = "public, max-age=10000";
    } else if (cmd == "/heatmap") {
      a = handleHeatMapReq(params);
    } else {
      a = util::http::Answer("404 Not Found", "dunno");
    }
  } catch (const std::runtime_error& e) {
    a = util::http::Answer("400 Bad Request", e.what());
  } catch (const std::invalid_argument& e) {
    a = util::http::Answer("400 Bad Request", e.what());
  } catch (const std::exception& e) {
    a = util::http::Answer("500 Internal Server Error", e.what());
  } catch (...) {
    a = util::http::Answer("500 Internal Server Error",
                           "Internal Server Error.");
  }

  a.params["Access-Control-Allow-Origin"] = "*";
  a.params["Server"] = "qlever-petrimaps-middleend";

  return a;
}

// _____________________________________________________________________________
util::http::Answer Server::handleHeatMapReq(const Params& pars) const {
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

  if (box.size() != 4) throw std::invalid_argument("Invalid request.");

  _m.lock();
  bool has = _rs.count(id);
  if (!has) {
    _m.unlock();
    throw std::invalid_argument("Session not found");
  }
  auto r = _rs[id];
  _m.unlock();

  LOG(INFO) << "[SERVER] Begin heat for session " << id;

  float x1 = std::atof(box[0].c_str());
  float y1 = std::atof(box[1].c_str());
  float x2 = std::atof(box[2].c_str());
  float y2 = std::atof(box[3].c_str());

  double mercW = fabs(x2 - x1);
  double mercH = fabs(y2 - y1);

  auto bbox = util::geo::FBox({x1, y1}, {x2, y2});

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

  std::vector<std::vector<size_t>> points(NUM_THREADS);

  std::vector<std::vector<float>> points2(NUM_THREADS);

  // initialize vectors to 0
  for (size_t i = 0; i < NUM_THREADS; i++) {
    points2[i].resize(w * h, 0);
  }

  double THRESHOLD = 200;

  if (util::geo::intersects(r->getPointGrid().getBBox(), bbox)) {
    LOG(INFO) << "[SERVER] Looking up display points...";
    if (res < THRESHOLD) {
      std::vector<ID_TYPE> ret;

      // duplicates are not possible with points
      r->getPointGrid().get(bbox, &ret);

      for (size_t i : ret) {
        const auto& p = r->getPoint(r->getObjects()[i].first);
        if (!util::geo::contains(p, bbox)) continue;

        int px = ((p.getX() - bbox.getLowerLeft().getX()) / mercW) * w;
        int py = h - ((p.getY() - bbox.getLowerLeft().getY()) / mercH) * h;

        if (px >= 0 && py >= 0 && px < w && py < h) {
          if (points2[0][w * py + px] == 0) points[0].push_back(w * py + px);
          points2[0][w * py + px] += 1;
        }
      }
    } else {
      // they intersect, we checked this above
      auto iBox = util::geo::intersection(r->getPointGrid().getBBox(), bbox);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
      for (size_t x =
               r->getPointGrid().getCellXFromX(iBox.getLowerLeft().getX());
           x <= r->getPointGrid().getCellXFromX(iBox.getUpperRight().getX());
           x++) {
        for (size_t y =
                 r->getPointGrid().getCellYFromY(iBox.getLowerLeft().getY());
             y <= r->getPointGrid().getCellYFromY(iBox.getUpperRight().getY());
             y++) {
          if (x >= r->getPointGrid().getXWidth() ||
              y >= r->getPointGrid().getYHeight()) {
            continue;
          }
          auto cell = r->getPointGrid().getCell(x, y);
          if (!cell || cell->size() == 0) continue;
          const auto& cellBox = r->getPointGrid().getBox(x, y);

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
            for (const auto& i : *cell) {
              const auto& p = r->getPoint(r->getObjects()[i].first);

              int px = ((p.getX() - bbox.getLowerLeft().getX()) / mercW) * w;
              int py =
                  h - ((p.getY() - bbox.getLowerLeft().getY()) / mercH) * h;
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

  // LINES

  if (util::geo::intersects(r->getLineGrid().getBBox(), bbox)) {
    LOG(INFO) << "[SERVER] Looking up display lines...";
    if (res < THRESHOLD) {
      std::vector<ID_TYPE> ret;

      r->getLineGrid().get(bbox, &ret);

      // sort to avoid duplicates
      __gnu_parallel::sort(ret.begin(), ret.end());

      for (size_t idx = 0; idx < ret.size(); idx++) {
        if (idx > 0 && ret[idx] == ret[idx - 1]) continue;
        auto lid = r->getObjects()[ret[idx]].first;
        const auto& lbox = r->getLineBBox(lid - I_OFFSET);
        if (!util::geo::intersects(lbox, bbox)) continue;

        uint8_t gi = 0;

        size_t start = r->getLine(lid - I_OFFSET);
        size_t end = r->getLineEnd(lid - I_OFFSET);

        // ___________________________________
        bool isects = false;

        util::geo::FPoint curPa, curPb;
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
          const util::geo::FPoint curP(
              mainX * M_COORD_GRANULARITY + cur.getX(),
              mainY * M_COORD_GRANULARITY + cur.getY());
          if (s == 0) {
            curPa = curP;
            s++;
          } else if (s == 1) {
            curPb = curP;
            s++;
          }

          if (s == 2) {
            s = 1;
            if (util::geo::intersects(
                    util::geo::LineSegment<float>(curPa, curPb), bbox)) {
              isects = true;
              break;
            }
            curPa = curPb;
          }
        }
        // ___________________________________

        if (!isects) continue;

        mainX = 0;
        mainY = 0;

        util::geo::DLine extrLine;
        extrLine.reserve(end - start);

        gi = 0;

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

          util::geo::DPoint p(mainX * M_COORD_GRANULARITY + cur.getX(),
                              mainY * M_COORD_GRANULARITY + cur.getY());
          extrLine.push_back(p);
        }

        // the factor depends on the render thickness of the line, make
        // this configurable!
        const auto& denseLine = util::geo::densify(extrLine, res * 3);

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
      auto iBox =
          util::geo::intersection(r->getLinePointGrid().getBBox(), bbox);

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
      for (size_t x =
               r->getLinePointGrid().getCellXFromX(iBox.getLowerLeft().getX());
           x <=
           r->getLinePointGrid().getCellXFromX(iBox.getUpperRight().getX());
           x++) {
        for (size_t y = r->getLinePointGrid().getCellYFromY(
                 iBox.getLowerLeft().getY());
             y <=
             r->getLinePointGrid().getCellYFromY(iBox.getUpperRight().getY());
             y++) {
          if (x >= r->getLinePointGrid().getXWidth() ||
              y >= r->getLinePointGrid().getYHeight())
            continue;

          auto cell = r->getLinePointGrid().getCell(x, y);
          if (!cell || cell->size() == 0) continue;
          const auto& cellBox = r->getLinePointGrid().getBox(x, y);

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

  for (size_t i = 0; i < NUM_THREADS; i++) {
    for (const auto& p : points[i]) {
      size_t y = p / w;
      size_t x = p - (y * w);
      heatmap_add_weighted_point(hm, x, y, points2[i][p]);
    }
  }

  LOG(INFO) << "[SERVER] ...done";
  LOG(INFO) << "[SERVER] Rendering heatmap...";

  std::vector<unsigned char> image(w * h * 4);
  // double sat = 5 * pow(res, 1.3);
  // heatmapr.nder_saturated_to(hm, heatmap_cs_Spectral_mixed_exp, sat,
  // &image[0]);
  heatmap_render_to(hm, heatmap_cs_Spectral_mixed_exp, &image[0]);

  LOG(INFO) << "[SERVER] ...done";
  LOG(INFO) << "[SERVER] Generating PNG...";

  auto answ = util::http::Answer("200 OK", writePNG(&image[0], w, h));
  answ.params["Content-Type"] = "image/png";

  LOG(INFO) << "[SERVER] ...done";

  heatmap_free(hm);

  return answ;
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
  auto gid = std::atoi(pars.find("gid")->second.c_str());

  bool noExport = pars.count("export") == 0 ||
                  pars.find("export")->second.empty() ||
                  !std::atoi(pars.find("export")->second.c_str());

  LOG(INFO) << "[SERVER] GeoJSON request for " << gid;

  _m.lock();
  bool has = _rs.count(id);
  if (!has) {
    _m.unlock();
    throw std::invalid_argument("Session not found");
  }

  // NOTE: reqor is a shared pointer here, so it doesnt
  // matter if _rs is cleared after unlock
  auto reqor = _rs[id];
  _m.unlock();

  // TODO: use concurrent read / exclusive write lock here
  reqor->getMutex().lock();
  auto res = reqor->getGeom(gid, rad);
  reqor->getMutex().unlock();

  util::json::Val dict;

  if (!noExport) {
    for (auto col : reqor->requestRow(reqor->getObjects()[gid].second)) {
      dict.dict[col.first] = col.second;
    }
  }

  std::stringstream json;

  if (res.poly.getOuter().size()) {
    GeoJsonOutput out(json);
    out.printLatLng(res.poly, dict);
  } else if (res.line.size()) {
    GeoJsonOutput out(json);
    out.printLatLng(res.line, dict);
  } else {
    GeoJsonOutput out(json);
    out.printLatLng(res.pos, dict);
  }

  auto answ = util::http::Answer("200 OK", json.str());
  answ.params["Content-Type"] = "application/json; charset=utf-8";

  if (!noExport) {
    answ.params["Content-Disposition"] = "attachement;filename:\"export.json\"";
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

  LOG(INFO) << "[SERVER] Click at " << x << ", " << y;

  _m.lock();
  bool has = _rs.count(id);
  if (!has) {
    _m.unlock();
    throw std::invalid_argument("Session not found");
  }

  // NOTE: reqor is a shared pointer here, so it doesnt
  // matter if _rs is cleared after unlock
  auto reqor = _rs[id];
  _m.unlock();

  reqor->getMutex().lock();
  auto res = reqor->getNearest({x, y}, rad);
  reqor->getMutex().unlock();

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

    auto ll = util::geo::webMercToLatLng<float>(res.pos.getX(), res.pos.getY());

    json << "]";
    json << std::setprecision(10) << ",\"ll\":{\"lat\" : " << ll.getY()
         << ",\"lng\":" << ll.getX() << "}";

    if (res.poly.getOuter().size()) {
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
      out.printLatLng(res.pos, {});
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

  _m.lock();
  if (id.size()) {
    clearSession(id);
  } else {
    clearSessions();
  }

  _m.unlock();

  auto answ = util::http::Answer("200 OK", "{}");
  answ.params["Content-Type"] = "application/json; charset=utf-8";

  return answ;
}

// _____________________________________________________________________________
util::http::Answer Server::handleLoadReq(const Params& pars) const {
  if (pars.count("backend") == 0 || pars.find("backend")->second.empty())
    throw std::invalid_argument("No backend (?backend=) specified.");
  auto query = pars.find("query")->second;
  auto backend = pars.find("backend")->second;

  LOG(INFO) << "Backend is " << backend;

  _m.lock();
  if (_queryCache.count(backend)) {
    _m.unlock();
    auto answ = util::http::Answer("200 OK", "{}");
    answ.params["Content-Type"] = "application/json; charset=utf-8";
    return answ;
  }

  auto cache = new GeomCache(backend, _maxMemory);

  try {
    loadCache(cache);
    _caches[backend] = cache;
  } catch (OutOfMemoryError& ex) {
    LOG(ERROR) << ex.what() << backend;
    delete cache;
    _m.unlock();
    auto answ = util::http::Answer("406 Not Acceptable", ex.what());
    answ.params["Content-Type"] = "application/json; charset=utf-8";
    return answ;
  }

  _m.unlock();

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

  std::string queryId = backend + "$" + query;

  _m.lock();
  if (_queryCache.count(queryId)) {
    auto id = _queryCache[queryId];
    auto reqor = _rs[id];
    _m.unlock();

    if (reqor->ready()) {
      reqor->getMutex().lock();

      auto bbox = reqor->getPointGrid().getBBox();
      bbox = util::geo::extendBox(reqor->getLineGrid().getBBox(), bbox);

      auto ll = bbox.getLowerLeft();
      auto ur = bbox.getUpperRight();

      double llX = ll.getX();
      double llY = ll.getY();
      double urX = ur.getX();
      double urY = ur.getY();

      std::stringstream json;
      json << std::fixed << "{\"qid\" : \"" << id << "\",\"bounds\":[[" << llX
           << "," << llY << "],[" << urX << "," << urY << "]]"
           << "}";

      auto answ = util::http::Answer("200 OK", json.str());
      answ.params["Content-Type"] = "application/json; charset=utf-8";
      reqor->getMutex().unlock();

      return answ;
    }

    // if not ready, lock global lock again
    _m.lock();
  }

  if (_caches.count(backend) == 0) {
    auto cache = new GeomCache(backend, _maxMemory);

    try {
      loadCache(cache);
      _caches[backend] = cache;
    } catch (OutOfMemoryError& ex) {
      LOG(ERROR) << ex.what() << backend;
      delete cache;
      _m.unlock();
      auto answ = util::http::Answer("406 Not Acceptable", ex.what());
      answ.params["Content-Type"] = "application/json; charset=utf-8";
      return answ;
    }
  }

  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> d(
      1, std::numeric_limits<int>::max());

  std::string id = std::to_string(d(rng));

  auto reqor =
      std::shared_ptr<Requestor>(new Requestor(_caches[backend], _maxMemory));

  _rs[id] = reqor;
  _queryCache[queryId] = id;
  reqor->getMutex().lock();
  _m.unlock();

  T_START(req);

  try {
    reqor->request(query);
  } catch (OutOfMemoryError& ex) {
    LOG(ERROR) << ex.what() << backend;
    reqor->getMutex().unlock();

    // delete cache, is now in unready state
    _m.lock();
    clearSession(id);
    _m.unlock();
    auto answ = util::http::Answer("406 Not Acceptable", ex.what());
    answ.params["Content-Type"] = "application/json; charset=utf-8";
    return answ;
  }

  LOG(INFO) << "[SERVER] ** TOTAL REQUEST TIME: " << T_STOP(req) << " ms";

  auto bbox = reqor->getPointGrid().getBBox();
  bbox = util::geo::extendBox(reqor->getLineGrid().getBBox(), bbox);
  reqor->getMutex().unlock();

  auto ll = bbox.getLowerLeft();
  auto ur = bbox.getUpperRight();

  std::stringstream json;
  json << std::fixed << "{\"qid\" : \"" << id << "\",\"bounds\":[[" << ll.getX()
       << "," << ll.getY() << "],[" << ur.getX() << "," << ur.getY() << "]]"
       << "}";

  auto answ = util::http::Answer("200 OK", json.str(), true);
  answ.params["Content-Type"] = "application/json; charset=utf-8";

  return answ;
}

// _____________________________________________________________________________
std::string Server::parseUrl(std::string u, std::string pl,
                             std::map<std::string, std::string>* params) {
  auto parts = util::split(u, '?');

  if (parts.size() > 1) {
    auto kvs = util::split(parts[1], '&');
    for (const auto& kv : kvs) {
      auto kvp = util::split(kv, '=');
      if (kvp.size() == 1) kvp.push_back("");
      (*params)[util::urlDecode(kvp[0])] = util::urlDecode(kvp[1]);
    }
  }

  // also parse post data
  auto kvs = util::split(pl, '&');
  for (const auto& kv : kvs) {
    auto kvp = util::split(kv, '=');
    if (kvp.size() == 1) kvp.push_back("");
    (*params)[util::urlDecode(kvp[0])] = util::urlDecode(kvp[1]);
  }

  return util::urlDecode(parts.front());
}

// _____________________________________________________________________________
inline void pngWriteCb(png_structp png_ptr, png_bytep data, png_size_t length) {
  std::stringstream* ss = (std::stringstream*)png_get_io_ptr(png_ptr);
  ss->write(reinterpret_cast<char*>(data), length);
}

// _____________________________________________________________________________
std::string Server::writePNG(const unsigned char* data, size_t w, size_t h) {
  png_structp png_ptr =
      png_create_write_struct(PNG_LIBPNG_VER_STRING, nullptr, nullptr, nullptr);
  if (!png_ptr) {
    return "";
  }

  png_infop info_ptr = png_create_info_struct(png_ptr);
  if (!info_ptr) {
    png_destroy_write_struct(&png_ptr, (png_infopp) nullptr);
    return "";
  }

  if (setjmp(png_jmpbuf(png_ptr))) {
    png_destroy_write_struct(&png_ptr, &info_ptr);
    return "";
  }

  std::stringstream ss;
  png_set_write_fn(png_ptr, &ss, pngWriteCb, 0);

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
  return ss.str();
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

    _m.lock();
    for (const auto& id : toDel) {
      clearSession(id);
    }
    _m.unlock();
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

  _m.lock();
  bool has = _rs.count(id);
  if (!has) {
    _m.unlock();
    throw std::invalid_argument("Session not found");
  }

  // NOTE: reqor is a shared pointer here, so it doesnt
  // matter if _rs is cleared after unlock
  auto reqor = _rs[id];
  _m.unlock();

  aw.params["Content-Encoding"] = "identity";
  aw.params["Content-Type"] = "application/json";
  aw.params["Content-Disposition"] = "attachement;filename:\"export.json\"";

  std::stringstream ss;
  ss << "HTTP/1.1 200 OK" << aw.status << "\r\n";
  for (const auto& kv : aw.params)
    ss << kv.first << ": " << kv.second << "\r\n";

  ss << "\r\n";
  ss << "{\"type\":\"FeatureCollection\",\"features\":[";

  std::string buff = ss.str();

  size_t writes = 0;

  while (writes != buff.size()) {
    int64_t out = write(sock, buff.c_str() + writes, buff.size() - writes);
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
            auto geom = util::geo::polygonFromWKT<float>(wkt);
            if (first) ss << ",";
            geoJsonOut.print(geom, dict);
            first = true;
          } catch (std::runtime_error& e) {
          }
          try {
            auto geom = util::geo::multiPolygonFromWKT<float>(wkt);
            if (first) ss << ",";
            geoJsonOut.print(geom, dict);
            first = true;
          } catch (std::runtime_error& e) {
          }
          try {
            auto geom = util::geo::pointFromWKT<float>(wkt);
            if (first) ss << ",";
            geoJsonOut.print(geom, dict);
            first = true;
          } catch (std::runtime_error& e) {
          }
          try {
            auto geom = util::geo::lineFromWKT<float>(wkt);
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
          int64_t out =
              write(sock, buff.c_str() + writes, buff.size() - writes);
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
    int64_t out = write(sock, buff.c_str() + writes, buff.size() - writes);
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
void Server::loadCache(GeomCache* cache) const {
  if (_cacheDir.size()) {
    std::string backend = cache->getBackendURL();
    util::replaceAll(backend, "/", "_");
    std::string cacheFile = _cacheDir + "/" + backend;
    if (access(cacheFile.c_str(), F_OK) != -1) {
      LOG(INFO) << "Reading from cache file " << cacheFile << "...";
      cache->fromDisk(cacheFile);
      LOG(INFO) << "done ...";
    } else {
      if (access(_cacheDir.c_str(), W_OK) != 0) {
        std::stringstream ss;
        ss << "No write access to cache dir " << _cacheDir;
        throw std::runtime_error(ss.str());
      }
      cache->request();
      cache->requestIds();
      LOG(INFO) << "Serializing to cache file " << cacheFile << "...";
      cache->serializeToDisk(cacheFile);
      LOG(INFO) << "done ...";
    }
  } else {
    cache->request();
    cache->requestIds();
  }
}

// _____________________________________________________________________________
