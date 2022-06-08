// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <omp.h>
#include <png.h>

#include <codecvt>
#include <locale>
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
#include "util/Misc.h"
#include "util/String.h"
#include "util/geo/Geo.h"
#include "util/log/Log.h"

using petrimaps::Params;
using petrimaps::Server;

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
    } else if (cmd == "/clearsession") {
      a = handleClearSessReq(params);
    } else if (cmd == "/clearsessions") {
      a = handleClearSessReq(params);
    } else if (cmd == "/load") {
      a = handleLoadReq(params);
    } else if (cmd == "/pos") {
      a = handlePosReq(params);
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

  LOG(INFO) << "[SERVER] Begin heat for session " << id;

  _m.lock();
  bool has = _rs.count(id);
  _m.unlock();

  if (!has) {
    throw std::invalid_argument("Session not found");
  }

  _m.lock();
  const Requestor& r = *_rs[id];
  _m.unlock();

  float x1 = atof(box[0].c_str());
  float y1 = atof(box[1].c_str());
  float x2 = atof(box[2].c_str());
  float y2 = atof(box[3].c_str());

  double mercW = fabs(x2 - x1);
  double mercH = fabs(y2 - y1);

  auto bbox = util::geo::FBox({x1, y1}, {x2, y2});

  int w = atoi(pars.find("width")->second.c_str());
  int h = atoi(pars.find("height")->second.c_str());

  double res = mercH / h;

  heatmap_t* hm = heatmap_new(w, h);

  double realCellSize = r.getPointGrid().getCellWidth();
  double virtCellSize = res * 2.5;

  size_t NUM_THREADS = 24;

  size_t subCellSize = (size_t)ceil(realCellSize / virtCellSize);
  size_t** subCellCount = new size_t*[NUM_THREADS];

  LOG(INFO) << "[SERVER] Query resolution: " << res;
  LOG(INFO) << "[SERVER] Virt cell size: " << virtCellSize;
  LOG(INFO) << "[SERVER] Num virt cells: " << subCellSize * subCellSize;

  std::vector<std::vector<std::pair<float, std::pair<float, float>>>> points(
      NUM_THREADS);

  double THRESHOLD = 50;

  if (res < THRESHOLD) {
    std::unordered_set<ID_TYPE> ret;

    LOG(INFO) << "[SERVER] Looking up display points...";
    r.getPointGrid().get(bbox, &ret);
    LOG(INFO) << "[SERVER] ... done (" << ret.size() << " points)";

    for (size_t i : ret) {
      const auto& p = r.getPoint(r.getObjects()[i].first);
      if (!util::geo::contains(p, bbox)) continue;

      points[0].push_back(
          {1,
           {((p.getX() - bbox.getLowerLeft().getX()) / mercW) * w,
            h - ((p.getY() - bbox.getLowerLeft().getY()) / mercH) * h}});
    }

  } else {
    for (size_t i = 0; i < NUM_THREADS; i++) {
      subCellCount[i] = new size_t[subCellSize * subCellSize];
    }

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
    for (size_t x = r.getPointGrid().getCellXFromX(bbox.getLowerLeft().getX());
         x <= r.getPointGrid().getCellXFromX(bbox.getUpperRight().getX());
         x++) {
      for (size_t y =
               r.getPointGrid().getCellYFromY(bbox.getLowerLeft().getY());
           y <= r.getPointGrid().getCellYFromY(bbox.getUpperRight().getY());
           y++) {
        if (x >= r.getPointGrid().getXWidth() ||
            y >= r.getPointGrid().getYHeight())
          continue;
        const auto& cell = r.getPointGrid().getCell(x, y);
        if (cell.size() == 0) continue;
        const auto& cellBox = r.getPointGrid().getBox(x, y);

        memset(subCellCount[omp_get_thread_num()], 0,
               subCellSize * subCellSize * sizeof(size_t));

        if (subCellSize == 1) {
          subCellCount[omp_get_thread_num()][0] = cell.size();
        } else {
          for (const auto& i : cell) {
            const auto& p = r.getPoint(r.getObjects()[i].first);

            float dx = fmax(0, p.getX() - cellBox.getLowerLeft().getX());
            size_t virtX = floor(dx / virtCellSize);

            float dy = fmax(0, p.getY() - cellBox.getLowerLeft().getY());
            size_t virtY = floor(dy / virtCellSize);

            if (virtX >= subCellSize) virtX = subCellSize - 1;
            if (virtY >= subCellSize) virtY = subCellSize - 1;

            assert(virtX * subCellSize + virtY < subCellSize * subCellSize);
            subCellCount[omp_get_thread_num()][virtX * subCellSize + virtY] +=
                1;
          }
        }

        for (size_t x = 0; x < subCellSize; x++) {
          for (size_t y = 0; y < subCellSize; y++) {
            if (subCellCount[omp_get_thread_num()][x * subCellSize + y] == 0)
              continue;

            points[omp_get_thread_num()].push_back(
                {subCellCount[omp_get_thread_num()][x * subCellSize + y],
                 {((cellBox.getLowerLeft().getX() + x * virtCellSize -
                    bbox.getLowerLeft().getX()) /
                   mercW) *
                      w,
                  h - ((cellBox.getLowerLeft().getY() + y * virtCellSize -
                        bbox.getLowerLeft().getY()) /
                       mercH) *
                          h}});
          }
        }
      }
    }
    for (size_t i = 0; i < NUM_THREADS; i++) {
      delete[] subCellCount[i];
    }
  }

  // LINES
  if (res < THRESHOLD) {
    std::unordered_set<ID_TYPE> ret;

    LOG(INFO) << "[SERVER] Looking up display lines...";
    r.getLineGrid().get(bbox, &ret);
    LOG(INFO) << "[SERVER] ... done (" << ret.size() << " lines)";

    for (size_t i : ret) {
      auto lid = r.getObjects()[i].first;
      const auto& lbox = r.getLineBBox(lid - I_OFFSET);
      if (!util::geo::intersects(lbox, bbox)) continue;

      uint8_t gi = 0;

      size_t start = r.getLine(lid - I_OFFSET);
      size_t end = r.getLineEnd(lid - I_OFFSET);

      // ___________________________________
      bool isects = false;

      util::geo::FPoint curPa, curPb;
      int s = 0;

      double mainX = 0;
      double mainY = 0;
      for (size_t i = start; i < end; i++) {
        // extract real geom
        const auto& cur = r.getLinePoints()[i];

        if (isMCoord(cur.getX())) {
          mainX = rmCoord(cur.getX());
          mainY = rmCoord(cur.getY());
          continue;
        }

        // skip bounding box at beginning
        gi++;
        if (gi < 3) continue;

        // extract real geometry
        util::geo::FPoint curP(mainX * 1000 + cur.getX(),
                               mainY * 1000 + cur.getY());
        if (s == 0) {
          curPa = curP;
          s++;
        } else if (s == 1) {
          curPb = curP;
          s++;
        }

        if (s == 2) {
          s = 1;
          if (util::geo::intersects(util::geo::LineSegment<float>(curPa, curPb),
                                    bbox)) {
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

      util::geo::FLine extrLine;
      extrLine.reserve(end - start);

      gi = 0;

      for (size_t i = start; i < end; i++) {
        // extract real geom
        const auto& cur = r.getLinePoints()[i];

        if (isMCoord(cur.getX())) {
          mainX = rmCoord(cur.getX());
          mainY = rmCoord(cur.getY());
          continue;
        }

        // skip bounding box at beginning
        gi++;
        if (gi < 3) continue;

        util::geo::FPoint p(mainX * 1000 + cur.getX(),
                            mainY * 1000 + cur.getY());
        extrLine.push_back(p);
      }

      const auto& denseLine = util::geo::densify(extrLine, res);

      for (const auto& p : denseLine) {
        points[0].push_back(
            {1,
             {((p.getX() - bbox.getLowerLeft().getX()) / mercW) * w,
              h - ((p.getY() - bbox.getLowerLeft().getY()) / mercH) * h}});
      }
    }

  } else {
    for (size_t i = 0; i < NUM_THREADS; i++) {
      subCellCount[i] = new size_t[subCellSize * subCellSize];
    }

#pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
    for (size_t x =
             r.getLinePointGrid().getCellXFromX(bbox.getLowerLeft().getX());
         x <= r.getLinePointGrid().getCellXFromX(bbox.getUpperRight().getX());
         x++) {
      for (size_t y =
               r.getLinePointGrid().getCellYFromY(bbox.getLowerLeft().getY());
           y <= r.getLinePointGrid().getCellYFromY(bbox.getUpperRight().getY());
           y++) {
        if (x >= r.getLinePointGrid().getXWidth() ||
            y >= r.getLinePointGrid().getYHeight())
          continue;
        const auto& cell = r.getLinePointGrid().getCell(x, y);
        if (cell.size() == 0) continue;
        const auto& cellBox = r.getLinePointGrid().getBox(x, y);

        memset(subCellCount[omp_get_thread_num()], 0,
               subCellSize * subCellSize * sizeof(size_t));

        if (subCellSize == 1) {
          subCellCount[omp_get_thread_num()][0] = cell.size();
        } else {
          for (const auto& p : cell) {
            if (!util::geo::contains(p, cellBox)) continue;
            float dx = fmax(0, p.getX() - cellBox.getLowerLeft().getX());
            size_t virtX = floor(dx / virtCellSize);

            float dy = fmax(0, p.getY() - cellBox.getLowerLeft().getY());
            size_t virtY = floor(dy / virtCellSize);

            if (virtX >= subCellSize) virtX = subCellSize - 1;
            if (virtY >= subCellSize) virtY = subCellSize - 1;

            assert(virtX * subCellSize + virtY < subCellSize * subCellSize);
            subCellCount[omp_get_thread_num()][virtX * subCellSize + virtY] +=
                1;
          }
        }

        for (size_t x = 0; x < subCellSize; x++) {
          for (size_t y = 0; y < subCellSize; y++) {
            if (subCellCount[omp_get_thread_num()][x * subCellSize + y] == 0)
              continue;

            points[omp_get_thread_num()].push_back(
                {subCellCount[omp_get_thread_num()][x * subCellSize + y],
                 {((cellBox.getLowerLeft().getX() + x * virtCellSize -
                    bbox.getLowerLeft().getX()) /
                   mercW) *
                      w,
                  h - ((cellBox.getLowerLeft().getY() + y * virtCellSize -
                        bbox.getLowerLeft().getY()) /
                       mercH) *
                          h}});
          }
        }
      }
    }
    for (size_t i = 0; i < NUM_THREADS; i++) {
      delete[] subCellCount[i];
    }
  }

  LOG(INFO) << "[SERVER] Adding points to heatmap...";

  for (size_t i = 0; i < NUM_THREADS; i++) {
    for (const auto& p : points[i]) {
      heatmap_add_weighted_point(hm, p.second.first, p.second.second, p.first);
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
  delete[] subCellCount;

  return answ;
}

// _____________________________________________________________________________
util::http::Answer Server::handlePosReq(const Params& pars) const {
  if (pars.count("x") == 0 || pars.find("x")->second.empty())
    throw std::invalid_argument("No x coord (?x=) specified.");
  float x = atof(pars.find("x")->second.c_str());

  if (pars.count("y") == 0 || pars.find("y")->second.empty())
    throw std::invalid_argument("No y coord (?y=) specified.");
  float y = atof(pars.find("y")->second.c_str());

  if (pars.count("id") == 0 || pars.find("id")->second.empty())
    throw std::invalid_argument("No session id (?id=) specified.");
  auto id = pars.find("id")->second;

  if (pars.count("rad") == 0 || pars.find("rad")->second.empty())
    throw std::invalid_argument("No rad (?rad=) specified.");
  auto rad = atof(pars.find("rad")->second.c_str());

  LOG(INFO) << "[SERVER] Click at " << x << ", " << y;

  _m.lock();
  bool has = _rs.count(id);
  _m.unlock();

  if (!has) throw std::invalid_argument("Session not found");

  _m.lock();
  const Requestor& r = *_rs[id];
  _m.unlock();

  r.getMutex().lock();
  auto res = r.getNearest({x, y}, rad);
  r.getMutex().unlock();

  std::stringstream json;

  json << "[";

  if (res.has) {
    json << "{\"attrs\" : [";

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
    clearSession(id);
    auto answ = util::http::Answer("406 Not Acceptable", ex.what());
    answ.params["Content-Type"] = "application/json; charset=utf-8";
    return answ;
  }

  LOG(INFO) << "[SERVER] ** TOTAL REQUEST TIME: " << T_STOP(req) << " ms";

  auto bbox = reqor->getPointGrid().getBBox();
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
      if (it->second == "id") {
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
  // TODO
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
