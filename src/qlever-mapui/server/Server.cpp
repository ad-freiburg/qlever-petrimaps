// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <omp.h>
#include <png.h>
#include <codecvt>
#include <locale>
#include <set>
#include <vector>
#include "qlever-mapui/build.h"
#include "qlever-mapui/index.h"
#include "qlever-mapui/server/Requestor.h"
#include "qlever-mapui/server/Server.h"
#include "util/Misc.h"
#include "util/String.h"
#include "util/geo/Geo.h"
#include "util/log/Log.h"

#include <random>
#include "3rdparty/heatmap.h"
#include "3rdparty/colorschemes/Spectral.h"

using mapui::Params;
using mapui::Server;

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
  } catch (std::runtime_error e) {
    a = util::http::Answer("400 Bad Request", e.what());
  } catch (std::invalid_argument e) {
    a = util::http::Answer("400 Bad Request", e.what());
  } catch (...) {
    a = util::http::Answer("500 Internal Server Error",
                           "Internal Server Error.");
  }

  a.params["Access-Control-Allow-Origin"] = "*";
  a.params["Server"] = "qlever-mapui-middleend";

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

  LOG(INFO) << "Begin heat for session " << id;

  _m.lock();
  bool has = _rs.count(id);
  _m.unlock();

  if (!has) {
    throw std::invalid_argument("Session not found");
  }

  _m.lock();
  const Requestor& r = *_rs[id];
  _m.unlock();

  double x1 = atof(box[0].c_str());
  double y1 = atof(box[1].c_str());
  double x2 = atof(box[2].c_str());
  double y2 = atof(box[3].c_str());

  double mercW = fabs(x2 - x1);
  double mercH = fabs(y2 - y1);

  auto bbox = util::geo::FBox({x1, y1}, {x2, y2});

  int w = atoi(pars.find("width")->second.c_str());
  int h = atoi(pars.find("height")->second.c_str());

  double res = mercH / h;

  heatmap_t* hm = heatmap_new(w, h);

  double realCellSize = 50000;  // TODO: get this from grid
  double virtCellSize = res * 2.5;

  size_t NUM_THREADS = 8;

  size_t subCellSize = (size_t)ceil(realCellSize / virtCellSize);
  size_t* subCellCount[NUM_THREADS];

  LOG(INFO) << "Resolution: " << res;
  LOG(INFO) << "Virt cell size: " << virtCellSize;
  LOG(INFO) << "Num virt cells: " << subCellSize * subCellSize;

  std::vector<std::pair<float, std::pair<float, float>>> points[NUM_THREADS];

  double THRESHOLD = 50;

  if (res < THRESHOLD) {
    std::set<size_t> ret;

    r.getPointGrid().get(bbox, &ret);
    LOG(INFO) << "Done lookup";

    for (size_t i : ret) {
      const auto& p = r.getPoints()[i];
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

#pragma omp parallel for num_threads(NUM_THREADS)
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
            const auto& p = r.getPoints()[i];
            float dx = p.getX() - cellBox.getLowerLeft().getX();
            size_t virtX = floor(dx / virtCellSize);

            float dy = p.getY() - cellBox.getLowerLeft().getY();
            size_t virtY = floor(dy / virtCellSize);

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
    std::set<size_t> ret;

    r.getLineGrid().get(bbox, &ret);
    LOG(INFO) << "Done lookup";

    for (size_t i : ret) {
      const auto& l = r.getLines()[i];
      if (!util::geo::intersects(l, bbox)) continue;

      for (const auto& p : util::geo::densify(l, res)) {
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
#pragma omp parallel for num_threads(NUM_THREADS)
    for (size_t x = r.getLineGrid().getCellXFromX(bbox.getLowerLeft().getX());
         x <= r.getLineGrid().getCellXFromX(bbox.getUpperRight().getX()); x++) {
      for (size_t y = r.getLineGrid().getCellYFromY(bbox.getLowerLeft().getY());
           y <= r.getLineGrid().getCellYFromY(bbox.getUpperRight().getY());
           y++) {
        if (x >= r.getLineGrid().getXWidth() ||
            y >= r.getLineGrid().getYHeight())
          continue;
        const auto& cell = r.getLineGrid().getCell(x, y);
        if (cell.size() == 0) continue;
        const auto& cellBox = r.getLineGrid().getBox(x, y);

        memset(subCellCount[omp_get_thread_num()], 0,
               subCellSize * subCellSize * sizeof(size_t));

        if (subCellSize == 1) {
          subCellCount[omp_get_thread_num()][0] = cell.size();
        } else {
          for (const auto& i : cell) {
            const auto& l = r.getLines()[i];
            for (const auto& p : l) {
              if (!util::geo::contains(p, cellBox)) continue;
              float dx = p.getX() - cellBox.getLowerLeft().getX();
              size_t virtX = floor(dx / virtCellSize);

              float dy = p.getY() - cellBox.getLowerLeft().getY();
              size_t virtY = floor(dy / virtCellSize);

              subCellCount[omp_get_thread_num()][virtX * subCellSize + virtY] +=
                  1;
            }
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

  for (size_t i = 0; i < NUM_THREADS; i++) {
    for (const auto& p : points[i]) {
      heatmap_add_weighted_point(hm, p.second.first, p.second.second, p.first);
    }
  }

  LOG(INFO) << "End point add";

  std::vector<unsigned char> image(w * h * 4);
  double sat = 5 * pow(res, 1.3);
  // heatmapr.nder_saturated_to(hm, heatmap_cs_Spectral_mixed_exp, sat,
  // &image[0]);
  heatmap_render_to(hm, heatmap_cs_Spectral_mixed_exp, &image[0]);

  LOG(INFO) << "End render";

  auto answ = util::http::Answer("200 OK", writePNG(&image[0], w, h), false);
  answ.params["Content-Type"] = "image/png";

  LOG(INFO) << "End render to PNG";

  heatmap_free(hm);

  return answ;
}

// _____________________________________________________________________________
util::http::Answer Server::handlePosReq(const Params& pars) const {
  if (pars.count("x") == 0 || pars.find("x")->second.empty())
    throw std::invalid_argument("No x coord (?x=) specified.");
  int x = atoi(pars.find("x")->second.c_str());

  if (pars.count("y") == 0 || pars.find("y")->second.empty())
    throw std::invalid_argument("No y coord (?y=) specified.");
  int y = atoi(pars.find("y")->second.c_str());

  if (pars.count("id") == 0 || pars.find("id")->second.empty())
    throw std::invalid_argument("No session id (?id=) specified.");
  auto id = pars.find("id")->second;

  LOG(INFO) << "Click at " << x << ", " << y;

  _m.lock();
  bool has = _rs.count(id);
  _m.unlock();

  if (!has) {
    throw std::invalid_argument("Session not found");
  }

  _m.lock();
  const Requestor& r = *_rs[id];
  _m.unlock();


  auto res = r.getNearest({x, y}, 100);

  std::stringstream json;

  json << "[";

  if (res.has) {
    json << "{\"attrs\" : [";

    bool first = true;

    for (const auto& kv : res.cols) {
      if (!first) {
        json << ",";
      }
      json << "[\"" << util::jsonStringEscape(kv.first) << "\",\"" << util::jsonStringEscape(kv.second) << "\"]";

      first = false;
    }

    auto ll = util::geo::webMercToLatLng<float>(res.pos.getX(), res.pos.getY());

    json << "]";
    json << std::setprecision(10) << ",\"ll\":{\"lat\" : " << ll.getY() << ",\"lng\":" << ll.getX() << "}";

    json << "}";
  }

  json << "]";

  auto answ = util::http::Answer("200 OK", json.str(), true);
  answ.params["Content-Type"] = "application/json; charset=utf-8";

  return answ;
}

// _____________________________________________________________________________
util::http::Answer Server::handleClearSessReq(const Params& pars) const {
  if (pars.count("id") == 0 || pars.find("id")->second.empty())
    throw std::invalid_argument("No session id (?id=) specified.");
  auto id = pars.find("id")->second;

  LOG(INFO) << "Clearing session " << id;

  _m.lock();

  if (_rs.count(id)) {
    _rs[id]->getMutex().lock();
    delete _rs[id];
    _rs.erase(id);

    // TODO: erase from querycache!
  }

  _m.unlock();

  auto answ = util::http::Answer("200 OK", "{}", true);
  answ.params["Content-Type"] = "application/json; charset=utf-8";

  return answ;
}

// _____________________________________________________________________________
util::http::Answer Server::handleQueryReq(const Params& pars) const {
  if (pars.count("query") == 0 || pars.find("query")->second.empty())
    throw std::invalid_argument("No query (?q=) specified.");
  if (pars.count("backend") == 0 || pars.find("backend")->second.empty())
    throw std::invalid_argument("No backend (?backend=) specified.");
  std::string cb;
  if (pars.count("cb")) cb = pars.find("cb")->second.c_str();
  auto query = pars.find("query")->second;
  auto backend = pars.find("backend")->second;

  LOG(INFO) << "Backend is " << backend;
  LOG(INFO) << "Query is " << query;

  std::string queryId = backend + "$" + query;

  _m.lock();
  if (_queryCache.count(queryId)) {
    auto id = _queryCache[queryId];
    auto reqor = _rs[id];
    _m.unlock();

    reqor->getMutex().lock();

    auto bbox = reqor->getPointGrid().getBBox();

    auto ll = bbox.getLowerLeft();
    auto ur = bbox.getUpperRight();

    std::stringstream json;
    json << std::fixed << "{\"qid\" : \"" << id << "\",\"bounds\":[[" << ll.getX() << "," << ll.getY() << "],[" << ur.getX() << "," << ur.getY() << "]]"
         << "}";

    auto answ = util::http::Answer("200 OK", json.str(), true);
    answ.params["Content-Type"] = "application/json; charset=utf-8";
    reqor->getMutex().unlock();

    return answ;
  }
  _m.unlock();

  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> d(
      1, std::numeric_limits<int>::max());

  std::string id = std::to_string(d(rng));

  auto reqor = new Requestor(backend);

  _m.lock();
  _rs[id] = reqor;
  _queryCache[queryId] = id;
  _m.unlock();

  T_START(req);
  reqor->request(query);

  LOG(INFO) << "TOTAL REQUEST TIME: " << T_STOP(req) << " ms";
  LOG(INFO) << "NUMBER RESULTS: " << reqor->size();

  auto bbox = reqor->getPointGrid().getBBox();

  auto ll = bbox.getLowerLeft();
  auto ur = bbox.getUpperRight();

  std::stringstream json;
  json << std::fixed << "{\"qid\" : \"" << id << "\",\"bounds\":[[" << ll.getX() << "," << ll.getY() << "],[" << ur.getX() << "," << ur.getY() << "]]"
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
  // NULLS are user error/warning functions.
  png_structp png_ptr =
      png_create_write_struct(PNG_LIBPNG_VER_STRING, nullptr, nullptr, nullptr);
  if (!png_ptr) {
    std::cerr << "Error initializing libpng write struct." << std::endl;
    return "";
  }

  png_infop info_ptr = png_create_info_struct(png_ptr);
  if (!info_ptr) {
    std::cerr << "Error initializing libpng info struct." << std::endl;
    png_destroy_write_struct(&png_ptr, (png_infopp) nullptr);
    return "";
  }

  if (setjmp(png_jmpbuf(png_ptr))) {
    std::cerr << "Error in setjmp!?" << std::endl;
    png_destroy_write_struct(&png_ptr, &info_ptr);
    return "";
  }

  // Could be used for progress report of some sort.
  std::stringstream ss;
  png_set_write_fn(png_ptr, &ss, pngWriteCb, 0);

  // turn on or off filtering, and/or choose specific filters.
  // You can use either a single PNG_FILTER_VALUE_NAME or the logical OR
  // of one or more PNG_FILTER_NAME masks.
  png_set_filter(png_ptr, 0, PNG_FILTER_NONE | PNG_FILTER_VALUE_NONE
                 // | PNG_FILTER_SUB   | PNG_FILTER_VALUE_SUB
                 // | PNG_FILTER_UP    | PNG_FILTER_VALUE_UP
                 // | PNG_FILTER_AVE   | PNG_FILTER_VALUE_AVE
                 // | PNG_FILTER_PAETH | PNG_FILTER_VALUE_PAETH
                 // | PNG_ALL_FILTERS
  );

  // set the zlib compression level.
  // 1 = fast but not much compression, 9 = slow but much compression.
  png_set_compression_level(png_ptr, 7);

  static const int bit_depth = 8;
  static const int color_type = PNG_COLOR_TYPE_RGB_ALPHA;
  static const int interlace_type =
      PNG_INTERLACE_ADAM7;  // or PNG_INTERLACE_NONE
  png_set_IHDR(png_ptr, info_ptr, w, h, bit_depth, color_type, interlace_type,
               PNG_COMPRESSION_TYPE_DEFAULT, PNG_FILTER_TYPE_DEFAULT);

  png_bytep* row_pointers =
      (png_byte**)png_malloc(png_ptr, h * sizeof(png_bytep));
  for (size_t y = 0; y < h; ++y) {
    row_pointers[y] = const_cast<png_bytep>(data + y * w * 4);
  }
  png_set_rows(png_ptr, info_ptr, row_pointers);

  png_write_png(png_ptr, info_ptr, PNG_TRANSFORM_IDENTITY, nullptr);

  // Cleanup
  png_free(png_ptr, row_pointers);
  png_destroy_write_struct(&png_ptr, &info_ptr);
  return ss.str();
}
