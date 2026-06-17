// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <cmath>
#include <map>
#include <iostream>

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
// clang-format on
#include "RenderContext.h"

using petrimaps::MapStyle;
using petrimaps::RenderContext;

// _____________________________________________________________________________
RenderContext::RenderContext(size_t w, size_t h, MapStyle style,
                             size_t numThreads)
    : _points(numThreads),
      _weights(numThreads),
      _rasterDims(numThreads),
      _image(w * h * 4),
      _style(style),
      _w(w),
      _h(h) {
  for (size_t i = 0; i < _points.size(); i++) {
    _rasterDims[i].resize(w * h, {1, 1});
    _weights[i].resize(w * h, 0);
  }
}

// _____________________________________________________________________________
void RenderContext::drawPoint(size_t tid, int px, int py, int w, int h,
                              double weight, double rasterW, double rasterH) {
  if (_style == RASTER) {
    if (px >= 0 && py >= 0 && px < w && py < h) {
      _rasterDims[tid][w * py + px] = {rasterW, rasterH};
      if (_weights[tid][w * py + px] == 0) {
        _points[tid].push_back(w * py + px);
        _weights[tid][w * py + px] = weight;
      } else {
        // not entirely correct, but looks good on very low zoom levels
        // where many raster cells are rendered onto the same pixel
        _weights[tid][w * py + px] =
            (_weights[tid][w * py + px] + weight) / 2.0;
      }
    }
  } else if (_style == OBJECTS) {
    // for the raw style, increase the size of the points a bit
    for (int x = px - 2; x < px + 2; x++) {
      for (int y = py - 2; y < py + 2; y++) {
        if (x >= 0 && y >= 0 && x < w && y < h) {
          if (_weights[tid][w * y + x] == 0) _points[tid].push_back(w * y + x);
          _weights[tid][w * y + x] += weight;
        }
      }
    }
  } else {
    if (px >= 0 && py >= 0 && px < w && py < h) {
      if (_weights[tid][w * py + px] == 0) _points[tid].push_back(w * py + px);
      _weights[tid][w * py + px] += weight;
    }
  }
}

// _____________________________________________________________________________
void RenderContext::drawLine(int x0, int y0, int x1, int y1, int w, int h) {
  // Bresenham
  int dx = abs(x1 - x0);
  int sx = x0 < x1 ? 1 : -1;
  int dy = -abs(y1 - y0);
  int sy = y0 < y1 ? 1 : -1;
  int error = dx + dy;

  while (true) {
    if (x0 >= 0 && y0 >= 0 && x0 < w && y0 < h) {
      size_t coord = y0 * w * 4 + x0 * 4;
      _image[coord] = 51;
      _image[coord + 1] = 136;
      _image[coord + 2] = 255;
      _image[coord + 3] = 150;
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

// _____________________________________________________________________________
void RenderContext::writeHeatmap(heatmap_t* hm, double res) {
  size_t NUM_THREADS = _points.size();

  if (_style == RASTER) {
    // first, aggregate possible stamp styles
    std::map<std::pair<float, float>, heatmap_stamp_t*> stamps;
    for (size_t i = 0; i < NUM_THREADS; i++) {
      for (const auto& p : _points[i]) {
        if (stamps.count(_rasterDims[i][p])) continue;
        if (_weights[i][p] == 0) continue;
        stamps[_rasterDims[i][p]] = rasterStamp(
            res, _rasterDims[i][p].first, _rasterDims[i][p].second, _w, _h);
      }
    }

    // now render per stamp style
    for (auto stamp : stamps) {
      for (size_t i = 0; i < NUM_THREADS; i++) {
        for (const auto& p : _points[i]) {
          size_t y = p / _w;
          size_t x = p - (y * _w);
          if (_weights[i][p] == 0) continue;
          if (_rasterDims[i][p] != stamp.first) continue;
          if (!stamp.second) continue;
          heatmap_add_weighted_point_with_stamp_no_aggreg(
              hm, x, y, _weights[i][p], stamp.second);
        }
      }
    }

    for (auto stamp : stamps) heatmap_stamp_free(stamp.second);
  } else if (_style == OBJECTS) {
    auto stamp = heatmap_stamp_gen(3);
    for (size_t i = 0; i < NUM_THREADS; i++) {
      for (const auto& p : _points[i]) {
        size_t y = p / _w;
        size_t x = p - (y * _w);
        if (_weights[i][p] > 0)
          heatmap_add_weighted_point_with_stamp(hm, x, y, 1, stamp);
      }
    }
    heatmap_stamp_free(stamp);
  } else {
    // HEATMAP
    for (size_t i = 0; i < NUM_THREADS; i++) {
      for (const auto& p : _points[i]) {
        size_t y = p / _w;
        size_t x = p - (y * _w);
        if (_weights[i][p] > 0)
          heatmap_add_weighted_point(hm, x, y, _weights[i][p]);
      }
    }
  }
}

// _____________________________________________________________________________
heatmap_stamp_t* RenderContext::rasterStamp(double res, double w, double h,
                                            double screenW,
                                            double screenH) const {
  if (w < 0) w = 0;
  if (h < 0) h = 0;
  if (screenW < 0) screenW = 0;
  if (screenH < 0) screenH = 0;
  if (std::isnan(w)) w = 0;
  if (std::isnan(h)) h = 0;

  int width = std::min(screenW * 2, (ceil(w / res)));
  int height = std::min(screenH * 2, (ceil(h / res)));

  float* stamp = (float*)calloc(width * height, sizeof(float));
  if (!stamp) return 0;

  for (int x = 0; x < width; x++) {
    for (int y = 0; y < height; y++) {
      stamp[x * height + y] = 1.0;
    }
  }

  return heatmap_stamp_new_with(width, height, stamp);
}
