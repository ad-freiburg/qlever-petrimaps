// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <unistd.h>

#include <cstdint>
#include <vector>
#include "3rdparty/heatmap.h"

#ifndef PETRIMAPS_SERVER_RENDERCONTEXT_H_
#define PETRIMAPS_SERVER_RENDERCONTEXT_H_

namespace petrimaps {

enum MapStyle { HEATMAP, OBJECTS, RASTER };

class RenderContext {
 public:
  RenderContext(size_t w, size_t h, MapStyle style, size_t numThreads);

  const std::vector<uint32_t>& getPoints(size_t i) { return _points[i]; }
  const std::vector<double>& getWeights(size_t i) { return _weights[i]; }
  const std::vector<std::pair<float, float>>& getRasterDims(size_t i) {
    return _rasterDims[i];
  }
  std::vector<unsigned char>& getImage() { return _image; }
  void drawPoint(size_t tid, int px, int py, int w, int h,
                 double weight, double rasterW, double rasterH);
  void drawLine(int x0, int y0, int x1, int y1, int w, int h);
  void writeHeatmap(heatmap_t* hm, double res);

 private:
  heatmap_stamp_t* rasterStamp(double res, double w, double h, double screenW,
                               double screenH) const;

  std::vector<std::vector<uint32_t>> _points;
  std::vector<std::vector<double>> _weights;
  std::vector<std::vector<std::pair<float, float>>> _rasterDims;
  std::vector<unsigned char> _image;
  MapStyle _style;

  size_t _w, _h;
};

}  // namespace petrimaps

#endif  // PETRIMAPS_SERVER_RENDERCONTEXT_H_
