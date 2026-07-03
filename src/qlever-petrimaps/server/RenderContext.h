// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <unistd.h>

#include <cstdint>
#include <vector>

#include "3rdparty/heatmap.h"
#include "util/geo/Geo.h"

#ifndef PETRIMAPS_SERVER_RENDERCONTEXT_H_
#define PETRIMAPS_SERVER_RENDERCONTEXT_H_

namespace petrimaps {

enum MapStyle { HEATMAP, OBJECTS, RASTER };

class RenderContext {
 public:
  RenderContext(int w, int h, double orx, double ory, double mercW,
                double mercH, MapStyle style, size_t numThreads);

  const std::vector<uint32_t>& getPoints(size_t i) { return _points[i]; }
  const std::vector<uint32_t>& getAreaFillPoints(size_t i) {
    return _areaFillPoints[i];
  }
  const std::vector<std::pair<float, float>>& getRasterDims(size_t i) {
    return _rasterDims[i];
  }
  std::vector<unsigned char>& getImage() { return _image; }
  void drawPoint(size_t tid, int px, int py, double weight, double rasterW,
                 double rasterH, int r = 1);
  void drawFillPoint(size_t tid, int px, int py, double weight, int r = 0);
  void drawLineSegment(int x0, int y0, int x1, int y1, int w, int h);
  void drawLine(size_t tid, const util::geo::DLine& line, double val);
  void drawArea(size_t tid, const util::geo::DLine& line, double val,
                bool border = true, bool inner = false);
  void writeHeatmap(heatmap_t* hm);
  void writeInteriorObjects(heatmap_t* hm);

  static util::geo::Point<int> mercToPx(util::geo::FPoint p, double orx,
                                        double ory, double mercW, double mercH,
                                        int w, int h) {
    return {((p.getX() - orx) / mercW) * w, h - ((p.getY() - ory) / mercH) * h};
  }

  static util::geo::Point<int> mercToPx(util::geo::DPoint p, double orx,
                                        double ory, double mercW, double mercH,
                                        int w, int h) {
    return {((p.getX() - orx) / mercW) * w, h - ((p.getY() - ory) / mercH) * h};
  }

 private:
  heatmap_stamp_t* rasterStamp(double w, double h) const;

  std::vector<std::vector<uint32_t>> _points;
  std::vector<std::vector<uint32_t>> _areaFillPoints;
  std::vector<std::vector<double>> _weights;
  std::vector<std::vector<double>> _areaFillWeights;
  std::vector<std::vector<std::pair<float, float>>> _rasterDims;
  std::vector<unsigned char> _image;
  MapStyle _style;

  int _w, _h;
  double _orx, _ory, _mercW, _mercH;
};

}  // namespace petrimaps

#endif  // PETRIMAPS_SERVER_RENDERCONTEXT_H_
