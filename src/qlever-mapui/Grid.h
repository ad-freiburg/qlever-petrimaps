// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef MAPUI_GRID_H_
#define MAPUI_GRID_H_

#include <map>
#include <unordered_set>
#include <vector>
#include "util/geo/Geo.h"

namespace mapui {

class GridException : public std::runtime_error {
 public:
  GridException(std::string const& msg) : std::runtime_error(msg) {}
};

template <typename V, template <typename> class G, typename T>
class Grid {
 public:
  Grid(const Grid<V, G, T>&) = delete;
  Grid(Grid<V, G, T>&& o)
      : _width(o._width),
        _height(o._height),
        _cellWidth(o._cellWidth),
        _cellHeight(o._cellHeight),
        _bb(o._bb),
        _xWidth(o._xWidth),
        _yHeight(o._yHeight),
        _grid(o._grid) {
    o._grid = 0;
  }

  Grid<V, G, T>& operator=(Grid<V, G, T>&& o) {
    _width = o._width;
    _height = o._height;
    _cellWidth = o._cellWidth;
    _cellHeight = o._cellHeight;
    _bb = o._bb;
    _xWidth = o._xWidth;
    _yHeight = o._yHeight;
    _grid = o._grid;
    o._grid = 0;

    return *this;
  };

  // initialization of a point grid with cell width w and cell height h
  // that covers the area of bounding box bbox
  Grid(double w, double h, const util::geo::Box<T>& bbox);

  // the empty grid
  Grid();

  ~Grid() {
    if (!_grid) return;
    for (size_t i = 0; i < _xWidth; i++) {
      delete[] _grid[i];
    }
    delete[] _grid;
  }

  // add object t to this grid
  void add(const G<T>& geom, const V& val);
  void add(const G<T>& geom, const util::geo::Box<T>& box, const V& val);
  void add(size_t x, size_t y, V val);

  void get(const util::geo::Box<T>& btbox, std::unordered_set<V>* s) const;
  void get(const G<T>& geom, double d, std::unordered_set<V>* s) const;
  void get(size_t x, size_t y, std::unordered_set<V>* s) const;
  const std::vector<V>& getCell(size_t x, size_t y) const;

  size_t getXWidth() const;
  size_t getYHeight() const;

  double getCellWidth() const { return _cellWidth; }
  double getCellHeight() const { return _cellHeight; }

  size_t getCellXFromX(double lon) const;
  size_t getCellYFromY(double lat) const;

  util::geo::Box<T> getBox(size_t x, size_t y) const;
  util::geo::Box<T> getBBox() const { return _bb; }

 private:
  double _width;
  double _height;

  double _cellWidth;
  double _cellHeight;

  util::geo::Box<T> _bb;

  size_t _xWidth;
  size_t _yHeight;

  std::vector<V>** _grid;
};

#include "qlever-mapui/Grid.tpp"

}  // namespace mapui

#endif  // MAPUI_GRID_H_
