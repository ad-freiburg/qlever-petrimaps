// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef PETRIMAPS_GRID_H_
#define PETRIMAPS_GRID_H_

#include <map>
#include <unordered_set>
#include <vector>
#include "util/geo/Geo.h"

namespace petrimaps {

class GridException : public std::runtime_error {
 public:
  GridException(std::string const& msg) : std::runtime_error(msg) {}
};

template <typename V, typename T>
class Grid {
 public:
  Grid(const Grid<V, T>&) = delete;
  Grid(Grid<V, T>&& o)
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

  Grid<V, T>& operator=(Grid<V, T>&& o) {
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
    for (size_t i = 0; i < _xWidth * _yHeight; i++) {
      if (!_grid[i]) continue;
      delete _grid[i];
    }
    delete[] _grid;
  }

  // add object t to this grid
  void add(const util::geo::Box<T>& box, const V& val);
  void add(const util::geo::Point<T>& box, const V& val);
  void add(size_t x, size_t y, V val);

  void get(const util::geo::Box<T>& btbox, std::unordered_set<V>* s) const;
  void get(size_t x, size_t y, std::unordered_set<V>* s) const;
  void get(const util::geo::Box<T>& btbox, std::vector<V>* s) const;
  void get(size_t x, size_t y, std::vector<V>* s) const;
  const std::vector<V>* getCell(size_t x, size_t y) const;

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

#include "qlever-petrimaps/Grid.tpp"

} // namespace petrimaps

#endif  // PETRIMAPS_GRID_H_
