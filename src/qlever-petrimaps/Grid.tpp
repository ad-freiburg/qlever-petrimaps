// Copyright 2017, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Patrick Brosi <brosip@informatik.uni-freiburg.de>

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
Grid<V, G, T>::Grid()
    : _width(0),
      _height(0),
      _cellWidth(0),
      _cellHeight(0),
      _xWidth(0),
      _yHeight(0),
      _grid(0) {}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
Grid<V, G, T>::Grid(double w, double h, const util::geo::Box<T>& bbox)
    : _cellWidth(fabs(w)), _cellHeight(fabs(h)), _bb(bbox), _grid() {
  _width = bbox.getUpperRight().getX() - bbox.getLowerLeft().getX();
  _height = bbox.getUpperRight().getY() - bbox.getLowerLeft().getY();

  if (_width < 0 || _height < 0) {
    _width = 0;
    _height = 0;
    _xWidth = 0;
    _yHeight = 0;
    return;
  }

  _xWidth = ceil(_width / _cellWidth);
  _yHeight = ceil(_height / _cellHeight);

  // resize rows
  _grid = new std::vector<V>*[_xWidth];

  // resize columns
  for (size_t x = 0; x < _xWidth; x++) {
    _grid[x] = new std::vector<V>[_yHeight];
  }
}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
void Grid<V, G, T>::add(const util::geo::Point<T>& p,
                        const V& val) {
  add(getCellXFromX(p.getX()), getCellYFromY(p.getY()), val);
}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
void Grid<V, G, T>::add(const util::geo::Box<T>& box,
                        const V& val) {
  size_t swX = getCellXFromX(box.getLowerLeft().getX());
  size_t swY = getCellYFromY(box.getLowerLeft().getY());

  size_t neX = getCellXFromX(box.getUpperRight().getX());
  size_t neY = getCellYFromY(box.getUpperRight().getY());

  for (size_t x = swX; x <= neX && x < _xWidth; x++) {
    for (size_t y = swY; y <= neY && y < _yHeight; y++) {
      // if (intersects(geom, getBox(x, y))) {
      add(x, y, val);
      // }
    }
  }
}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
void Grid<V, G, T>::add(size_t x, size_t y, V val) {
  _grid[x][y].push_back(val);
}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
void Grid<V, G, T>::get(const util::geo::Box<T>& box,
                        std::unordered_set<V>* s) const {
  size_t swX = getCellXFromX(box.getLowerLeft().getX());
  size_t swY = getCellYFromY(box.getLowerLeft().getY());

  size_t neX = getCellXFromX(box.getUpperRight().getX());
  size_t neY = getCellYFromY(box.getUpperRight().getY());

  for (size_t x = swX; x <= neX && x < _xWidth; x++)
    for (size_t y = swY; y <= neY && y < _yHeight; y++) get(x, y, s);
}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
void Grid<V, G, T>::get(const G<T>& geom, double d,
                        std::unordered_set<V>* s) const {
  util::geo::Box<T> a = getBoundingBox(geom);
  util::geo::Box<T> b(util::geo::Point<T>(a.getLowerLeft().getX() - d,
                                          a.getLowerLeft().getY() - d),
                      util::geo::Point<T>(a.getUpperRight().getX() + d,
                                          a.getUpperRight().getY() + d));
  return get(b, s);
}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
void Grid<V, G, T>::get(size_t x, size_t y, std::unordered_set<V>* s) const {
  s->insert(_grid[x][y].begin(), _grid[x][y].end());
}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
const std::vector<V>& Grid<V, G, T>::getCell(size_t x, size_t y) const {
  return _grid[x][y];
}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
util::geo::Box<T> Grid<V, G, T>::getBox(size_t x, size_t y) const {
  util::geo::Point<T> sw(_bb.getLowerLeft().getX() + x * _cellWidth,
                         _bb.getLowerLeft().getY() + y * _cellHeight);
  util::geo::Point<T> ne(_bb.getLowerLeft().getX() + (x + 1) * _cellWidth,
                         _bb.getLowerLeft().getY() + (y + 1) * _cellHeight);
  return util::geo::Box<T>(sw, ne);
}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
size_t Grid<V, G, T>::getCellXFromX(double x) const {
  float dist = x - _bb.getLowerLeft().getX();
  if (dist < 0) return 0;
  return dist / _cellWidth;
}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
size_t Grid<V, G, T>::getCellYFromY(double y) const {
  float dist = y - _bb.getLowerLeft().getY();
  if (dist < 0) return 0;
  return dist / _cellHeight;
}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
size_t Grid<V, G, T>::getXWidth() const {
  return _xWidth;
}

// _____________________________________________________________________________
template <typename V, template <typename> class G, typename T>
size_t Grid<V, G, T>::getYHeight() const {
  return _yHeight;
}
