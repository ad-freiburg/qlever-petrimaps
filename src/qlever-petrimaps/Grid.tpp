// Copyright 2017, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Patrick Brosi <brosip@informatik.uni-freiburg.de>

// _____________________________________________________________________________
template <typename V, typename T, typename W>
Grid<V, T, W>::Grid()
    : _width(0),
      _height(0),
      _cellWidth(0),
      _cellHeight(0),
      _xWidth(0),
      _yHeight(0),
      _grid(0) {}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
Grid<V, T, W>::Grid(double w, double h, const util::geo::Box<T>& bbox)
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
  _grid = new std::vector<V>*[_xWidth * _yHeight];
  memset(_grid, 0, _xWidth * _yHeight * sizeof(std::vector<V>*));

  _cellSums.resize(_xWidth * _yHeight);
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
void Grid<V, T, W>::add(const util::geo::Point<T>& p, W weight, const V& val) {
  add(getCellXFromX(p.getX()), getCellYFromY(p.getY()), weight, val);
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
void Grid<V, T, W>::add(const util::geo::Box<T>& box, W weight, const V& val) {
  size_t swX = getCellXFromX(box.getLowerLeft().getX());
  size_t swY = getCellYFromY(box.getLowerLeft().getY());

  size_t neX = getCellXFromX(box.getUpperRight().getX());
  size_t neY = getCellYFromY(box.getUpperRight().getY());

  for (size_t x = swX; x <= neX && x < _xWidth; x++) {
    for (size_t y = swY; y <= neY && y < _yHeight; y++) {
      add(x, y, weight, val);
    }
  }
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
void Grid<V, T, W>::add(size_t x, size_t y, W weight, V val) {
  if (x >= _xWidth || y >= _yHeight) return;
  if (!_grid[y * _xWidth + x]) _grid[y * _xWidth + x] = new std::vector<V>();
  _grid[y * _xWidth + x]->push_back(val);
  _cellSums[y * _xWidth + x] += weight;
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
void Grid<V, T, W>::get(const util::geo::Box<T>& box, std::vector<V>* s) const {
  size_t swX = getCellXFromX(box.getLowerLeft().getX());
  size_t swY = getCellYFromY(box.getLowerLeft().getY());

  size_t neX = getCellXFromX(box.getUpperRight().getX());
  size_t neY = getCellYFromY(box.getUpperRight().getY());

  for (size_t x = swX; x <= neX && x < _xWidth; x++)
    for (size_t y = swY; y <= neY && y < _yHeight; y++) get(x, y, s);
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
void Grid<V, T, W>::get(const util::geo::Box<T>& box,
                        std::unordered_set<V>* s) const {
  size_t swX = getCellXFromX(box.getLowerLeft().getX());
  size_t swY = getCellYFromY(box.getLowerLeft().getY());

  size_t neX = getCellXFromX(box.getUpperRight().getX());
  size_t neY = getCellYFromY(box.getUpperRight().getY());

  for (size_t x = swX; x <= neX && x < _xWidth; x++)
    for (size_t y = swY; y <= neY && y < _yHeight; y++) get(x, y, s);
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
void Grid<V, T, W>::get(size_t x, size_t y, std::unordered_set<V>* s) const {
  if (!_grid[y * _xWidth + x]) return;
  s->insert(_grid[y * _xWidth + x]->begin(), _grid[y * _xWidth + x]->end());
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
void Grid<V, T, W>::get(size_t x, size_t y, std::vector<V>* s) const {
  if (!_grid[y * _xWidth + x]) return;
  s->insert(s->end(), _grid[y * _xWidth + x]->begin(),
            _grid[y * _xWidth + x]->end());
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
const std::vector<V>* Grid<V, T, W>::getCell(size_t x, size_t y) const {
  return _grid[y * _xWidth + x];
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
W Grid<V, T, W>::getCellSum(size_t x, size_t y) const {
  return _cellSums[y * _xWidth + x];
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
util::geo::Box<T> Grid<V, T, W>::getBox(size_t x, size_t y) const {
  util::geo::Point<T> sw(_bb.getLowerLeft().getX() + x * _cellWidth,
                         _bb.getLowerLeft().getY() + y * _cellHeight);
  util::geo::Point<T> ne(_bb.getLowerLeft().getX() + (x + 1) * _cellWidth,
                         _bb.getLowerLeft().getY() + (y + 1) * _cellHeight);
  return util::geo::Box<T>(sw, ne);
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
size_t Grid<V, T, W>::getCellXFromX(double x) const {
  float dist = x - _bb.getLowerLeft().getX();
  if (dist < 0) return 0;
  return dist / _cellWidth;
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
size_t Grid<V, T, W>::getCellYFromY(double y) const {
  float dist = y - _bb.getLowerLeft().getY();
  if (dist < 0) return 0;
  return dist / _cellHeight;
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
size_t Grid<V, T, W>::getXWidth() const {
  return _xWidth;
}

// _____________________________________________________________________________
template <typename V, typename T, typename W>
size_t Grid<V, T, W>::getYHeight() const {
  return _yHeight;
}
