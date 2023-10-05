// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#ifndef UTIL_GRAPH_GRAPH_H_
#define UTIL_GRAPH_GRAPH_H_

#include <cassert>
#include <iostream>
#include <set>
#include <string>

#include "util/graph/Edge.h"
#include "util/graph/Node.h"

namespace util {
namespace graph {

template <typename N, typename E>
class Graph {
 public:
  Graph() {} ;
  Graph(const Graph& g) = delete;
  Graph(Graph& g) = delete;
  void operator=(const Graph& other) = delete;
  void operator=(Graph& other) = delete;
  virtual ~Graph();
  virtual Node<N, E>* addNd() = 0;
  virtual Node<N, E>* addNd(const N& pl) = 0;
  Edge<N, E>* addEdg(Node<N, E>* from, Node<N, E>* to);
  virtual Edge<N, E>* addEdg(Node<N, E>* from, Node<N, E>* to, const E& p) = 0;
  virtual Edge<N, E>* addEdg(Node<N, E>* from, Node<N, E>* to, E&& p) = 0;
  Edge<N, E>* getEdg(Node<N, E>* from, Node<N, E>* to);
  const Edge<N, E>* getEdg(const Node<N, E>* from, const Node<N, E>* to) const;

  virtual Node<N, E>* mergeNds(Node<N, E>* a, Node<N, E>* b) = 0;

  const std::set<Node<N, E>*>& getNds() const;

  static Node<N, E>* sharedNode(const Edge<N, E>* a, const Edge<N, E>* b);

  typename std::set<Node<N, E>*>::iterator delNd(Node<N, E>* n);
  typename std::set<Node<N, E>*>::iterator delNd(
      typename std::set<Node<N, E>*>::iterator i);
  void delEdg(Node<N, E>* from, Node<N, E>* to);

 protected:
  std::set<Node<N, E>*> _nodes;
};

#include "util/graph/Graph.tpp"
}  // namespace graph
}  // namespace util

#endif  // UTIL_GRAPH_GRAPH_H_
