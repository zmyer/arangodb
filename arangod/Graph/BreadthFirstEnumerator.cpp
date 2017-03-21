////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2016 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Michael Hackstein
////////////////////////////////////////////////////////////////////////////////

#include "BreadthFirstEnumerator.h"

#include "VocBase/Traverser.h"
#include "VocBase/TraverserOptions.h"

#include <velocypack/Slice.h>
#include <velocypack/velocypack-aliases.h>

using namespace arangodb;
using namespace arangodb::traverser;

using BreadthFirstEnumerator = arangodb::graph::BreadthFirstEnumerator;

BreadthFirstEnumerator::BreadthFirstEnumerator(Traverser* traverser,
                                               VPackSlice startVertex,
                                               TraverserOptions const* opts)
    : PathEnumerator(traverser, startVertex, opts),
      _schreierIndex(1),
      _lastReturned(0),
      _currentDepth(0),
      _toSearchPos(0) {
  _schreier.reserve(32);
  _schreier.emplace_back(startVertex);

  _toSearch.emplace_back(NextStep(0));
}

bool BreadthFirstEnumerator::next() {
  if (_isFirst) {
    _isFirst = false;
    if (_opts->minDepth == 0) {
      computeEnumeratedPath(_lastReturned++);
      return true;
    }
    _lastReturned++;
  }

  if (_lastReturned < _schreierIndex) {
    // We still have something on our stack.
    // Paths have been read but not returned.
    computeEnumeratedPath(_lastReturned++);
    return true;
  }

  if (_opts->maxDepth == 0) {
    // Short circuit.
    // We cannot find any path of length 0 or less
    return false;
  }
  // Avoid large call stacks.
  // Loop will be left if we are either finished
  // with searching.
  // Or we found vertices in the next depth for
  // a vertex.
  while (true) {
    if (_toSearchPos >= _toSearch.size()) {
      // This depth is done. GoTo next
      if (_nextDepth.empty()) {
        // That's it. we are done.
        _enumeratedPath.edges.clear();
        _enumeratedPath.vertices.clear();
        return false;
      }
      // Save copies:
      // We clear current
      // we swap current and next.
      // So now current is filled
      // and next is empty.
      _toSearch.clear();
      _toSearchPos = 0;
      _toSearch.swap(_nextDepth);
      _currentDepth++;
      TRI_ASSERT(_toSearchPos < _toSearch.size());
      TRI_ASSERT(_nextDepth.empty());
      TRI_ASSERT(_currentDepth < _opts->maxDepth);
    }
    // This access is always safe.
    // If not it should have bailed out before.
    TRI_ASSERT(_toSearchPos < _toSearch.size());

    _tmpEdges.clear();
    auto const nextIdx = _toSearch[_toSearchPos++].sourceIdx;
    auto const nextVertex = _schreier[nextIdx].vertex;

    std::unique_ptr<arangodb::traverser::EdgeCursor> cursor(_opts->nextCursor(_traverser->mmdr(), nextVertex, _currentDepth));
    if (cursor != nullptr) {
      size_t cursorIdx;
      bool shouldReturnPath = _currentDepth + 1 >= _opts->minDepth;
      bool didInsert = false;
      while (cursor->readAll(_tmpEdges, cursorIdx)) {
        if (!_tmpEdges.empty()) {
          _traverser->_readDocuments += _tmpEdges.size();
          VPackSlice v;
          for (auto const& e : _tmpEdges) {
            if (_opts->uniqueEdges ==
                TraverserOptions::UniquenessLevel::GLOBAL) {
              if (_returnedEdges.find(e) == _returnedEdges.end()) {
                // Edge not yet visited. Mark and continue.
                _returnedEdges.emplace(e);
              } else {
                _traverser->_filteredPaths++;
                continue;
              }
            }

            if (!_traverser->edgeMatchesConditions(e, nextVertex,
                                                   _currentDepth,
                                                   cursorIdx)) {
              continue;
            }
            if (_traverser->getSingleVertex(e, nextVertex, _currentDepth, v)) {
              _schreier.emplace_back(nextIdx, e, v);
              if (_currentDepth < _opts->maxDepth - 1) {
                _nextDepth.emplace_back(NextStep(_schreierIndex));
              }
              _schreierIndex++;
              didInsert = true;
            }
          }
          _tmpEdges.clear();
        }
      }
      if (!shouldReturnPath) {
        _lastReturned = _schreierIndex;
        didInsert = false;
      }
      if (didInsert) {
        // We exit the loop here.
        // _schreierIndex is moved forward
        break;
      }
    }
    // Nothing found for this vertex.
    // _toSearchPos is increased so
    // we are not stuck in an endless loop
  }

  // _lastReturned points to the last used
  // entry. We compute the path to it
  // and increase the schreierIndex to point
  // to the next free position.
  computeEnumeratedPath(_lastReturned++);
  return true;
}

// TODO Optimize this. Remove enumeratedPath
// All can be read from schreier vector directly
arangodb::aql::AqlValue BreadthFirstEnumerator::lastVertexToAqlValue() {
  return _traverser->fetchVertexData(StringRef(
      _enumeratedPath.vertices.back()));
}

arangodb::aql::AqlValue BreadthFirstEnumerator::lastEdgeToAqlValue() {
  if (_enumeratedPath.edges.empty()) {
    return arangodb::aql::AqlValue(arangodb::basics::VelocyPackHelper::NullValue());
  }
  return _traverser->fetchEdgeData(_enumeratedPath.edges.back());
}

arangodb::aql::AqlValue BreadthFirstEnumerator::pathToAqlValue(
    arangodb::velocypack::Builder& result) {
  result.clear();
  result.openObject();
  result.add(VPackValue("edges"));
  result.openArray();
  for (auto const& it : _enumeratedPath.edges) {
    _traverser->addEdgeToVelocyPack(it, result);
  }
  result.close();
  result.add(VPackValue("vertices"));
  result.openArray();
  for (auto const& it : _enumeratedPath.vertices) {
    _traverser->addVertexToVelocyPack(it, result);
  }
  result.close();
  result.close();
  return arangodb::aql::AqlValue(result.slice());
}

void BreadthFirstEnumerator::computeEnumeratedPath(size_t index) {
  TRI_ASSERT(index < _schreier.size());

  size_t depth = getDepth(index);
  _enumeratedPath.edges.clear();
  _enumeratedPath.vertices.clear();
  _enumeratedPath.edges.resize(depth);
  _enumeratedPath.vertices.resize(depth + 1);

  // Computed path. Insert it into the path enumerator.
  while (index != 0) {
    TRI_ASSERT(depth > 0);
    PathStep const& current = _schreier[index];
    _enumeratedPath.vertices[depth] = current.vertex;
    _enumeratedPath.edges[depth - 1] = current.edge;

    index = current.sourceIdx;
    --depth;
  }

  _enumeratedPath.vertices[0] = _schreier[0].vertex;
}


