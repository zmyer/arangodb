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

#include "PathEnumerator.h"
#include "Basics/VelocyPackHelper.h"
#include "VocBase/Traverser.h"

using DepthFirstEnumerator = arangodb::traverser::DepthFirstEnumerator;
using NeighborsEnumerator = arangodb::traverser::NeighborsEnumerator;
using Traverser = arangodb::traverser::Traverser;
using TraverserOptions = arangodb::traverser::TraverserOptions;

bool DepthFirstEnumerator::next() {
  if (_isFirst) {
    _isFirst = false;
    if (_opts->minDepth == 0) {
      return true;
    }
  }
  if (_enumeratedPath.vertices.empty()) {
    // We are done;
    return false;
  }

  size_t cursorId = 0;

  while (true) {
    if (_enumeratedPath.edges.size() < _opts->maxDepth) {
      // We are not done with this path, so
      // we reserve the cursor for next depth
      auto cursor = _opts->nextCursor(_traverser->mmdr(), _enumeratedPath.vertices.back(),
                                      _enumeratedPath.edges.size());
      if (cursor != nullptr) {
        _edgeCursors.emplace(cursor);
      }
    } else {
      if (!_enumeratedPath.edges.empty()) {
        // This path is at the end. cut the last step
        _enumeratedPath.vertices.pop_back();
        _enumeratedPath.edges.pop_back();
      }
    }

    while (!_edgeCursors.empty()) {
      TRI_ASSERT(_edgeCursors.size() == _enumeratedPath.edges.size() + 1);
      auto& cursor = _edgeCursors.top();
      if (cursor->next(_enumeratedPath.edges, cursorId)) {
        ++_traverser->_readDocuments;
        if (_opts->uniqueEdges == TraverserOptions::UniquenessLevel::GLOBAL) {
          if (_returnedEdges.find(_enumeratedPath.edges.back()) ==
              _returnedEdges.end()) {
            // Edge not yet visited. Mark and continue.
            _returnedEdges.emplace(_enumeratedPath.edges.back());
          } else {
            _traverser->_filteredPaths++;
            TRI_ASSERT(!_enumeratedPath.edges.empty());
            _enumeratedPath.edges.pop_back();
            continue;
          }
        }
        if (!_traverser->edgeMatchesConditions(_enumeratedPath.edges.back(),
                                               _enumeratedPath.vertices.back(),
                                               _enumeratedPath.edges.size() - 1,
                                               cursorId)) {
            // This edge does not pass the filtering
            TRI_ASSERT(!_enumeratedPath.edges.empty());
            _enumeratedPath.edges.pop_back();
            continue;
        }

        if (_opts->uniqueEdges == TraverserOptions::UniquenessLevel::PATH) {
          auto& e = _enumeratedPath.edges.back();
          bool foundOnce = false;
          for (auto const& it : _enumeratedPath.edges) {
            if (foundOnce) {
              foundOnce = false; // if we leave with foundOnce == false we found the edge earlier
              break;
            }
            if (it == e) {
              foundOnce = true;
            }
          }
          if (!foundOnce) {
            // We found it and it was not the last element (expected)
            // This edge is allready on the path
            TRI_ASSERT(!_enumeratedPath.edges.empty());
            _enumeratedPath.edges.pop_back();
            continue;
          }
        }

        // We have to check if edge and vertex is valid
        if (_traverser->getVertex(_enumeratedPath.edges.back(),
                                  _enumeratedPath.vertices)) {
          // case both are valid.
          if (_opts->uniqueVertices == TraverserOptions::UniquenessLevel::PATH) {
            auto& e = _enumeratedPath.vertices.back();
            bool foundOnce = false;
            for (auto const& it : _enumeratedPath.vertices) {
              if (foundOnce) {
                foundOnce = false;  // if we leave with foundOnce == false we
                                    // found the edge earlier
                break;
              }
              if (it == e) {
                foundOnce = true;
              }
            }
            if (!foundOnce) {
              // We found it and it was not the last element (expected)
              // This vertex is allready on the path
              TRI_ASSERT(!_enumeratedPath.edges.empty());
              _enumeratedPath.vertices.pop_back();
              _enumeratedPath.edges.pop_back();
              continue;
            }
          }
          if (_enumeratedPath.edges.size() < _opts->minDepth) {
            // Do not return, but leave this loop. Continue with the outer.
            break;
          }

          return true;
        }
        // Vertex Invalid. Revoke edge
        TRI_ASSERT(!_enumeratedPath.edges.empty());
        _enumeratedPath.edges.pop_back();
        continue;
      } else {
        // cursor is empty.
        _edgeCursors.pop();
        if (!_enumeratedPath.edges.empty()) {
          _enumeratedPath.edges.pop_back();
          _enumeratedPath.vertices.pop_back();
        }
      }
    }
    if (_edgeCursors.empty()) {
      // If we get here all cursors are exhausted.
      _enumeratedPath.edges.clear();
      _enumeratedPath.vertices.clear();
      return false;
    }
  }
}

arangodb::aql::AqlValue DepthFirstEnumerator::lastVertexToAqlValue() {
  return _traverser->fetchVertexData(_enumeratedPath.vertices.back());
}

arangodb::aql::AqlValue DepthFirstEnumerator::lastEdgeToAqlValue() {
  if (_enumeratedPath.edges.empty()) {
    return arangodb::aql::AqlValue(arangodb::basics::VelocyPackHelper::NullValue());
  }
  return _traverser->fetchEdgeData(_enumeratedPath.edges.back());
}

arangodb::aql::AqlValue DepthFirstEnumerator::pathToAqlValue(arangodb::velocypack::Builder& result) {
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

NeighborsEnumerator::NeighborsEnumerator(Traverser* traverser,
                                         VPackSlice startVertex,
                                         TraverserOptions const* opts)
    : PathEnumerator(traverser, startVertex, opts),
      _searchDepth(0) {
  _allFound.insert(arangodb::basics::VPackHashedSlice(startVertex));
  _currentDepth.insert(arangodb::basics::VPackHashedSlice(startVertex));
  _iterator = _currentDepth.begin();
}

bool NeighborsEnumerator::next() {
  if (_isFirst) {
    _isFirst = false;
    if (_opts->minDepth == 0) {
      return true;
    }
  }

  if (_iterator == _currentDepth.end() || ++_iterator == _currentDepth.end()) {
    do {
      // This depth is done. Get next
      if (_opts->maxDepth == _searchDepth) {
        // We are finished.
        return false;
      }

      _lastDepth.swap(_currentDepth);
      _currentDepth.clear();
      for (auto const& nextVertex : _lastDepth) {
        size_t cursorIdx = 0;
        std::unique_ptr<arangodb::traverser::EdgeCursor> cursor(
            _opts->nextCursor(_traverser->mmdr(), nextVertex.slice, _searchDepth));
        while (cursor->readAll(_tmpEdges, cursorIdx)) {
          if (!_tmpEdges.empty()) {
            _traverser->_readDocuments += _tmpEdges.size();
            VPackSlice v;
            for (auto const& e : _tmpEdges) {
              if (_traverser->getSingleVertex(e, nextVertex.slice, _searchDepth, v)) {
                arangodb::basics::VPackHashedSlice hashed(v);
                if (_allFound.find(hashed) == _allFound.end()) {
                  _currentDepth.emplace(hashed);
                  _allFound.emplace(hashed);
                }
              }
            }
            _tmpEdges.clear();
          }
        }
      }
      if (_currentDepth.empty()) {
        // Nothing found. Cannot do anything more.
        return false;
      }
      ++_searchDepth;
    } while (_searchDepth < _opts->minDepth);
    _iterator = _currentDepth.begin();
  }
  TRI_ASSERT(_iterator != _currentDepth.end());
  return true;
}

arangodb::aql::AqlValue NeighborsEnumerator::lastVertexToAqlValue() {
  TRI_ASSERT(_iterator != _currentDepth.end());
  return _traverser->fetchVertexData((*_iterator).slice);
}

arangodb::aql::AqlValue NeighborsEnumerator::lastEdgeToAqlValue() {
  // TODO should return Optimizer failed
  THROW_ARANGO_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
}

arangodb::aql::AqlValue NeighborsEnumerator::pathToAqlValue(arangodb::velocypack::Builder& result) {
  // TODO should return Optimizer failed
  THROW_ARANGO_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
}
