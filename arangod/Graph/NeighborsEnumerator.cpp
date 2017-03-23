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

#include "NeighborsEnumerator.h"

#include "Basics/VelocyPackHelper.h"
#include "VocBase/Traverser.h"

using namespace arangodb;
using namespace arangodb::traverser;
using namespace arangodb::graph;

NeighborsEnumerator::NeighborsEnumerator(Traverser* traverser,
                                         VPackSlice const& startVertex,
                                         TraverserOptions* opts)
    : PathEnumerator(traverser, startVertex.copyString(), opts),
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
        StringRef vId(nextVertex.slice);
        std::unique_ptr<arangodb::traverser::EdgeCursor> cursor(
            _opts->nextCursor(_traverser->mmdr(), vId, _searchDepth));
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
  StringRef vid(_iterator->slice);
  return _traverser->fetchVertexData(vid);
}

arangodb::aql::AqlValue NeighborsEnumerator::lastEdgeToAqlValue() {
  // TODO should return Optimizer failed
  THROW_ARANGO_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
}

arangodb::aql::AqlValue NeighborsEnumerator::pathToAqlValue(arangodb::velocypack::Builder& result) {
  // TODO should return Optimizer failed
  THROW_ARANGO_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
}
