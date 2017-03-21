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

#ifndef ARANGODB_VOCBASE_PATHENUMERATOR_H
#define ARANGODB_VOCBASE_PATHENUMERATOR_H 1

#include "Basics/Common.h"
#include "VocBase/TraverserOptions.h"
#include <velocypack/Slice.h>
#include <stack>

namespace arangodb {
namespace aql {
struct AqlValue;
}

namespace velocypack {
class Builder;
}

namespace traverser {
class Traverser;
struct TraverserOptions;

struct EnumeratedPath {
  std::vector<arangodb::velocypack::Slice> edges;
  std::vector<arangodb::velocypack::Slice> vertices;
  EnumeratedPath() {}
};


class PathEnumerator {

 protected:

  //////////////////////////////////////////////////////////////////////////////
  /// @brief This is the class that knows the details on how to
  ///        load the data and how to return data in the expected format
  ///        NOTE: This class does not known the traverser.
  //////////////////////////////////////////////////////////////////////////////

   traverser::Traverser* _traverser;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief Indicates if we issue next() the first time.
  ///        It shall return an empty path in this case.
  //////////////////////////////////////////////////////////////////////////////

  bool _isFirst;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief Maximal path length which should be enumerated.
  //////////////////////////////////////////////////////////////////////////////

  TraverserOptions const* _opts; 

  //////////////////////////////////////////////////////////////////////////////
  /// @brief List of the last path is used to
  //////////////////////////////////////////////////////////////////////////////

  EnumeratedPath _enumeratedPath;

  /// @brief List which edges have been visited already.
  std::unordered_set<arangodb::velocypack::Slice> _returnedEdges;

 public:
  PathEnumerator(Traverser* traverser, arangodb::velocypack::Slice startVertex,
                 TraverserOptions const* opts)
      : _traverser(traverser), _isFirst(true), _opts(opts) {
    TRI_ASSERT(startVertex.isString());
    _enumeratedPath.vertices.push_back(startVertex);
    TRI_ASSERT(_enumeratedPath.vertices.size() == 1);
  }

  virtual ~PathEnumerator() {}

  //////////////////////////////////////////////////////////////////////////////
  /// @brief Compute the next Path element from the traversal.
  ///        Returns false if there is no next path element.
  ///        Only if this is true one can compute the AQL values.
  //////////////////////////////////////////////////////////////////////////////

  virtual bool next() = 0;

  virtual aql::AqlValue lastVertexToAqlValue() = 0;
  virtual aql::AqlValue lastEdgeToAqlValue() = 0;
  virtual aql::AqlValue pathToAqlValue(arangodb::velocypack::Builder&) = 0;
};

class DepthFirstEnumerator final : public PathEnumerator {
 private:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief The stack of EdgeCursors to walk through.
  //////////////////////////////////////////////////////////////////////////////

  std::stack<std::unique_ptr<EdgeCursor>> _edgeCursors;

 public:
  DepthFirstEnumerator(Traverser* traverser,
                       arangodb::velocypack::Slice startVertex,
                       TraverserOptions const* opts)
      : PathEnumerator(traverser, startVertex, opts) {}

  ~DepthFirstEnumerator() {
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief Get the next Path element from the traversal.
  //////////////////////////////////////////////////////////////////////////////

  bool next() override;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief Prunes the current path prefix, the next function should not return
  ///        any path having this prefix anymore.
  //////////////////////////////////////////////////////////////////////////////

  aql::AqlValue lastVertexToAqlValue() override;

  aql::AqlValue lastEdgeToAqlValue() override;

  aql::AqlValue pathToAqlValue(arangodb::velocypack::Builder& result) override;

};


} // namespace traverser
} // namespace arangodb

#endif
