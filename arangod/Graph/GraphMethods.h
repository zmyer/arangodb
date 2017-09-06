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

#ifndef ARANGOD_GRAPH_GRAPH_METHODS_H
#define ARANGOD_GRAPH_GRAPH_METHODS_H 1

////////////////////////////////////////////////////////////////////////////////
///  Description
///
///  This class is supposed to deliver helper methods required
///  in the various graph cases (during query runtime).
///  It may be specialized for certain use-cases to deliver even more
///  methods.
///  It has to be created from BaseOptions (or an extension thereof).
///  This class contains code specifically for Cluster and SingleServer exection.
////////////////////////////////////////////////////////////////////////////////

namespace arangodb {

namespace aql {
class FixedVarExpressionContext;
}

namespace velocypack {
class Builder;
}

class TraverserCache;

namespace graph {

class GraphMethods {
  void clearVariableValues();

  void setVariableValue(aql::Variable const*, aql::AqlValue const);

  void serializeVariables(arangodb::velocypack::Builder&) const;

  TraverserCache* cache() const;

  void activateCache(
      bool enableDocumentCache,
      std::unordered_map<ServerID, traverser::TraverserEngineID> const*
          engines);

 protected:

  /// @brief the traverser cache
  std::unique_ptr<TraverserCache> _cache;

 private:

  /// @brief Context required for variables in AQL
  ///        Required to evaluate AQL Conditions
  aql::FixedVarExpressionContext* _ctx;
};

}  // namespace graph
}  // namespace arangodb

#endif
